// Package durable adds a DBOS durable process around the SQLGen checkout pipeline.
// The DBOS dependency is isolated in this nested module, not pulled into SQLGen itself.
package durable

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	marketplace "github.com/CAFxX/sqlgen/examples/modular_marketplace"
	"github.com/dbos-inc/dbos-transact-golang/dbos"
	_ "github.com/dbos-inc/dbos-transact-golang/dbos/driver/sqlite"
)

const (
	ApplicationName = "sqlgen-marketplace"
	CheckoutQueue   = "marketplace-checkout"
)

type Carrier interface {
	// CreateLabel must pass idempotencyKey to the carrier provider's idempotency
	// facility. A DBOS step can be retried if the provider succeeded but the
	// worker crashed before DBOS persisted the step result.
	CreateLabel(ctx context.Context, idempotencyKey string, orderID int64) (string, error)
}

type WorkflowInput struct {
	Checkout         marketplace.CheckoutInput
	FulfillmentDelay time.Duration
}

type WorkflowResult struct {
	OrderID        int64
	TrackingNumber string
}

type Worker struct {
	ctx      dbos.Context
	queue    dbos.Queue
	checkout *marketplace.Service
	carrier  Carrier
}

func NewWorker(parent context.Context, dbosDB *sql.DB, carrier Carrier) (*Worker, error) {
	if dbosDB == nil {
		return nil, errors.New("durable: database is nil")
	}
	if carrier == nil {
		return nil, errors.New("durable: carrier is nil")
	}

	ctx, err := dbos.NewContext(parent, dbos.Config{
		AppName:        ApplicationName,
		SQLiteSystemDB: dbosDB,
	})
	if err != nil {
		return nil, fmt.Errorf("create DBOS context: %w", err)
	}

	w := &Worker{
		ctx:      ctx,
		checkout: marketplace.New(dbosDB),
		carrier:  carrier,
	}
	dbos.RegisterWorkflow(ctx, w.ProcessOrder)

	queue, err := dbos.RegisterQueue(ctx, CheckoutQueue,
		dbos.WithPartitionQueue(),
		dbos.WithPartitionConcurrency(8),
		dbos.WithPartitionWorkerConcurrency(2),
	)
	if err != nil {
		_ = w.checkout.Close()
		_ = dbos.Shutdown(ctx, 5*time.Second)
		return nil, fmt.Errorf("register checkout queue: %w", err)
	}
	w.queue = queue

	if err := dbos.Launch(ctx); err != nil {
		_ = w.checkout.Close()
		_ = dbos.Shutdown(ctx, 5*time.Second)
		return nil, fmt.Errorf("launch DBOS: %w", err)
	}
	return w, nil
}

// Submit starts or retrieves the workflow identified by (marketplace, order).
// The tenant/marketplace is also the queue partition key, so one noisy store
// cannot consume another store's per-partition concurrency budget.
func (w *Worker) Submit(input WorkflowInput) (dbos.WorkflowHandle[WorkflowResult], error) {
	marketplaceID := input.Checkout.MarketplaceID
	orderID := input.Checkout.OrderID
	if marketplaceID <= 0 || orderID <= 0 {
		return nil, marketplace.ErrInvalidCheckout
	}

	workflowID := fmt.Sprintf("sqlgen-marketplace-checkout-%d-%d", marketplaceID, orderID)
	return dbos.RunWorkflow(w.ctx, w.ProcessOrder, input,
		dbos.WithQueue(w.queue),
		dbos.WithQueuePartitionKey(strconv.FormatInt(marketplaceID, 10)),
		dbos.WithWorkflowID(workflowID),
	)
}

// ProcessOrder is registered before Launch. It is intentionally a workflow
// function, not one large step: each nondeterministic or externally visible
// operation is checkpointed independently.
func (w *Worker) ProcessOrder(ctx dbos.Context, input WorkflowInput) (WorkflowResult, error) {
	if _, err := dbos.RunAsStep(ctx, func(stepCtx context.Context) (bool, error) {
		return true, w.checkout.Checkout(stepCtx, input.Checkout)
	}, dbos.WithStepName("marketplace.sqlgen_checkout")); err != nil {
		return WorkflowResult{}, fmt.Errorf("checkout: %w", err)
	}

	// DBOS persists the wake-up deadline. A process restart does not restart the
	// delay from zero; it resumes the workflow at the next recorded operation.
	if input.FulfillmentDelay > 0 {
		if _, err := dbos.Sleep(ctx, input.FulfillmentDelay); err != nil {
			return WorkflowResult{}, fmt.Errorf("wait before fulfillment: %w", err)
		}
	}

	idempotencyKey := fmt.Sprintf("checkout:%d:%d:carrier-label",
		input.Checkout.MarketplaceID, input.Checkout.OrderID)
	trackingNumber, err := dbos.RunAsStep(ctx, func(stepCtx context.Context) (string, error) {
		return w.carrier.CreateLabel(stepCtx, idempotencyKey, input.Checkout.OrderID)
	}, dbos.WithStepName("marketplace.carrier_create_label"))
	if err != nil {
		return WorkflowResult{}, fmt.Errorf("create carrier label: %w", err)
	}

	return WorkflowResult{
		OrderID:        input.Checkout.OrderID,
		TrackingNumber: trackingNumber,
	}, nil
}

func (w *Worker) Close() error {
	shutdownErr := dbos.Shutdown(w.ctx, 10*time.Second)
	closeErr := w.checkout.Close()
	return errors.Join(shutdownErr, closeErr)
}

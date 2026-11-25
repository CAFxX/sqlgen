package runtime

import (
	"errors"
	"fmt"
	"iter"
	"slices"
	"testing"
)

// seqOf yields the rows in order and, when failAt >= 0, an error after failAt rows.
func seqOf(rows []int, failAt int, failure error) iter.Seq2[int, error] {
	return func(yield func(int, error) bool) {
		for i, r := range rows {
			if i == failAt {
				yield(0, failure)
				return
			}
			if !yield(r, nil) {
				return
			}
		}
		if failAt >= len(rows) && failure != nil {
			yield(0, failure)
		}
	}
}

func TestCollectAppendToEach(t *testing.T) {
	boom := errors.New("boom")
	rows := []int{1, 2, 3, 4}

	got, err := Collect(seqOf(rows, -1, nil))
	if err != nil || !slices.Equal(got, rows) {
		t.Fatalf("Collect = %v, %v", got, err)
	}
	if got, err := Collect(seqOf(nil, -1, nil)); err != nil || got != nil || len(got) != 0 {
		t.Fatalf("Collect of no rows = %#v, %v; want a nil slice and no error", got, err)
	}
	if got, err := Collect(seqOf(rows, 2, boom)); !errors.Is(err, boom) || got != nil {
		t.Fatalf("Collect on error = %v, %v; want nil and the error", got, err)
	}

	// AppendTo keeps the caller's data and appends after it.
	dst := make([]int, 2, 16)
	dst[0], dst[1] = 10, 20
	out, err := AppendTo(dst, seqOf(rows, -1, nil))
	if err != nil || !slices.Equal(out, []int{10, 20, 1, 2, 3, 4}) {
		t.Fatalf("AppendTo = %v, %v", out, err)
	}
	if &out[0] != &dst[0] {
		t.Fatal("AppendTo with enough capacity must reuse dst's array")
	}
	// ... and on error returns dst exactly as passed (same length, caller data intact), never a partial append.
	out, err = AppendTo(dst, seqOf(rows, 2, boom))
	if !errors.Is(err, boom) || len(out) != 2 || out[0] != 10 || out[1] != 20 || &out[0] != &dst[0] {
		t.Fatalf("AppendTo on error = %v, %v; want dst unchanged and the error", out, err)
	}
	if out, err := AppendTo([]int(nil), seqOf(nil, -1, nil)); err != nil || out != nil {
		t.Fatalf("AppendTo(nil, no rows) = %#v, %v; want nil", out, err)
	}
	if out, err := AppendTo(dst, nil); !errors.Is(err, ErrNilArgument) || len(out) != 2 {
		t.Fatalf("AppendTo(nil seq) = %v, %v", out, err)
	}

	// Each: in order; the callback's error is returned unchanged and stops the iteration.
	var seen []int
	if err := Each(seqOf(rows, -1, nil), func(v int) error { seen = append(seen, v); return nil }); err != nil || !slices.Equal(seen, rows) {
		t.Fatalf("Each = %v, %v", seen, err)
	}
	seen = nil
	if err := Each(seqOf(rows, -1, nil), func(v int) error {
		seen = append(seen, v)
		if v == 2 {
			return boom
		}
		return nil
	}); err != boom || !slices.Equal(seen, []int{1, 2}) {
		t.Fatalf("Each with failing callback = %v, %v", seen, err)
	}
	// ErrStop ends the iteration early without an error.
	seen = nil
	if err := Each(seqOf(rows, -1, nil), func(v int) error {
		seen = append(seen, v)
		if v == 3 {
			return ErrStop
		}
		return nil
	}); err != nil || !slices.Equal(seen, []int{1, 2, 3}) {
		t.Fatalf("Each with ErrStop = %v, %v", seen, err)
	}
	// A wrapped ErrStop stops just as cleanly.
	seen = nil
	if err := Each(seqOf(rows, -1, nil), func(v int) error {
		seen = append(seen, v)
		if v == 2 {
			return fmt.Errorf("done early: %w", ErrStop)
		}
		return nil
	}); err != nil || !slices.Equal(seen, []int{1, 2}) {
		t.Fatalf("Each with wrapped ErrStop = %v, %v", seen, err)
	}
	// ErrStop is a callback-only signal: a sequence error wrapping it is still returned.
	seqStop := fmt.Errorf("driver: %w", ErrStop)
	if err := Each(seqOf(rows, 1, seqStop), func(int) error { return nil }); err != seqStop {
		t.Fatalf("Each on a sequence failing with a wrapped ErrStop = %v", err)
	}
	// An error from the sequence is returned and the callback is not called for it.
	seen = nil
	if err := Each(seqOf(rows, 2, boom), func(v int) error { seen = append(seen, v); return nil }); !errors.Is(err, boom) || !slices.Equal(seen, []int{1, 2}) {
		t.Fatalf("Each on a failing sequence = %v, %v", seen, err)
	}
	if err := Each(seqOf(rows, -1, nil), nil); !errors.Is(err, ErrNilArgument) {
		t.Fatalf("Each(nil fn) = %v", err)
	}
	if err := Each[int](nil, func(int) error { return nil }); !errors.Is(err, ErrNilArgument) {
		t.Fatalf("Each(nil seq) = %v", err)
	}
}

// A panic in the callback unwinds through the sequence, whose deferred cleanup (releasing the rows of a
// generated <Name>Seq) must run.
func TestEachPanicRunsSequenceCleanup(t *testing.T) {
	released := false
	seq := func(yield func(int, error) bool) {
		defer func() { released = true }()
		for i := 0; ; i++ {
			if !yield(i, nil) {
				return
			}
		}
	}
	func() {
		defer func() {
			if r := recover(); r != "cb" {
				t.Fatalf("recovered %v, want the callback's panic", r)
			}
		}()
		_ = Each(seq, func(int) error { panic("cb") })
	}()
	if !released {
		t.Fatal("the sequence's cleanup did not run after a panic in the callback")
	}
}

// The helpers add nothing to the allocations of the append itself (the closure handed to the sequence stays
// on the stack when the sequence is a plain function).
func TestSeqHelpersAllocations(t *testing.T) {
	rows := make([]int, 64)
	seq := FromSlice(rows)
	dst := make([]int, 0, 128)
	if n := testing.AllocsPerRun(100, func() { dst, _ = AppendTo(dst[:0], seq) }); n != 0 {
		t.Errorf("AppendTo into a slice with capacity: %v allocs/op, want 0", n)
	}
	sum := 0
	if n := testing.AllocsPerRun(100, func() { _ = Each(seq, func(v int) error { sum += v; return nil }) }); n != 0 {
		t.Errorf("Each: %v allocs/op, want 0", n)
	}
	if n := testing.AllocsPerRun(100, func() { _, _ = Collect(seq) }); n > 7 {
		t.Errorf("Collect of 64 rows: %v allocs/op; want only the slice growth (<=7)", n)
	}
}

// If is the generated Pipeline.If for the runtime's own tests (the handle type is *Pipeline itself).
func (p *Pipeline) If(cond PipelineCond, fn func(*Pipeline)) *IfChain[*Pipeline] {
	return If(p, cond, fn, func(b *Pipeline) *Pipeline { return b })
}

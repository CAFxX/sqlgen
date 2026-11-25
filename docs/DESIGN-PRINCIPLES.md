# sqlgen design principles

These rules apply to every user-facing surface: the YAML keys, the SQL DSL, the generated API, the `runtime`
package, the wire drivers, errors and DOCS. A design that breaks one needs a written exception that the owner
accepts. Reviewers and implementers are briefed with this file and [API-REVIEW-CHECKLIST.md](API-REVIEW-CHECKLIST.md).

## 1. One source of truth
- The user states each fact once. sqlgen must not ask the user to restate it in a second form (SQL and Go,
  YAML and SQL, a flag and a value, a cursor and its order) and then trust the two to agree.
- When two inputs must agree, sqlgen derives one from the other or checks that they agree when the code is
  generated. If it cannot do either, the design is wrong.
- Facts that the database already knows (types, nullability, uniqueness) are checked against it where possible.

## 2. Same semantics on every path
- The wire path, the sequential path and every engine return the same rows, in the same order, with the same
  errors and the same commit outcome. Only the round-trip count may differ.
- When an engine cannot match, the operation fails loudly (at generate time if possible) or takes the
  sequential path observably. It must not differ silently.
- A differential test that fails is a product bug. Relaxing its oracle needs a row in DOCS
  "Known divergences" that the owner has signed off.
- Performance fallbacks must be observable (`CacheStats`).

## 3. Misuse does not compile, or fails loudly
- Values that belong to one statement have that statement's type: masks, orders, cursors, terms and params.
  Do not use a bare `uint64`, `any` or untyped alias for them.
- An operation that would be silently ignored is an error: queuing on a pipeline that already ran
  (`ErrPipelineRan`), queuing outside the running `If` arm or on an expired arm handle, a zero
  `PipelineCond`, unknown mask bits, a cursor used with a different order.
- Zero values are either valid and meaningful or rejected.
- Lifecycles are enforced by the API shape (closures, handles that expire), not by DOCS prose.

## 4. The implementation's gaps are not the user's job
- When the implementation cannot do something on one path, it fixes that path. It does not hand the user a
  callback, a cast, a second pool or a test assertion to make up for it.
- DOCS wording such as "keep ... in sync", "remember to", "add an explicit cast" or "do not ... again" is a design
  defect report: it is fixed, or recorded in DOCS "Known divergences" (a test rejects it anywhere else).

## 5. Small, idiomatic Go surface
- `runtime` exports only what users call; generated-code plumbing is marked "used by generated code".
- Generated types do not embed runtime types that would promote internal methods.
- Prefer values and ranges over callbacks. Avoid several positional parameters of the same type and positional
  `bool` parameters.
- Every per-statement name starts with the statement's name.
- A knob ships only with a measured use case. Defaults must be safe.

## 6. Premises are reviewable
- SPEC.md is history, not requirements. Before building on an inherited design, write down its premise and
  check it against these principles.
- An exception to any principle is recorded in DOCS "Known divergences" with the reason and the owner's
  approval. An unrecorded exception is a bug.

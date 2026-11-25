# API review checklist

Required for every user-facing change (YAML, DSL, generated API, runtime exports, wire drivers, errors, DOCS).
A fresh reviewer runs it with [DESIGN-PRINCIPLES.md](DESIGN-PRINCIPLES.md) and the diff, and reports a judgement,
not only demonstrated bugs.

**Single source of truth**
- [ ] Does the user state any fact twice (SQL and Go, flag and value, YAML and SQL, cursor and order, cast and type)? If so, can it be derived or checked?
- [ ] Is any fact the database knows (type, nullability, uniqueness) restated without a check?

**Path independence**
- [ ] For the same inputs, do wire, sequential and every engine give the same rows, order, errors and commit outcome?
- [ ] Is there a test that runs both paths with ONE user input, not with a pair the harness keeps consistent?
- [ ] Was any test oracle relaxed? If so, is there an owner-approved row in DOCS "Known divergences"?
- [ ] Is every fallback observable?

**Misuse resistance**
- [ ] Does a value from another statement or pipeline, or a zero value, compile and run? Try it.
- [ ] Does any call silently do nothing (after Flush, flag off, unknown bits, closed branch)?
- [ ] Is a lifecycle or ordering rule enforced by the API's shape, or only by DOCS?

**Idiomatic Go and leakage**
- [ ] Are new exported symbols ones that users call? Diff `go doc -short ./runtime` before and after.
- [ ] Callbacks where a value or iterator would do? Same-typed or `bool` positional parameters? `any` in a signature?
- [ ] Is naming consistent with the per-statement scheme and the existing DSL directives?
- [ ] Does new DOCS text say "must", "remember", "keep in sync", "add a cast", "do not ... again", "only on the X path"? Each one is fixed or recorded in "Known divergences".

**Premise and alternatives**
- [ ] What premise does this rely on (SPEC, an earlier decision)? Is it still true?
- [ ] Name one alternative design and why it lost: "if we were starting today, would we build it this way?"

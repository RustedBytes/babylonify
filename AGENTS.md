## Purpose

This repository is a Rust project. When working here, prefer small, correct, well-tested changes that match existing patterns over broad refactors.

## Working style

- Read the touched module and nearby tests before editing.
- Keep diffs focused on the task.
- Do not rename public types, functions, modules, features, or CLI flags unless the task requires it.
- Preserve backward compatibility unless explicitly asked to break it.
- Prefer the smallest safe change that solves the problem completely.
- When behavior changes, add or update tests in the same change.

## Rust standards

- Target idiomatic stable Rust unless the repository already requires nightly.
- Prefer clarity over cleverness.
- Use the standard library before adding dependencies.
- Avoid unnecessary cloning, allocation, and dynamic dispatch.
- Propagate errors with structured types; avoid `unwrap()` and `expect()` in production code.
- Use `unwrap()` only in tests, fixtures, or when a panic is explicitly intended and documented.
- Favor iterators, enums, traits, and ownership-aware APIs when they improve readability.
- Keep functions short and single-purpose.
- Document non-obvious invariants, safety assumptions, and performance-sensitive code.

## Unsafe code

- Avoid `unsafe` unless it is essential.
- If `unsafe` is required:
  - keep the unsafe block as small as possible
  - explain the safety invariants in comments
  - add tests that exercise the assumptions
- Do not introduce `unsafe` as a micro-optimization without evidence.

## Concurrency and async

- Match the async runtime already used by the project.
- Do not block inside async code.
- Be explicit about cancellation, timeouts, and backpressure where relevant.
- For concurrent code, prefer simple synchronization and minimize lock scope.
- Watch for `Send`/`Sync` correctness and accidental contention.

## Dependencies

- Do not add new crates unless necessary.
- Reuse existing crates already in the repo when reasonable.
- If adding a dependency is required:
  - choose a well-maintained crate
  - enable the fewest features needed
  - explain briefly why it is needed in the final summary

## Project structure

- Keep module layout consistent with the existing crate structure.
- Put unit tests close to the code they cover when that is already the local convention.
- Use integration tests for public behavior, CLI behavior, or cross-module flows.
- Do not create new top-level modules or crates unless the task clearly benefits from it.

## Testing

Before finishing, run the smallest relevant checks first, then broaden if needed.

### Preferred command order

1. Run formatting:
   - `cargo fmt --all`

2. Run linting:
   - `cargo clippy --all-targets --all-features -- -D warnings`

3. Run tests:
   - `cargo test --all-features`

4. If this is a workspace and the change is isolated, prefer:
   - `cargo test -p <crate_name>`
   - `cargo clippy -p <crate_name> --all-targets --all-features -- -D warnings`

5. If benchmarks or docs are relevant:
   - `cargo test --doc`
   - `cargo bench`

- Do not claim code works unless the relevant checks pass.
- If you cannot run a command, say so explicitly and explain why.

## Review checklist

Before completing work, verify:

- the code builds
- formatting is clean
- clippy passes without adding blanket `allow` attributes
- tests cover the changed behavior
- error handling is explicit and useful
- no obvious performance regressions were introduced
- no secrets, tokens, or local paths were added
- public API changes are intentional and documented

## Code change guidelines

- Prefer fixing root causes over patching symptoms.
- Preserve existing logging and tracing style.
- Do not add noisy logs.
- For hot paths, avoid incidental allocations and repeated work.
- For parsers and protocol code, include edge-case tests.
- For serde types, preserve wire compatibility unless explicitly asked otherwise.
- For CLI changes, update help text, examples, and tests if applicable.

## Documentation

- Update Rustdoc, README snippets, or examples when behavior changes.
- Add concise comments for tricky logic, but do not comment obvious code.
- Include usage examples for public APIs when that would help future readers.

## Final response format

When reporting back, include:

1. What changed
2. Why it changed
3. What commands were run
4. What passed or failed
5. Any follow-up risks or assumptions

## Repository-specific notes

- If this repo defines local conventions in `README.md`, `CONTRIBUTING.md`, workspace manifests, or crate-level docs, follow those over generic Rust preferences.
- If a more specific `AGENTS.md` exists in a subdirectory, its instructions override this file for files in that subtree.

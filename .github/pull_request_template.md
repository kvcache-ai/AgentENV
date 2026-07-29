## What

<!-- Summarize the change. Keep the PR focused on one problem. -->

## Why

<!-- Explain the user or maintainer problem this solves. -->

## Related issue

<!-- Use "Closes #123" for an issue resolved by this PR. Non-trivial changes should normally have an issue or prior design discussion. -->

Closes #

## Scope and non-goals

<!-- State what is intentionally included and excluded. Call out unrelated refactors. -->

## Design and behavior changes

<!-- Describe important data flow, lifecycle, failure handling, concurrency, or operational changes. Include diagrams for substantial changes. -->

## Compatibility and operations

<!-- Explain applicable compatibility and deployment impact. Write "N/A" with a reason where appropriate. -->

- Public API or generated protocol:
- Configuration or defaults:
- Snapshot manifest, artifact layout, or storage format:
- Upgrade and rollback:
- Host requirements, permissions, ports, or dependencies:

## Validation

<!-- Check only commands you actually ran. Add focused tests and exact commands below. -->

- [ ] `make fmt`
- [ ] `make clippy`
- [ ] `make test-unit`
- [ ] Relevant Rust integration tests
- [ ] `make -C services test` (required when `services/` changes)
- [ ] Generated clients/server regenerated with the documented `make` target
- [ ] Documentation updated
- [ ] Benchmarks or performance comparison completed

Commands and results:

```text

```

Skipped checks and reasons:

## Risks and reviewer notes

<!-- Identify correctness, security, compatibility, resource, and operational risks. Point reviewers to the most important files or commits. -->

## Checklist

- [ ] The PR contains one coherent change and no unrelated formatting or refactoring.
- [ ] New behavior is covered by tests, or I explained why testing is impractical.
- [ ] Logs and examples contain no credentials, tokens, or private registry information.
- [ ] I did not manually edit generated code without updating its source and regenerating it.

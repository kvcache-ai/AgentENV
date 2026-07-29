# Contributing to AgentENV

Thank you for contributing to AgentENV. The project spans Rust and Go services, Firecracker, KVM, Linux networking, ublk, OverlayBD, snapshots, and distributed control-plane components. Focused reports and changes are essential for safe review.

## Before opening an issue

Search open and closed issues and GitHub Discussions first.

Use the issue form that best matches the request:

- **Bug report** for a reproducible failure.
- **Performance regression** for a measured comparison between exact versions.
- **Feature request** for a concrete user problem with acceptance criteria.
- **Documentation issue** for incorrect, missing, outdated, or unclear documentation.

Usage questions, early ideas, and open-ended design discussion belong in GitHub Discussions rather than the issue tracker.

Do not report security vulnerabilities in a public issue. Follow [SECURITY.md](SECURITY.md) and use the repository's private security advisory form.

Maintainers may close issues that:

- do not include enough information to reproduce or understand the problem;
- duplicate an existing issue or discussion;
- are general Linux, Firecracker, registry, or infrastructure support requests without an AgentENV defect;
- propose a technology without explaining the user problem or use case;
- contain unverified generated content instead of observed behavior and evidence; or
- remain unanswered after being marked as needing information.

## Bug reports

A useful bug report includes:

- an exact release or commit;
- whether the source was modified;
- Linux distribution, kernel, architecture, and whether the host is bare metal or virtualized;
- KVM, ublk, filesystem, and storage details when relevant;
- the exact command, request, and redacted configuration;
- minimal numbered reproduction steps;
- expected and actual behavior;
- complete relevant logs with timestamps; and
- regression range and reproduction frequency when known.

Never include credentials, registry tokens, private image references, customer data, or other secrets.

## Feature proposals

Start with the problem and use case, not an implementation. Explain:

- who needs the feature and under what workload;
- why current behavior or workarounds are insufficient;
- observable desired behavior and acceptance criteria;
- alternatives considered;
- compatibility and operational impact; and
- explicit non-goals.

Large changes to APIs, sandbox lifecycle, snapshot or storage formats, distributed control-plane behavior, or host setup should have design agreement before implementation begins. Acceptance of an issue does not imply approval of a particular design.

## Pull requests

Before writing a substantial change, open or find an issue and align on the approach. Small fixes and documentation improvements may be submitted directly when their scope is obvious.

Keep each pull request focused on one coherent change:

- do not mix features, broad refactors, dependency updates, and unrelated formatting;
- explain non-obvious lifecycle, concurrency, storage, and failure-handling decisions;
- add tests for new behavior and regressions;
- update documentation and configuration examples;
- call out API, configuration, snapshot format, artifact layout, dependency, host-permission, and rollback impact; and
- use Conventional Commit prefixes such as `feat:`, `fix:`, `refactor:`, `ci:`, and `chore:`.

Generated code under `thirdparty/`, `src/api/generated/`, and `src/custom_extension_api/generated/` is machine-managed. Change the source schema and use the documented `make` target instead of manually editing generated files.

## Build and test

Common checks from the repository root are:

```bash
make
make fmt
make clippy
make test
make test-unit
make test-integration
make bench
```

For changes under `services/`, also run:

```bash
make -C services test
```

Use the narrowest relevant test while developing, then run the applicable repository checks before requesting review. Integration tests require root, `/dev/kvm`, network namespace support; if a required check cannot run in your environment, state that clearly in the pull request.

## Generated APIs

Use the existing code generation targets:

```bash
make firecracker-client
make envd-http-client
make agentenv-server
make custom-extension-client
```

Include both the schema/source change and regenerated output in the same pull request. The custom extension generator does not remove obsolete model files automatically, so remove orphaned generated files after schema deletions.

## Review expectations

Reviewers may request that a pull request be split when independent changes can be reviewed or reverted separately. A pull request is ready for review when:

- its purpose and scope are clear;
- relevant tests pass or skipped checks are explained;
- compatibility and operational risks are documented;
- generated files are reproducible from their source definitions; and
- commits and discussion do not expose sensitive information.

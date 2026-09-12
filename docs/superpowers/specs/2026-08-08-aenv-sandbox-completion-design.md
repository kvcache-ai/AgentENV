# AENV sandbox-ID dynamic completion

## Goal

Add a local-only, first follow-up to merged PR #89: complete sandbox IDs for
the existing `aenv` CLI without changing the AgentENV server API. Dynamic
lookup must be fast and invisible on failure; static completion supplied by
`aenv completion bash|zsh|fish` must remain available in every case.

## Scope

This change completes only sandbox IDs. It deliberately excludes template and
snapshot names, installer integration, and any server-side completion API.

The completion state policy is centralized rather than embedded in individual
commands:

| Commands | Eligible sandbox state |
| --- | --- |
| `exec`, `upload`, `download`, `pause`, `timeout`, `snapshot create` | `Running` |
| `resume` | `Paused` |
| `connect`, `delete`, `snapshot list --sandbox-id` | `Running` or `Paused` |

## Design

`aenv` gains an internal `__complete` command. The command accepts a small,
versioned request produced by a shell adapter, identifies whether the cursor is
at a supported sandbox-ID argument, and prints matching candidates. It is not
documented as a user-facing API.

The command constructs a completion-specific `Client` with short connection
and total request timeouts. It calls the existing `GET /v2/sandboxes` endpoint,
filters `ListedSandbox` records by the policy above and by the typed prefix,
deduplicates IDs, and writes only machine-readable candidates to stdout.

Missing credentials, non-success responses, malformed output, unavailable
servers, and timeouts produce no candidates and no stderr output. Completion
must never use the normal CLI client's five-second connect or 120-second total
timeout.

The generated Bash, Zsh, and Fish static scripts will call the internal command
only at the supported argument positions. The static generator remains the
source of truth for all other commands, flags, aliases, enum values, and path
arguments. Shell-specific glue translates the shared candidate format into each
shell's candidate/description convention.

## Candidate output

The first version uses tab-separated records:

```text
<sandbox-id>\t<alias or template ID> (<state>)
```

The first field is always the inserted value. Shell adapters may ignore the
description where their completion system does not safely render it.

## Testing

- Unit-test command/argument recognition and the state-policy table.
- Test prefix filtering, deduplication, stable ordering, and candidate
  serialization.
- Use a local mock HTTP server to test successful lookup and every failure
  fallback. Assert the fallback is empty and writes nothing to stderr.
- Snapshot-test the generated shell wrappers for Bash, Zsh, and Fish and test
  that they preserve static completion outside a sandbox-ID position.
- Run `cargo fmt --all -- --check`, `cargo check -p aenv --bin aenv`, and the
  focused `aenv` test suite.

## Delivery order

1. Add the isolated policy, lookup, output protocol, and tests.
2. Add shell adapters for Bash, Zsh, and Fish on top of the shared protocol.
3. Verify the generated static scripts continue to expose all PR #89 behavior.
4. In a separate follow-up, handle template/snapshot names and `start --cold`.
5. In a later follow-up, add installer and uninstaller integration.

## Risks and mitigations

- **Shell integration is the slowest part.** Generated `clap_complete` scripts
  do not provide a stable dynamic hook. Keep adapters small, assert their
  generated text in tests, and avoid coupling the core lookup logic to one
  shell.
- **Completion can make interactive shells feel broken.** The completion-only
  client uses sub-second timeouts and suppresses all runtime errors.
- **Command semantics can drift.** One state-policy table and command-level
  tests make changes to command applicability explicit.

## Non-goals

- Changing `clap` to its unstable dynamic-completion API.
- Adding persistent caches or new server endpoints.
- Editing users' shell startup files.

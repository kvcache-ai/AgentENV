# AENV CLI Dynamic Completion: AgentENV-Owned Hook

## Summary

This experiment keeps the static completion generator introduced by PR #89 and
adds a small shell adapter that calls a hidden internal command:

```text
aenv __complete --index <cursor> -- <shell words...>
```

The hook reads the existing sandbox list API, filters IDs by the command's
expected state, and prints one candidate per line. Network errors, missing
credentials, and unsupported cursor positions are silent so completion never
breaks the command line.

## Supported dynamic positions

- Running sandbox: `pause`, `exec`, `upload`, `download`, `timeout`, and
  `snapshot create`
- Paused sandbox: `resume`
- Active sandbox (running or paused): `connect`/`cn`, `delete`/`rm`, and
  `snapshot list --sandbox-id`

The shell-specific adapter is appended to the generated Bash, Zsh, or Fish
script. Bash and Zsh fall back to the static adapter when the hook returns no
results; Fish simply contributes no dynamic candidates in that case.

## Why this is an alternative

Unlike the `clap_complete` `unstable-dynamic` experiment, this approach does
not depend on Clap's unstable shell protocol. The protocol and command shape
are owned by AgentENV, which makes the implementation easier to inspect and
extend, but requires maintaining three small shell adapters and the cursor
interpretation logic.

This is intentionally a parallel design for issue #37. The two branches are
not intended to be merged together; maintainers can choose the API and
maintenance trade-off they prefer.

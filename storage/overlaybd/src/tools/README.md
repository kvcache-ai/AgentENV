# OverlayBD OCI Conversion Tools

This directory contains the Rust-side conversion helpers for OCI -> overlaybd
layer conversion.

The actual `overlaybd-create`, `overlaybd-apply`, and `overlaybd-commit`
binaries are **not** committed to this repository. AgentENV setup downloads the
configured pinned static OverlayBD release archive at startup and extracts the
tools into:

- `<deps_path>/overlaybd/bin`

The pinned release tag and asset URL live in `config/deps_manifest.toml` under
`[overlaybd]`. The tools are statically linked and do not require an
OverlayBD-specific shared-library directory or `LD_LIBRARY_PATH`.

Because AgentENV currently uses the original upstream `overlaybd-apply`
behavior, setup also installs the packaged default config to:

- `/etc/overlaybd/overlaybd.json`

Use `overlaybd::tools::OverlaybdTools::from_overlaybd_install_root(...)` to
point the wrapper at that extracted installation root.

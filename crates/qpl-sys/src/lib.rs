//! Raw FFI bindings to the Intel Query Processing Library (QPL) 1.9.0.
//!
//! `bindings.rs` is generated from the QPL 1.9.0 headers and keeps only the
//! symbols AgentENV uses. `build.rs` links the system `libqpl` found by
//! pkg-config and rejects other versions. Callers own all safety invariants.
//!
//! Regenerate from the repository root with bindgen 0.72.1 and the QPL 1.9.0
//! headers installed under `/usr/include`:
//!
//! ```sh
//! bindgen /usr/include/qpl/qpl.h \
//!     --output crates/qpl-sys/src/bindings.rs --formatter prettyplease \
//!     --no-doc-comments --no-prepend-enum-name --merge-extern-blocks \
//!     --allowlist-function 'qpl_(get_job_size|init_job|execute_job|submit_job|wait_job|fini_job|get_safe_deflate_compression_buffer_size|get_library_version)' \
//!     --allowlist-type 'qpl_(job|path_t|status|operation|compression_levels)' \
//!     --allowlist-var 'QPL_(FLAG|DEVICE_NUMA_ID)_.*'
//! ```
#![cfg(target_arch = "x86_64")]
#![allow(non_camel_case_types, non_snake_case, non_upper_case_globals)]

include!("bindings.rs");

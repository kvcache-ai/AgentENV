//! Links the system QPL found by pkg-config.
//!
//! `src/bindings.rs` is generated from the QPL 1.9.0 headers, so any other
//! installed version is rejected. A missing QPL only warns, so `cargo check`
//! and `cargo clippy` still work; `-lqpl` is emitted anyway so the final link
//! fails with `cannot find -lqpl`.

const QPL_VERSION: &str = "1.9.0";

fn main() {
    if std::env::var("CARGO_CFG_TARGET_ARCH").as_deref() != Ok("x86_64") {
        return;
    }

    if pkg_config::Config::new()
        .exactly_version(QPL_VERSION)
        .probe("qpl")
        .is_ok()
    {
        return;
    }

    if let Ok(lib) = pkg_config::Config::new().cargo_metadata(false).probe("qpl") {
        panic!(
            "qpl-sys requires QPL {QPL_VERSION}, but pkg-config found QPL {}; \
             the bindings in src/bindings.rs only match QPL {QPL_VERSION}",
            lib.version
        );
    }

    println!(
        "cargo:warning=QPL {QPL_VERSION} not found by pkg-config; linking will fail with \
         `cannot find -lqpl`. Install QPL {QPL_VERSION} (shared library with qpl.pc) and set \
         PKG_CONFIG_PATH if it is outside the default search path."
    );
    println!("cargo:rustc-link-lib=qpl");
}

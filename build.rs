//! Build-script: embed the release version so the running binary can report
//! it (GET /v1/about, mesh software_version). Resolution order:
//! 1. `GITHUB_REF_NAME` when it is a `v*` tag (the workflow cuts these) → strip `v`.
//! 2. else `CARGO_PKG_VERSION` (the `[package].version` in Cargo.toml).
fn main() {
    let version = std::env::var("GITHUB_REF_NAME")
        .ok()
        .filter(|r| r.starts_with('v'))
        .map(|r| r.trim_start_matches('v').to_owned())
        .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_owned());
    println!("cargo:rustc-env=TABBIFY_BUILD_VERSION={version}");
    // Re-run when the tag changes so a retag re-embeds the new version.
    println!("cargo:rerun-if-env-changed=GITHUB_REF_NAME");
}

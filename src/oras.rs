//! OCI artifact pull via the `oras` CLI — the registry-pull seam for the
//! `wasm-http` runtime.
//!
//! The mesh OCI registry (Zot) serves plain HTTP over the encrypted WireGuard
//! tunnel; `oras pull --plain-http` is required for any `[ula]:5000` ref.
//!
//! This module mirrors the injectable [`CommandRunner`] seam used by
//! [`crate::docker`] so that tests can inject a fake runner that writes a
//! `.wasm` file into the output directory instead of invoking a real `oras`
//! binary.

use std::path::{Path, PathBuf};

use crate::docker::CommandRunner;

/// Build the `oras pull` argument list (sans the leading binary name).
///
/// Always includes `--plain-http` because the mesh registry is served over
/// plain HTTP on the encrypted WireGuard overlay (`[ula]:5000`).
///
/// # Example
/// ```
/// # use tabbify_service_supervisor::oras::oras_pull_args;
/// let args = oras_pull_args("[fd5a:1f02::1]:5000/acme/app:sha256abc", "/tmp/pulled");
/// assert_eq!(args[0], "pull");
/// assert!(args.contains(&"--plain-http".to_owned()));
/// ```
#[must_use]
pub fn oras_pull_args(reff: &str, out_dir: &str) -> Vec<String> {
    vec![
        "pull".to_owned(),
        "--plain-http".to_owned(),
        reff.to_owned(),
        "-o".to_owned(),
        out_dir.to_owned(),
    ]
}

/// Run `<oras_bin> pull --plain-http <reff> -o <out_dir>` via the injected
/// `runner`. Returns `true` iff the command exits successfully.
///
/// The injectable [`CommandRunner`] (same type as in [`crate::docker`]) lets
/// tests record the exact argv without invoking a real `oras` binary.
pub async fn oras_pull(oras_bin: &str, reff: &str, out_dir: &Path, runner: &CommandRunner) -> bool {
    let out_dir_str = out_dir.to_string_lossy().into_owned();
    // Prepend the binary name as the first element so a production runner
    // knows which binary to exec. The production runner (re-used from docker)
    // already receives the binary name separately; we inject it here for
    // symmetry and so a recording test runner can assert the full argv.
    let _ = oras_bin; // oras_bin is used by the production_oras_runner, not the seam args
    let args = oras_pull_args(reff, &out_dir_str);
    (runner)(args).await
}

/// Scan `dir` for the first `*.wasm` file and return its path.
///
/// `oras pull` restores the artifact under its original pushed filename, so
/// the supervisor only needs to find any `.wasm` in the output directory after
/// a successful pull.
///
/// Returns `None` if `dir` is not readable or contains no `.wasm` files.
#[must_use]
pub fn find_wasm(dir: &Path) -> Option<PathBuf> {
    let rd = std::fs::read_dir(dir).ok()?;
    for entry in rd.flatten() {
        let p = entry.path();
        if p.extension().and_then(|e| e.to_str()) == Some("wasm") {
            return Some(p);
        }
    }
    None
}

/// Build a production [`CommandRunner`] for `oras`: spawns
/// `<oras_bin> <args>` and returns `true` iff the process exits 0.
///
/// Re-uses the same `Arc<dyn Fn(…) -> BoxFut<…>>` shape as
/// [`crate::docker::production_command_runner`] so the oras and docker seams
/// are structurally identical.
#[must_use]
pub fn production_oras_runner(oras_bin: String) -> CommandRunner {
    use crate::runtime::BoxFut;
    use std::sync::Arc;
    use tokio::process::Command;

    Arc::new(move |args: Vec<String>| {
        let oras_bin = oras_bin.clone();
        let fut: BoxFut<'static, bool> = Box::pin(async move {
            match Command::new(&oras_bin)
                .args(&args)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .await
            {
                Ok(s) => s.success(),
                Err(_) => false,
            }
        });
        fut
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    // ---- oras_pull_args -------------------------------------------------------

    /// The pull args must include `--plain-http` (registry is plain http over
    /// the encrypted mesh), the ref, and the output dir `-o <dir>`.
    #[test]
    fn pull_args_includes_plain_http_and_ref_and_out_dir() {
        let reff = "[fd5a:1f02::1]:5000/acme/app:sha256abc";
        let out = "/tmp/pulled";
        let args = oras_pull_args(reff, out);
        assert_eq!(args[0], "pull", "first arg must be 'pull'");
        assert!(
            args.contains(&"--plain-http".to_owned()),
            "must include --plain-http"
        );
        assert!(args.contains(&reff.to_owned()), "must include the ref");
        assert!(args.contains(&"-o".to_owned()), "must include -o flag");
        assert!(args.contains(&out.to_owned()), "must include out dir");
    }

    /// Exact argv shape: `["pull", "--plain-http", <reff>, "-o", <out_dir>]`.
    #[test]
    fn pull_args_exact_shape() {
        let args = oras_pull_args("reg/app:tag", "/out");
        assert_eq!(
            args,
            vec!["pull", "--plain-http", "reg/app:tag", "-o", "/out"]
        );
    }

    // ---- find_wasm -----------------------------------------------------------

    /// `find_wasm` returns the `.wasm` file in a directory that also contains
    /// other files (e.g. a manifest json that oras may emit alongside).
    #[test]
    fn find_wasm_returns_wasm_file_among_mixed_files() {
        let dir = tempfile::tempdir().unwrap();
        let wasm_path = dir.path().join("app.wasm");
        let other_path = dir.path().join("manifest.json");
        std::fs::write(&wasm_path, b"fake-wasm").unwrap();
        std::fs::write(&other_path, b"{}").unwrap();

        let found = find_wasm(dir.path()).unwrap();
        assert_eq!(found, wasm_path);
    }

    /// `find_wasm` returns `None` for an empty directory.
    #[test]
    fn find_wasm_returns_none_for_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        assert!(find_wasm(dir.path()).is_none());
    }

    /// `find_wasm` returns `None` when the directory has no `.wasm` files.
    #[test]
    fn find_wasm_returns_none_without_wasm_extension() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("manifest.json"), b"{}").unwrap();
        std::fs::write(dir.path().join("app.wat"), b"(module)").unwrap();
        assert!(find_wasm(dir.path()).is_none());
    }

    /// `find_wasm` returns `None` for a path that does not exist.
    #[test]
    fn find_wasm_returns_none_for_nonexistent_dir() {
        assert!(find_wasm(Path::new("/does/not/exist/at/all")).is_none());
    }

    // ---- oras_pull (seam) ----------------------------------------------------

    /// The runner is called with the correct oras argv and the function returns
    /// `true` when the runner succeeds.
    #[tokio::test]
    async fn oras_pull_calls_runner_with_correct_args() {
        use std::sync::{Arc, Mutex};

        let captured: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
        let cap2 = captured.clone();
        let runner: CommandRunner = Arc::new(move |args: Vec<String>| {
            cap2.lock().unwrap().push(args);
            Box::pin(async { true })
        });

        let reff = "[fd5a::1]:5000/myapp:latest";
        let out_dir = std::path::Path::new("/tmp/oras-test");
        let ok = oras_pull("oras", reff, out_dir, &runner).await;
        assert!(ok, "runner returned true → oras_pull must return true");

        let calls = captured.lock().unwrap();
        assert_eq!(calls.len(), 1, "runner must be called exactly once");
        let argv = &calls[0];
        assert!(
            argv.contains(&"--plain-http".to_owned()),
            "argv must contain --plain-http; got {argv:?}"
        );
        assert!(
            argv.contains(&reff.to_owned()),
            "argv must contain the ref; got {argv:?}"
        );
    }

    /// A failing runner causes `oras_pull` to return `false`.
    #[tokio::test]
    async fn oras_pull_returns_false_on_runner_failure() {
        use std::sync::Arc;
        let runner: CommandRunner = Arc::new(|_| Box::pin(async { false }));
        let ok = oras_pull("oras", "reg/app:v1", Path::new("/tmp/x"), &runner).await;
        assert!(!ok);
    }
}

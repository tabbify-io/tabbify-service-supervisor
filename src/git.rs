//! Secure `git clone` helper for the build-runner.
//!
//! This module provides a non-leaking clone path: when a short-lived token is
//! present the token is **never** placed in the process argv (visible via `ps`
//! or `/proc/<pid>/cmdline`). Instead:
//!
//! 1. The clone URL gets only a non-secret username injected
//!    (`x-access-token@`) — no token in the URL.
//! 2. The token is written to a `0600` tempfile; a tiny `0700` askpass script
//!    (`cat <tokenfile>`) is written next to it.
//! 3. Git calls the askpass script for its password prompt, reading the token
//!    from the file. `GIT_TERMINAL_PROMPT=0` ensures git never blocks if the
//!    askpass fails.
//! 4. Both files are deleted (best-effort) after the clone — on success AND on
//!    error.
//!
//! The [`GitRun`] seam lets tests record the argv + env without invoking git.

use std::future::Future;
use std::path::Path;
use std::pin::Pin;

use anyhow::{Context, Result, bail};

// ── seam ─────────────────────────────────────────────────────────────────────

/// Injectable git command executor.
///
/// Receives the git sub-command argument list and a list of extra environment
/// variables to set, then runs (or records) the command.
///
/// The seam allows tests to assert on argv + env without spawning a real
/// `git` process.
pub type GitRun = std::sync::Arc<
    dyn Fn(Vec<String>, Vec<(String, String)>) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>
        + Send
        + Sync,
>;

/// Build the production [`GitRun`]: spawns `<git_bin> <args>` with the given
/// extra environment variables and returns `Ok(())` on exit 0.
///
/// # Errors
/// Spawn failure or a non-zero exit status.
pub fn real_git_run(git_bin: String) -> GitRun {
    std::sync::Arc::new(move |args, env| {
        let git_bin = git_bin.clone();
        Box::pin(async move {
            let mut cmd = tokio::process::Command::new(&git_bin);
            cmd.args(&args);
            for (k, v) in &env {
                cmd.env(k, v);
            }
            let status = cmd.status().await.context("spawn git")?;
            if !status.success() {
                bail!("git {:?} failed: {status}", args.first());
            }
            Ok(())
        })
    })
}

// ── public API ────────────────────────────────────────────────────────────────

/// Clone `repo_url` at `git_ref` into `dest` using `runner`.
///
/// When `token` is `Some`:
/// - `repo_url` must be an `https://` URL. The username `x-access-token` is
///   injected after the scheme (`https://x-access-token@…`). The token itself
///   is **not** placed in the URL or in git's argv.
/// - The token is written to `<dest_parent>/.tabbify-git-token` (mode `0600`).
/// - A tiny askpass script (`#!/bin/sh\ncat <tokenfile>\n`, mode `0700`) is
///   written to `<dest_parent>/.tabbify-askpass.sh`.
/// - `GIT_ASKPASS=<script>` and `GIT_TERMINAL_PROMPT=0` are set for git.
/// - Both files are removed after the clone (best-effort, even on error).
///
/// When `token` is `None` the URL is used as-is and no askpass env is set
/// (suitable for public repositories).
///
/// # Errors
/// I/O errors writing the askpass files, or a non-zero exit from git.
pub async fn clone(
    repo_url: &str,
    git_ref: &str,
    token: Option<&str>,
    dest: &Path,
    runner: &GitRun,
) -> Result<()> {
    // Parent directory for the token + askpass files.
    let parent = dest
        .parent()
        .with_context(|| format!("dest path has no parent: {}", dest.display()))?;

    if let Some(tok) = token {
        let token_path = parent.join(".tabbify-git-token");
        let askpass_path = parent.join(".tabbify-askpass.sh");

        // Write token file (0600).
        write_secret_file(&token_path, tok, 0o600)?;

        // Write askpass script (0700): `#!/bin/sh\ncat "<token_path>"\n`.
        let script = format!("#!/bin/sh\ncat \"{}\"\n", token_path.to_string_lossy());
        write_secret_file(&askpass_path, &script, 0o700)?;

        // Inject the non-secret username into the URL.
        let url = inject_username(repo_url, "x-access-token");

        let dest_str = dest.to_string_lossy().into_owned();
        let args = vec![
            "clone".to_owned(),
            "--depth".to_owned(),
            "1".to_owned(),
            "--branch".to_owned(),
            git_ref.to_owned(),
            url,
            dest_str,
        ];
        let env = vec![
            (
                "GIT_ASKPASS".to_owned(),
                askpass_path.to_string_lossy().into_owned(),
            ),
            ("GIT_TERMINAL_PROMPT".to_owned(), "0".to_owned()),
        ];

        let result = runner(args, env).await;

        // Best-effort cleanup — always, regardless of success or failure.
        let _ = std::fs::remove_file(&token_path);
        let _ = std::fs::remove_file(&askpass_path);

        result
    } else {
        // Public repo: plain clone, no askpass env.
        let dest_str = dest.to_string_lossy().into_owned();
        let args = vec![
            "clone".to_owned(),
            "--depth".to_owned(),
            "1".to_owned(),
            "--branch".to_owned(),
            git_ref.to_owned(),
            repo_url.to_owned(),
            dest_str,
        ];
        runner(args, vec![]).await
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// Inject `username` into an `https://` URL.
///
/// `https://github.com/…` → `https://<username>@github.com/…`
///
/// If the URL already contains a userinfo component or is not an `https://`
/// URL it is returned unchanged (best-effort — callers should only pass well-
/// formed `https://` URLs without pre-existing credentials).
pub fn inject_username(url: &str, username: &str) -> String {
    const PREFIX: &str = "https://";
    if !url.starts_with(PREFIX) {
        // Not https or already has a different scheme — return unchanged.
        return url.to_owned();
    }
    let rest = &url[PREFIX.len()..];
    // If userinfo is already present (contains '@' before the first '/'),
    // return unchanged to avoid double-injection.
    let host_part = rest.split('/').next().unwrap_or(rest);
    if host_part.contains('@') {
        return url.to_owned();
    }
    format!("{PREFIX}{username}@{rest}")
}

/// Write `content` to `path` with the given Unix permission bits, creating
/// the file if it does not exist and truncating if it does.
///
/// Uses [`std::os::unix::fs::PermissionsExt`] to enforce the mode immediately
/// after creation so the file is never briefly world-readable.
///
/// # Errors
/// Any I/O error creating, writing, or `chmod`-ing the file.
fn write_secret_file(path: &Path, content: &str, mode: u32) -> Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::PermissionsExt as _;

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .with_context(|| format!("create secret file {}", path.display()))?;

    // Restrict permissions before writing any secret content.
    file.set_permissions(std::fs::Permissions::from_mode(mode))
        .with_context(|| format!("chmod {:o} {}", mode, path.display()))?;

    file.write_all(content.as_bytes())
        .with_context(|| format!("write secret file {}", path.display()))?;

    Ok(())
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    // -- inject_username -------------------------------------------------------

    #[test]
    fn inject_username_adds_x_access_token() {
        assert_eq!(
            inject_username("https://github.com/acme/app.git", "x-access-token"),
            "https://x-access-token@github.com/acme/app.git"
        );
    }

    #[test]
    fn inject_username_leaves_non_https_unchanged() {
        let url = "git@github.com:acme/app.git";
        assert_eq!(inject_username(url, "x-access-token"), url);
    }

    #[test]
    fn inject_username_leaves_already_credentialled_url_unchanged() {
        let url = "https://existing-user@github.com/acme/app.git";
        assert_eq!(inject_username(url, "x-access-token"), url);
    }

    #[test]
    fn inject_username_works_without_dot_git_suffix() {
        assert_eq!(
            inject_username("https://github.com/acme/app", "x-access-token"),
            "https://x-access-token@github.com/acme/app"
        );
    }

    // -- clone with token: token must not appear in argv ----------------------

    /// The cardinal security property: a short-lived token must NEVER appear
    /// in the git argv (which is visible to other processes via `ps` or
    /// `/proc/<pid>/cmdline`). The token must live only in the 0600 token file.
    #[tokio::test]
    async fn clone_with_token_keeps_token_out_of_argv() {
        let dir = tempfile::tempdir().unwrap();
        let recorded = std::sync::Arc::new(std::sync::Mutex::new((
            Vec::<String>::new(),
            Vec::<(String, String)>::new(),
        )));
        let r = recorded.clone();
        let runner: GitRun = std::sync::Arc::new(move |args, env| {
            *r.lock().unwrap() = (args.clone(), env.clone());
            Box::pin(async { Ok(()) })
        });

        clone(
            "https://github.com/acme/app.git",
            "v1",
            Some("ghs_SECRETtoken"),
            &dir.path().join("src"),
            &runner,
        )
        .await
        .unwrap();

        let (args, env) = recorded.lock().unwrap().clone();

        // Security invariant: token must NOT appear anywhere in argv.
        assert!(
            args.iter().all(|a| !a.contains("ghs_SECRETtoken")),
            "token must NOT be in argv: {args:?}"
        );
        // The non-secret username must be in the URL argument.
        assert!(
            args.iter().any(|a| a.contains("x-access-token@github.com")),
            "url must carry non-secret username: {args:?}"
        );
        // GIT_ASKPASS must be set.
        assert!(
            env.iter().any(|(k, _)| k == "GIT_ASKPASS"),
            "GIT_ASKPASS must be set in env"
        );
        // GIT_TERMINAL_PROMPT=0 must be set.
        assert!(
            env.iter()
                .any(|(k, v)| k == "GIT_TERMINAL_PROMPT" && v == "0"),
            "GIT_TERMINAL_PROMPT=0 must be set"
        );
    }

    /// The token file and askpass script must be cleaned up after a successful
    /// clone (the fake runner never reads them, but they are created on disk).
    #[tokio::test]
    async fn clone_with_token_cleans_up_files_on_success() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("src");
        let token_file = dir.path().join(".tabbify-git-token");
        let askpass_file = dir.path().join(".tabbify-askpass.sh");

        let runner: GitRun = std::sync::Arc::new(|_args, _env| Box::pin(async { Ok(()) }));

        clone(
            "https://github.com/acme/app.git",
            "main",
            Some("ghs_TOKEN"),
            &dest,
            &runner,
        )
        .await
        .unwrap();

        assert!(
            !token_file.exists(),
            "token file must be removed after clone"
        );
        assert!(
            !askpass_file.exists(),
            "askpass script must be removed after clone"
        );
    }

    /// The token file and askpass script must also be cleaned up when the
    /// runner returns an error.
    #[tokio::test]
    async fn clone_with_token_cleans_up_files_on_error() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("src");
        let token_file = dir.path().join(".tabbify-git-token");
        let askpass_file = dir.path().join(".tabbify-askpass.sh");

        let runner: GitRun = std::sync::Arc::new(|_args, _env| {
            Box::pin(async { Err(anyhow::anyhow!("git clone failed")) })
        });

        let _ = clone(
            "https://github.com/acme/app.git",
            "main",
            Some("ghs_TOKEN"),
            &dest,
            &runner,
        )
        .await;

        assert!(
            !token_file.exists(),
            "token file must be removed even on error"
        );
        assert!(
            !askpass_file.exists(),
            "askpass script must be removed even on error"
        );
    }

    // -- clone without token: plain URL, no askpass ---------------------------

    /// Without a token the original URL must be passed verbatim and GIT_ASKPASS
    /// must NOT be set (no unnecessary env).
    #[tokio::test]
    async fn clone_without_token_uses_plain_url_and_no_askpass() {
        let dir = tempfile::tempdir().unwrap();
        let recorded = std::sync::Arc::new(std::sync::Mutex::new((
            Vec::<String>::new(),
            Vec::<(String, String)>::new(),
        )));
        let r = recorded.clone();
        let runner: GitRun = std::sync::Arc::new(move |args, env| {
            *r.lock().unwrap() = (args, env);
            Box::pin(async { Ok(()) })
        });

        clone(
            "https://github.com/acme/pub.git",
            "main",
            None,
            &dir.path().join("src"),
            &runner,
        )
        .await
        .unwrap();

        let (args, env) = recorded.lock().unwrap().clone();

        assert!(
            args.iter().any(|a| a == "https://github.com/acme/pub.git"),
            "original URL must be in argv for public clone: {args:?}"
        );
        assert!(
            !env.iter().any(|(k, _)| k == "GIT_ASKPASS"),
            "GIT_ASKPASS must NOT be set for public clone"
        );
    }

    // -- real git clone (ignored, network-dependent) --------------------------

    /// Smoke-test against a real tiny public repo. Run with:
    ///   `cargo test --lib git::tests::real_clone_public_repo -- --ignored`
    #[tokio::test]
    #[ignore]
    async fn real_clone_public_repo() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("repo");
        let runner = real_git_run("git".to_owned());
        clone(
            "https://github.com/nicowillis/hello-world.git",
            "main",
            None,
            &dest,
            &runner,
        )
        .await
        .unwrap();
        assert!(dest.exists(), "cloned repo directory must exist");
    }
}

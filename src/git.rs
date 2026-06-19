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
            // Capture stderr (via `output`) instead of inheriting it, so a
            // non-zero exit can surface git's own diagnostic in the bailed
            // error rather than an opaque status string.
            let output = cmd.output().await.context("spawn git")?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                bail!(format_git_error(&args, output.status.code(), &stderr));
            }
            Ok(())
        })
    })
}

/// Injectable git command executor that CAPTURES stdout.
///
/// Sibling to [`GitRun`]: where `GitRun` discards stdout (it only cares about
/// success/failure of side-effecting commands like `init`/`fetch`/`checkout`),
/// this seam runs a read-only git query (`rev-parse HEAD`) and returns its
/// stdout so the builder can learn the resolved commit SHA. Kept as a separate
/// type so the existing clone path + its tests are untouched.
///
/// # Errors
/// Spawn failure or a non-zero exit status (the `Result<String>` carries the
/// captured stdout on success).
pub type GitCapture = std::sync::Arc<
    dyn Fn(Vec<String>) -> Pin<Box<dyn Future<Output = Result<String>> + Send>> + Send + Sync,
>;

/// Build the production [`GitCapture`]: spawns `<git_bin> <args>`, captures
/// stdout, and returns it as a `String` on exit 0 (bails with git's stderr
/// otherwise).
///
/// # Errors
/// Spawn failure or a non-zero exit status.
pub fn real_git_capture(git_bin: String) -> GitCapture {
    std::sync::Arc::new(move |args| {
        let git_bin = git_bin.clone();
        Box::pin(async move {
            let output = tokio::process::Command::new(&git_bin)
                .args(&args)
                .output()
                .await
                .context("spawn git")?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                bail!(format_git_error(&args, output.status.code(), &stderr));
            }
            Ok(String::from_utf8_lossy(&output.stdout).into_owned())
        })
    })
}

/// Resolve the commit SHA that a shallow clone left checked out at HEAD.
///
/// After [`clone`] runs `git fetch --depth 1 origin <ref>` + `git checkout
/// FETCH_HEAD`, the working tree is at a DETACHED HEAD pinned to the fetched
/// commit. `git -C <dest> rev-parse HEAD` prints that commit's 40-hex SHA.
///
/// This is the FAIL-CLOSED seam for tagging the built image with an IMMUTABLE
/// commit SHA instead of the (possibly mutable) branch/tag ref: when the
/// requested ref is a branch like `main`, the image must be tagged with the
/// resolved commit, never the floating branch name. If the SHA cannot be
/// proven (the command fails, or the output is not a 40-char lowercase hex
/// string), this returns an `Err` so the build aborts rather than shipping a
/// mutable tag.
///
/// # Errors
/// The git command failed, or its stdout was not a valid 40-char lowercase hex
/// commit SHA.
pub async fn resolve_cloned_head(dest: &Path, capture: &GitCapture) -> Result<String> {
    let dest_str = dest.to_string_lossy().into_owned();
    let out = capture(vec![
        "-C".to_owned(),
        dest_str,
        "rev-parse".to_owned(),
        "HEAD".to_owned(),
    ])
    .await
    .context("git rev-parse HEAD")?;
    let sha = out.trim();
    if !is_commit_sha(sha) {
        bail!(
            "git rev-parse HEAD did not yield a 40-char lowercase hex commit sha (got {sha:?}); \
             refusing to tag the image with an unproven commit"
        );
    }
    Ok(sha.to_owned())
}

/// Is `s` a full 40-character lowercase hexadecimal commit SHA?
///
/// We require the canonical full SHA (not an abbreviation, not uppercase) so the
/// image tag is unambiguous and matches the lowercase-only OCI tag charset.
fn is_commit_sha(s: &str) -> bool {
    s.len() == 40 && s.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Maximum number of bytes of git stderr to keep in an error message. We keep
/// the *tail* because git prints its decisive `fatal:`/`error:` line last.
const MAX_STDERR_TAIL: usize = 2_000;

/// Format a non-opaque error for a failed git invocation.
///
/// The argv we build always starts with either a global flag (`-C <dest>`) or
/// the subcommand itself (`init`). The old code printed `args.first()`, which
/// surfaced the meaningless `Some("-C")`. This instead picks the *meaningful*
/// subcommand (the first argv element that is neither the `-C` global flag, its
/// path operand, nor any dash-prefixed flag) and appends the exit code plus the
/// tail of git's stderr — e.g.
///
/// ```text
/// git fetch failed (128): fatal: Repository not found
/// ```
///
/// The token never reaches argv (it lives only in the askpass file), so the
/// argv is safe to echo. git's stderr likewise does not carry the token (the
/// askpass script `cat`s it directly to git's password prompt, not to stderr).
fn format_git_error(args: &[String], code: Option<i32>, stderr: &str) -> String {
    let subcommand = git_subcommand(args);
    let code_str = match code {
        Some(c) => c.to_string(),
        None => "signal".to_owned(),
    };

    let trimmed = stderr.trim();
    // Keep only the tail of large stderr so the message stays bounded.
    let tail: String = if trimmed.len() > MAX_STDERR_TAIL {
        let start = trimmed.len() - MAX_STDERR_TAIL;
        // Snap to a char boundary so slicing never panics on multibyte input.
        let start = (start..=trimmed.len())
            .find(|&i| trimmed.is_char_boundary(i))
            .unwrap_or(trimmed.len());
        format!("…{}", &trimmed[start..])
    } else {
        trimmed.to_owned()
    };

    if tail.is_empty() {
        format!("git {subcommand} failed ({code_str})")
    } else {
        format!("git {subcommand} failed ({code_str}): {tail}")
    }
}

/// Extract the meaningful git subcommand from the argv we construct.
///
/// Skips the `-C` global flag and its directory operand, and skips any other
/// dash-prefixed flag, returning the first plain token (e.g. `fetch`,
/// `checkout`, `init`, `remote`). Falls back to `"?"` if none is found.
fn git_subcommand(args: &[String]) -> &str {
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if arg == "-C" {
            // The `-C` flag takes a path operand; skip it too.
            iter.next();
            continue;
        }
        if arg.starts_with('-') {
            continue;
        }
        return arg;
    }
    "?"
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

    // ── multi-step impl ──────────────────────────────────────────────────────
    //
    // `git clone --branch <ref>` only accepts BRANCH or TAG names — it rejects a
    // commit SHA with "Remote branch X not found in upstream origin". GitHub's
    // push webhook delivers `after` as the commit SHA, so a single-step clone is
    // unusable in our pipeline. We unfold to the universal four-step sequence:
    //
    //   1. `git init -q <dest>`                  (no auth)
    //   2. `git -C <dest> remote add origin <U>` (no auth; URL carries non-secret
    //                                             username when a token is set)
    //   3. `git -C <dest> fetch --depth 1 origin <ref>` (auth via askpass; works
    //                                             for SHA, branch, AND tag because
    //                                             `git fetch <ref>` is universal)
    //   4. `git -C <dest> checkout FETCH_HEAD`   (no auth)
    //
    // This stays shallow (one-commit fetch) and supports every kind of ref
    // GitHub sends. The token's lifetime is still scoped to the fetch step:
    // the askpass tempfiles are written just before step 3 and removed
    // immediately after, win or lose.

    // Ensure dest's parent exists before any git command; `git init` will
    // create `dest` itself.
    if !parent.as_os_str().is_empty() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create dest parent dir {}", parent.display()))?;
    }
    let dest_str = dest.to_string_lossy().into_owned();

    // Step 1: git init <dest>.
    runner(
        vec!["init".to_owned(), "-q".to_owned(), dest_str.clone()],
        vec![],
    )
    .await
    .context("git init")?;

    // Step 2: git -C <dest> remote add origin <url>. The URL carries the
    // non-secret `x-access-token` username when a token is present so git's
    // askpass is triggered by the credential prompt (which our wrapper script
    // satisfies from the 0600 token file).
    let url = if token.is_some() {
        inject_username(repo_url, "x-access-token")
    } else {
        repo_url.to_owned()
    };
    runner(
        vec![
            "-C".to_owned(),
            dest_str.clone(),
            "remote".to_owned(),
            "add".to_owned(),
            "origin".to_owned(),
            url,
        ],
        vec![],
    )
    .await
    .context("git remote add")?;

    // Step 3: git -C <dest> fetch --depth 1 origin <ref>. This is the only
    // step that needs the token, so the askpass tempfiles live only for the
    // duration of this call.
    let (env, token_paths) = if let Some(tok) = token {
        let token_path = parent.join(".tabbify-git-token");
        let askpass_path = parent.join(".tabbify-askpass.sh");

        write_secret_file(&token_path, tok, 0o600)?;
        let script = format!("#!/bin/sh\ncat \"{}\"\n", token_path.to_string_lossy());
        write_secret_file(&askpass_path, &script, 0o700)?;

        let env = vec![
            (
                "GIT_ASKPASS".to_owned(),
                askpass_path.to_string_lossy().into_owned(),
            ),
            ("GIT_TERMINAL_PROMPT".to_owned(), "0".to_owned()),
        ];
        (env, Some((token_path, askpass_path)))
    } else {
        (vec![], None)
    };

    let fetch_result = runner(
        vec![
            "-C".to_owned(),
            dest_str.clone(),
            "fetch".to_owned(),
            "--depth".to_owned(),
            "1".to_owned(),
            "origin".to_owned(),
            git_ref.to_owned(),
        ],
        env,
    )
    .await;

    // Clean up secret files immediately after the fetch — always, regardless
    // of success or failure.
    if let Some((token_path, askpass_path)) = token_paths {
        let _ = std::fs::remove_file(&token_path);
        let _ = std::fs::remove_file(&askpass_path);
    }
    fetch_result.context("git fetch")?;

    // Step 4: git -C <dest> checkout FETCH_HEAD.
    runner(
        vec![
            "-C".to_owned(),
            dest_str,
            "checkout".to_owned(),
            "FETCH_HEAD".to_owned(),
        ],
        vec![],
    )
    .await
    .context("git checkout")?;

    Ok(())
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

    // -- format_git_error ------------------------------------------------------

    /// The error must name the meaningful git subcommand (e.g. `fetch`), not the
    /// first argv element which is the `-C` global flag. The old code printed
    /// `git Some("-C") failed`, which was opaque.
    #[test]
    fn format_git_error_names_subcommand_not_dash_c() {
        let args = vec![
            "-C".to_owned(),
            "/tmp/dest".to_owned(),
            "fetch".to_owned(),
            "--depth".to_owned(),
            "1".to_owned(),
            "origin".to_owned(),
            "deadbeef".to_owned(),
        ];
        let msg = format_git_error(&args, Some(128), "fatal: Repository not found\n");
        assert!(
            msg.contains("git fetch"),
            "error must name the `fetch` subcommand: {msg}"
        );
        assert!(
            !msg.contains("Some(\"-C\")"),
            "error must NOT contain the opaque `Some(\"-C\")`: {msg}"
        );
    }

    /// The exit code and the git stderr text must both be surfaced.
    #[test]
    fn format_git_error_includes_code_and_stderr() {
        let args = vec![
            "-C".to_owned(),
            "/tmp/dest".to_owned(),
            "fetch".to_owned(),
            "origin".to_owned(),
            "main".to_owned(),
        ];
        let msg = format_git_error(&args, Some(128), "fatal: Repository not found\n");
        assert!(
            msg.contains("128"),
            "error must include the exit code: {msg}"
        );
        assert!(
            msg.contains("Repository not found"),
            "error must include the git stderr text: {msg}"
        );
    }

    /// A signal-terminated process (no exit code) must still format cleanly.
    #[test]
    fn format_git_error_handles_missing_code() {
        let args = vec!["init".to_owned(), "-q".to_owned(), "/tmp/dest".to_owned()];
        let msg = format_git_error(&args, None, "some failure\n");
        assert!(
            msg.contains("git init"),
            "must still name the subcommand: {msg}"
        );
        assert!(
            msg.contains("some failure"),
            "must still include stderr: {msg}"
        );
        assert!(
            !msg.contains("Some("),
            "must not leak a debug-formatted Option: {msg}"
        );
    }

    /// Empty stderr must not crash and must produce a still-useful message.
    #[test]
    fn format_git_error_handles_empty_stderr() {
        let args = vec![
            "-C".to_owned(),
            "/tmp/dest".to_owned(),
            "checkout".to_owned(),
            "FETCH_HEAD".to_owned(),
        ];
        let msg = format_git_error(&args, Some(1), "");
        assert!(
            msg.contains("git checkout"),
            "must name the subcommand even with empty stderr: {msg}"
        );
        assert!(msg.contains('1'), "must include the exit code: {msg}");
    }

    /// Only the tail of a very long stderr is kept (bounded message size).
    #[test]
    fn format_git_error_keeps_stderr_tail() {
        let args = vec!["-C".to_owned(), "/d".to_owned(), "fetch".to_owned()];
        let long = format!("{}TAIL_MARKER_END", "x".repeat(10_000));
        let msg = format_git_error(&args, Some(128), &long);
        assert!(
            msg.contains("TAIL_MARKER_END"),
            "the tail of stderr (where git's final fatal: line lives) must survive: {msg}"
        );
        assert!(
            msg.len() < 5_000,
            "the message must be bounded, not echo the full 10k stderr: len={}",
            msg.len()
        );
    }

    /// The propagated error from a failing fetch must read meaningfully through
    /// `clone()`'s `.context("git fetch")` wrapper — never `git Some("-C")`.
    /// This drives `clone()` with a stub returning an error carrying git stderr
    /// and asserts the whole chain stays readable.
    #[tokio::test]
    async fn clone_propagates_meaningful_subcommand_and_stderr() {
        let dir = tempfile::tempdir().unwrap();
        // Stub runner: fail any `fetch` call with a formatted, stderr-bearing
        // error mirroring what `real_git_run` now produces.
        let runner: GitRun = std::sync::Arc::new(|args: Vec<String>, _env| {
            let is_fetch = args.iter().any(|a| a == "fetch");
            Box::pin(async move {
                if is_fetch {
                    bail!(format_git_error(
                        &args,
                        Some(128),
                        "fatal: Repository not found\n"
                    ));
                }
                Ok(())
            })
        });

        let err = clone(
            "https://github.com/acme/missing.git",
            "deadbeef",
            None,
            &dir.path().join("src"),
            &runner,
        )
        .await
        .expect_err("clone must fail when fetch fails");

        let chain = format!("{err:#}");
        assert!(
            chain.contains("git fetch"),
            "propagated error must name the failing subcommand: {chain}"
        );
        assert!(
            chain.contains("Repository not found"),
            "propagated error must carry the git stderr: {chain}"
        );
        assert!(
            !chain.contains("Some(\"-C\")"),
            "propagated error must NOT be the opaque `Some(\"-C\")`: {chain}"
        );
    }

    // -- resolve_cloned_head ---------------------------------------------------

    /// A clean 40-char lowercase hex SHA on stdout must be returned verbatim
    /// (trimmed of the trailing newline `git rev-parse` always prints).
    #[tokio::test]
    async fn resolve_cloned_head_returns_trimmed_sha() {
        let sha = "c64f621abcdef0123456789abcdef0123456789a";
        let capture: GitCapture =
            std::sync::Arc::new(move |_args| Box::pin(async move { Ok(format!("{sha}\n")) }));
        let got = resolve_cloned_head(Path::new("/tmp/dest"), &capture)
            .await
            .unwrap();
        assert_eq!(got, sha);
    }

    /// The rev-parse argv must target the clone dest via `-C` and ask for HEAD —
    /// the detached-HEAD commit the shallow fetch+checkout left behind.
    #[tokio::test]
    async fn resolve_cloned_head_invokes_rev_parse_head_in_dest() {
        let recorded: std::sync::Arc<std::sync::Mutex<Vec<String>>> =
            std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let r = recorded.clone();
        let capture: GitCapture = std::sync::Arc::new(move |args: Vec<String>| {
            *r.lock().unwrap() = args;
            Box::pin(async {
                Ok("c64f621abcdef0123456789abcdef0123456789a\n".to_owned())
            })
        });
        resolve_cloned_head(Path::new("/tmp/dest"), &capture)
            .await
            .unwrap();
        let args = recorded.lock().unwrap().clone();
        assert_eq!(
            args,
            vec!["-C", "/tmp/dest", "rev-parse", "HEAD"],
            "must run `git -C <dest> rev-parse HEAD`"
        );
    }

    /// FAIL-CLOSED: a non-SHA stdout (e.g. a branch name leaked through, or
    /// garbage) must produce an Err — a build that cannot prove its commit must
    /// NOT proceed (it would otherwise ship a mutable tag).
    #[tokio::test]
    async fn resolve_cloned_head_rejects_non_sha_output() {
        let capture: GitCapture =
            std::sync::Arc::new(|_args| Box::pin(async { Ok("main\n".to_owned()) }));
        let err = resolve_cloned_head(Path::new("/tmp/dest"), &capture)
            .await
            .expect_err("non-sha output must fail closed");
        assert!(
            err.to_string().contains("commit sha"),
            "error must explain the unproven-commit refusal: {err}"
        );
    }

    /// FAIL-CLOSED: an ABBREVIATED sha (7 hex chars) is rejected — only the full
    /// canonical 40-char form is accepted as an immutable tag.
    #[tokio::test]
    async fn resolve_cloned_head_rejects_abbreviated_sha() {
        let capture: GitCapture =
            std::sync::Arc::new(|_args| Box::pin(async { Ok("c64f621\n".to_owned()) }));
        assert!(
            resolve_cloned_head(Path::new("/tmp/dest"), &capture)
                .await
                .is_err(),
            "an abbreviated 7-char sha must be rejected"
        );
    }

    /// FAIL-CLOSED: an UPPERCASE-hex sha is rejected — OCI tags are lowercase and
    /// git emits lowercase, so an uppercase value signals a malformed/forged
    /// output rather than a real rev-parse result.
    #[tokio::test]
    async fn resolve_cloned_head_rejects_uppercase_sha() {
        let upper = "C64F621ABCDEF0123456789ABCDEF0123456789A";
        let capture: GitCapture = std::sync::Arc::new(move |_args| {
            Box::pin(async move { Ok(format!("{upper}\n")) })
        });
        assert!(
            resolve_cloned_head(Path::new("/tmp/dest"), &capture)
                .await
                .is_err(),
            "an uppercase-hex sha must be rejected"
        );
    }

    /// FAIL-CLOSED: when the underlying git command itself errors, the failure
    /// propagates (no SHA, no fabricated tag).
    #[tokio::test]
    async fn resolve_cloned_head_propagates_command_error() {
        let capture: GitCapture = std::sync::Arc::new(|_args| {
            Box::pin(async { Err(anyhow::anyhow!("git rev-parse failed (128): not a git repo")) })
        });
        let err = resolve_cloned_head(Path::new("/tmp/dest"), &capture)
            .await
            .expect_err("a failed git command must propagate");
        let chain = format!("{err:#}");
        assert!(
            chain.contains("rev-parse"),
            "error must reference the rev-parse step: {chain}"
        );
    }

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

    /// One recorded git invocation: (argv, env vars).
    type Call = (Vec<String>, Vec<(String, String)>);
    /// A `GitRun` that records every invocation. Returns the recorded list to
    /// the test so it can assert across the entire init+remote+fetch+checkout
    /// sequence (the new multi-step clone makes four runner calls).
    type Calls = Vec<Call>;

    fn recording_runner() -> (GitRun, std::sync::Arc<std::sync::Mutex<Calls>>) {
        let log: std::sync::Arc<std::sync::Mutex<Calls>> =
            std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let l = log.clone();
        let r: GitRun = std::sync::Arc::new(move |args, env| {
            l.lock().unwrap().push((args.clone(), env.clone()));
            Box::pin(async { Ok(()) })
        });
        (r, log)
    }

    /// Pick the recorded call whose first arg matches `subcommand`. For
    /// "remote", "fetch", "checkout" the subcommand follows the `-C <dest>`
    /// prefix; we scan for it inside the argv. For "init" it is argv[0].
    fn find_call<'a>(calls: &'a [Call], subcommand: &str) -> &'a Call {
        calls
            .iter()
            .find(|(args, _)| args.iter().any(|a| a == subcommand))
            .unwrap_or_else(|| panic!("no recorded call contains {subcommand:?}; got {calls:?}"))
    }

    // -- clone with token: token must not appear in argv ----------------------

    /// The cardinal security property: a short-lived token must NEVER appear
    /// in the git argv (which is visible to other processes via `ps` or
    /// `/proc/<pid>/cmdline`). The token must live only in the 0600 token file.
    /// This must hold across every git invocation in the multi-step clone.
    #[tokio::test]
    async fn clone_with_token_keeps_token_out_of_argv() {
        let dir = tempfile::tempdir().unwrap();
        let (runner, log) = recording_runner();

        clone(
            "https://github.com/acme/app.git",
            "v1",
            Some("ghs_SECRETtoken"),
            &dir.path().join("src"),
            &runner,
        )
        .await
        .unwrap();

        let calls = log.lock().unwrap().clone();
        // The clone is unfolded into init, remote add, fetch, checkout.
        assert_eq!(calls.len(), 4, "expected 4 git invocations, got {calls:?}");

        // Security invariant: token must NOT appear in ANY argv.
        for (args, _) in &calls {
            assert!(
                args.iter().all(|a| !a.contains("ghs_SECRETtoken")),
                "token must NOT be in argv: {args:?}"
            );
        }

        // `remote add` step carries the non-secret username in the URL.
        let (remote_args, _) = find_call(&calls, "remote");
        assert!(
            remote_args
                .iter()
                .any(|a| a.contains("x-access-token@github.com")),
            "remote add URL must carry non-secret username: {remote_args:?}"
        );

        // GIT_ASKPASS + GIT_TERMINAL_PROMPT=0 must be set ONLY on the fetch
        // step (the only call that performs network IO needing the password).
        let (_, fetch_env) = find_call(&calls, "fetch");
        assert!(
            fetch_env.iter().any(|(k, _)| k == "GIT_ASKPASS"),
            "fetch env must include GIT_ASKPASS"
        );
        assert!(
            fetch_env
                .iter()
                .any(|(k, v)| k == "GIT_TERMINAL_PROMPT" && v == "0"),
            "fetch env must include GIT_TERMINAL_PROMPT=0"
        );

        // The other steps must NOT leak the askpass env (defense in depth).
        for sub in ["init", "remote", "checkout"] {
            let (_, env) = find_call(&calls, sub);
            assert!(
                !env.iter().any(|(k, _)| k == "GIT_ASKPASS"),
                "{sub} must not carry GIT_ASKPASS in env"
            );
        }
    }

    /// Sequence check: init → remote add → fetch → checkout.
    #[tokio::test]
    async fn clone_invokes_init_remote_fetch_checkout_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let (runner, log) = recording_runner();

        clone(
            "https://github.com/acme/app.git",
            "deadbeef",
            None,
            &dir.path().join("src"),
            &runner,
        )
        .await
        .unwrap();

        let calls = log.lock().unwrap().clone();
        let sequence: Vec<String> = calls
            .iter()
            .map(|(args, _)| {
                args.iter()
                    .find(|a| matches!(a.as_str(), "init" | "remote" | "fetch" | "checkout"))
                    .cloned()
                    .unwrap_or_default()
            })
            .collect();
        assert_eq!(
            sequence,
            vec!["init", "remote", "fetch", "checkout"],
            "clone must run init -> remote add -> fetch -> checkout in order; got {calls:?}"
        );
    }

    /// Fetch passes `<ref>` verbatim — this is what makes the universal path
    /// work for branches, tags, AND raw commit SHAs (the old `--branch <sha>`
    /// approach was rejected by GitHub with "Remote branch X not found").
    #[tokio::test]
    async fn clone_fetches_ref_verbatim_supporting_sha_branch_or_tag() {
        let dir = tempfile::tempdir().unwrap();
        let (runner, log) = recording_runner();

        // A 40-char hex string mimics a real commit SHA.
        let sha = "c64f621abcdef0123456789abcdef0123456789a";
        clone(
            "https://github.com/acme/app.git",
            sha,
            None,
            &dir.path().join("src"),
            &runner,
        )
        .await
        .unwrap();

        let calls = log.lock().unwrap().clone();
        let (fetch_args, _) = find_call(&calls, "fetch");
        assert!(
            fetch_args.iter().any(|a| a == sha),
            "fetch must include the SHA verbatim: {fetch_args:?}"
        );
        assert!(
            !fetch_args.iter().any(|a| a == "--branch"),
            "fetch must NOT use --branch (incompatible with SHAs): {fetch_args:?}"
        );
        assert!(
            fetch_args.iter().any(|a| a == "--depth"),
            "fetch must stay shallow with --depth 1: {fetch_args:?}"
        );

        // Checkout targets FETCH_HEAD, not the ref directly (so the SHA isn't
        // resolved against the local repo, which has nothing yet — fetch is
        // the only place the ref lives).
        let (checkout_args, _) = find_call(&calls, "checkout");
        assert!(
            checkout_args.iter().any(|a| a == "FETCH_HEAD"),
            "checkout must target FETCH_HEAD: {checkout_args:?}"
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

    /// Without a token the original URL must be passed verbatim and
    /// GIT_ASKPASS must NOT be set on ANY of the four git invocations.
    #[tokio::test]
    async fn clone_without_token_uses_plain_url_and_no_askpass() {
        let dir = tempfile::tempdir().unwrap();
        let (runner, log) = recording_runner();

        clone(
            "https://github.com/acme/pub.git",
            "main",
            None,
            &dir.path().join("src"),
            &runner,
        )
        .await
        .unwrap();

        let calls = log.lock().unwrap().clone();
        let (remote_args, _) = find_call(&calls, "remote");
        assert!(
            remote_args
                .iter()
                .any(|a| a == "https://github.com/acme/pub.git"),
            "remote add must carry the original URL for public clone: {remote_args:?}"
        );
        for (_, env) in &calls {
            assert!(
                !env.iter().any(|(k, _)| k == "GIT_ASKPASS"),
                "GIT_ASKPASS must NOT be set on any call for public clone: {env:?}"
            );
        }
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

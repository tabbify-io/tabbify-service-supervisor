//! OCI image push via the `skopeo` CLI — the registry-PUSH seam for the docker
//! build pipeline. Run by the supervisor process (which is on the mesh), it
//! copies the just-built image from the local docker daemon to the mesh registry,
//! so the docker daemon itself never needs a mesh route. Mirrors `crate::oras`.

use crate::docker::CommandRunner;

/// `skopeo copy --dest-tls-verify=false docker-daemon:<local_tag>:latest docker://<reff>`
/// argv (sans the leading binary). `--dest-tls-verify=false` allows the plain-HTTP
/// mesh registry (same reason `oras` uses `--plain-http`). The source reads the
/// built image from the local docker daemon (via its socket); the destination is
/// the registry, reached over the supervisor's mesh-routed network.
///
/// The local docker tag is created as a bare name (`tbf-build-<uuid>`), which the
/// daemon stores as `:latest`; skopeo's `docker-daemon:` transport requires the
/// explicit `name:tag`, so we append `:latest`.
///
/// # Example
/// ```
/// # use tabbify_supervisor::skopeo::skopeo_push_args;
/// let args = skopeo_push_args("tbf-build-u", "[fd5a::1]:5000/acme/u:abc");
/// assert_eq!(args[0], "copy");
/// assert!(args.contains(&"--dest-tls-verify=false".to_owned()));
/// assert!(args.contains(&"docker-daemon:tbf-build-u:latest".to_owned()));
/// assert!(args.contains(&"docker://[fd5a::1]:5000/acme/u:abc".to_owned()));
/// ```
#[must_use]
pub fn skopeo_push_args(local_tag: &str, reff: &str) -> Vec<String> {
    vec![
        "copy".to_owned(),
        "--dest-tls-verify=false".to_owned(),
        format!("docker-daemon:{local_tag}:latest"),
        format!("docker://{reff}"),
    ]
}

/// Run `<skopeo_bin> copy --dest-tls-verify=false docker-daemon:<local_tag>:latest
/// docker://<reff>` via the injected `runner`.
///
/// Returns `Ok(())` iff the command exits successfully; `Err(stderr)` carries the
/// captured skopeo diagnostic so the build runner can bail with the real reason
/// (e.g. `unauthorized` / `name unknown`) instead of a bare image ref. Mirrors
/// [`crate::docker::push_image`] (which returns the `Result`, NOT a bool) because
/// the build path needs the failure detail.
///
/// The `skopeo_bin` is consumed by [`production_skopeo_runner`]; the seam args
/// themselves don't carry the binary name.
pub async fn skopeo_push(
    skopeo_bin: &str,
    local_tag: &str,
    reff: &str,
    runner: &CommandRunner,
) -> Result<(), String> {
    let _ = skopeo_bin; // skopeo_bin is used by production_skopeo_runner, not the seam args
    let args = skopeo_push_args(local_tag, reff);
    (runner)(args).await
}

/// Build a production [`CommandRunner`] for `skopeo`: spawns
/// `<skopeo_bin> <args>`, captures stderr, and returns `Ok(())` iff the process
/// exits 0; on a non-zero exit the captured stderr (trimmed) is returned in
/// `Err`, and a spawn failure returns the OS error in `Err`.
///
/// Re-uses the same `Arc<dyn Fn(…) -> BoxFut<…>>` shape as
/// [`crate::docker::production_command_runner`] / [`crate::oras::production_oras_runner`]
/// so the skopeo, oras, and docker seams are structurally identical.
#[must_use]
pub fn production_skopeo_runner(skopeo_bin: String) -> CommandRunner {
    use std::sync::Arc;

    use tokio::process::Command;

    use crate::runtime::BoxFut;

    Arc::new(move |args: Vec<String>| {
        let skopeo_bin = skopeo_bin.clone();
        let fut: BoxFut<'static, Result<(), String>> = Box::pin(async move {
            match Command::new(&skopeo_bin)
                .args(&args)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::piped())
                .output()
                .await
            {
                Ok(out) if out.status.success() => Ok(()),
                Ok(out) => {
                    let stderr = String::from_utf8_lossy(&out.stderr);
                    let stderr = stderr.trim();
                    let code = out
                        .status
                        .code()
                        .map_or_else(|| "signal".to_owned(), |c| c.to_string());
                    let argv = args.join(" ");
                    Err(if stderr.is_empty() {
                        format!("`{skopeo_bin} {argv}` exited with status {code}")
                    } else {
                        format!("`{skopeo_bin} {argv}` exited with status {code}: {stderr}")
                    })
                }
                Err(e) => Err(format!(
                    "failed to spawn `{skopeo_bin} {}`: {e}",
                    args.join(" ")
                )),
            }
        });
        fut
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    // ---- skopeo_push_args ----------------------------------------------------

    /// The push args must include `--dest-tls-verify=false` (the mesh registry is
    /// plain HTTP over the encrypted overlay), the `docker-daemon:<tag>:latest`
    /// source, and the `docker://<reff>` destination.
    #[test]
    fn push_args_includes_dest_tls_verify_and_daemon_source_and_registry_dest() {
        let local_tag = "tbf-build-u";
        let reff = "[fd5a:1f02::1]:5000/acme/app:sha256abc";
        let args = skopeo_push_args(local_tag, reff);
        assert_eq!(args[0], "copy", "first arg must be 'copy'");
        assert!(
            args.contains(&"--dest-tls-verify=false".to_owned()),
            "must include --dest-tls-verify=false; got {args:?}"
        );
        assert!(
            args.contains(&format!("docker-daemon:{local_tag}:latest")),
            "must read the built image from the local docker daemon; got {args:?}"
        );
        assert!(
            args.contains(&format!("docker://{reff}")),
            "must push to docker://<reff>; got {args:?}"
        );
    }

    /// Exact argv shape:
    /// `["copy", "--dest-tls-verify=false", "docker-daemon:<tag>:latest", "docker://<reff>"]`.
    #[test]
    fn push_args_exact_shape() {
        let args = skopeo_push_args("tbf-build-u", "reg/app:tag");
        assert_eq!(
            args,
            vec![
                "copy",
                "--dest-tls-verify=false",
                "docker-daemon:tbf-build-u:latest",
                "docker://reg/app:tag",
            ]
        );
    }

    // ---- skopeo_push (seam) --------------------------------------------------

    /// The runner is called with the correct skopeo push argv and the function
    /// returns `Ok(())` when the runner succeeds.
    #[tokio::test]
    async fn skopeo_push_calls_runner_with_correct_args() {
        use std::sync::{Arc, Mutex};

        let captured: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
        let cap2 = captured.clone();
        let runner: CommandRunner = Arc::new(move |args: Vec<String>| {
            cap2.lock().unwrap().push(args);
            Box::pin(async { Ok(()) })
        });

        let local_tag = "tbf-build-u";
        let reff = "[fd5a::1]:5000/myapp:latest";
        let res = skopeo_push("skopeo", local_tag, reff, &runner).await;
        assert!(res.is_ok(), "runner succeeded → skopeo_push must be Ok; got {res:?}");

        let calls = captured.lock().unwrap();
        assert_eq!(calls.len(), 1, "runner must be called exactly once");
        let argv = &calls[0];
        assert_eq!(argv[0], "copy", "argv must start with copy; got {argv:?}");
        assert!(
            argv.contains(&"--dest-tls-verify=false".to_owned()),
            "argv must contain --dest-tls-verify=false; got {argv:?}"
        );
        assert!(
            argv.contains(&format!("docker-daemon:{local_tag}:latest")),
            "argv must contain the docker-daemon source; got {argv:?}"
        );
        assert!(
            argv.contains(&format!("docker://{reff}")),
            "argv must contain the docker:// destination; got {argv:?}"
        );
    }

    /// A failing runner causes `skopeo_push` to return the captured stderr in
    /// `Err` (the build needs the real reason, not a bare bool).
    #[tokio::test]
    async fn skopeo_push_returns_err_on_runner_failure() {
        use std::sync::Arc;
        let runner: CommandRunner =
            Arc::new(|_| Box::pin(async { Err("name unknown: repository not found".to_owned()) }));
        let res = skopeo_push("skopeo", "tbf-build-u", "reg/app:v1", &runner).await;
        let err = res.unwrap_err();
        assert!(
            err.contains("name unknown"),
            "the captured stderr must survive in Err; got {err}"
        );
    }
}

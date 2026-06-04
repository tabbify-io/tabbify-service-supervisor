//! Registry PUSH for the docker build pipeline: a TWO-step
//! `skopeo → OCI layout → oras → registry` copy, run by the supervisor
//! process (which is on the mesh) so the docker daemon itself never needs a
//! mesh route.
//!
//! Why two steps: the mesh registry lives at a BRACKETED-IPv6 address
//! (`[fd5a:…]:5000/…`) and skopeo cannot parse such image references AT ALL
//! (`docker://[::1]:5000/x:y` → "invalid reference format"; verified
//! empirically on skopeo 1.9 AND 1.18 — the containers/image reference
//! grammar has no IPv6-literal support). `oras`, which the run-side already
//! uses to PULL exactly these refs, parses them fine. So skopeo only does
//! what it is uniquely good at — reading the built image out of the docker
//! daemon into an OCI layout — and oras does the registry push.

use crate::docker::CommandRunner;

/// Fixed tag the intermediate OCI layout is written/read under. Internal to
/// the two-step push; never visible in the registry.
const LAYOUT_TAG: &str = "build";

/// Step 1 argv (BINARY-PREFIXED): `skopeo copy docker-daemon:<local_tag>:latest
/// oci:<layout_dir>:build`. Local transports only — no registry reference is
/// parsed, which is exactly why skopeo is safe here.
///
/// The local docker tag is created as a bare name (`tbf-build-<uuid>`), which the
/// daemon stores as `:latest`; skopeo's `docker-daemon:` transport requires the
/// explicit `name:tag`, so we append `:latest`.
///
/// # Example
/// ```
/// # use tabbify_supervisor::skopeo::skopeo_to_layout_args;
/// let args = skopeo_to_layout_args("skopeo", "tbf-build-u", "/tmp/oci");
/// assert_eq!(args[0], "skopeo");
/// assert_eq!(args[1], "copy");
/// assert!(args.contains(&"docker-daemon:tbf-build-u:latest".to_owned()));
/// assert!(args.contains(&"oci:/tmp/oci:build".to_owned()));
/// ```
#[must_use]
pub fn skopeo_to_layout_args(skopeo_bin: &str, local_tag: &str, layout_dir: &str) -> Vec<String> {
    vec![
        skopeo_bin.to_owned(),
        "copy".to_owned(),
        format!("docker-daemon:{local_tag}:latest"),
        format!("oci:{layout_dir}:{LAYOUT_TAG}"),
    ]
}

/// Step 2 argv (BINARY-PREFIXED): `oras copy --from-oci-layout
/// <layout_dir>:build --to-plain-http <reff>` — the registry push. oras
/// parses bracketed-IPv6 refs (the run-side pulls with the same form);
/// `--to-plain-http` matches the plain-HTTP mesh registry over the encrypted
/// overlay (mirrors the pull side's `--from-plain-http`).
///
/// # Example
/// ```
/// # use tabbify_supervisor::skopeo::oras_push_args;
/// let args = oras_push_args("oras", "/tmp/oci", "[fd5a::1]:5000/acme/u:abc");
/// assert_eq!(args[0], "oras");
/// assert_eq!(args[1], "copy");
/// assert!(args.contains(&"--from-oci-layout".to_owned()));
/// assert!(args.contains(&"--to-plain-http".to_owned()));
/// ```
#[must_use]
pub fn oras_push_args(oras_bin: &str, layout_dir: &str, reff: &str) -> Vec<String> {
    vec![
        oras_bin.to_owned(),
        "copy".to_owned(),
        "--from-oci-layout".to_owned(),
        format!("{layout_dir}:{LAYOUT_TAG}"),
        "--to-plain-http".to_owned(),
        reff.to_owned(),
    ]
}

/// Push the built image to the registry: docker daemon → OCI layout
/// (skopeo, local transports) → registry (oras, IPv6-capable refs).
///
/// Both steps go through the SAME injected `runner`, whose argv carries the
/// binary as `args[0]` (see [`production_tool_runner`]). Returns `Ok(())`
/// iff both steps exit successfully; `Err(stderr)` carries the captured
/// diagnostic of the FAILED step so the build runner bails with the real
/// reason (e.g. `unauthorized` / `name unknown`) instead of a bare ref.
pub async fn push_to_registry(
    skopeo_bin: &str,
    oras_bin: &str,
    local_tag: &str,
    reff: &str,
    layout_dir: &str,
    runner: &CommandRunner,
) -> Result<(), String> {
    (runner)(skopeo_to_layout_args(skopeo_bin, local_tag, layout_dir)).await?;
    (runner)(oras_push_args(oras_bin, layout_dir, reff)).await
}

/// Build a production [`CommandRunner`] that spawns `args[0]` as the binary
/// with `args[1..]` as its argv — the two-step push drives DIFFERENT
/// binaries (skopeo, oras) through one seam. Captures stderr and returns
/// `Ok(())` iff the process exits 0; on a non-zero exit the captured stderr
/// (trimmed) is returned in `Err`, and a spawn failure returns the OS error.
///
/// Re-uses the same `Arc<dyn Fn(…) -> BoxFut<…>>` shape as
/// [`crate::docker::production_command_runner`] so the tool and docker seams
/// are structurally identical.
#[must_use]
pub fn production_tool_runner() -> CommandRunner {
    use std::sync::Arc;

    use tokio::process::Command;

    use crate::runtime::BoxFut;

    Arc::new(move |args: Vec<String>| {
        let fut: BoxFut<'static, Result<(), String>> = Box::pin(async move {
            let Some((skopeo_bin, rest)) = args.split_first() else {
                return Err("tool runner: empty argv".to_owned());
            };
            let skopeo_bin = skopeo_bin.clone();
            let args: Vec<String> = rest.to_vec();
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

    /// Step-1 argv: binary-prefixed, local transports only (docker-daemon →
    /// oci layout), and NO registry reference anywhere — skopeo cannot parse
    /// the mesh registry's bracketed-IPv6 refs.
    #[test]
    fn layout_args_exact_shape() {
        let args = skopeo_to_layout_args("skopeo", "tbf-build-u", "/tmp/w/oci");
        assert_eq!(
            args,
            vec![
                "skopeo",
                "copy",
                "docker-daemon:tbf-build-u:latest",
                "oci:/tmp/w/oci:build",
            ]
        );
    }

    /// Step-2 argv: binary-prefixed oras copy, layout source, plain-http
    /// registry destination carrying the bracketed-IPv6 reff VERBATIM.
    #[test]
    fn oras_push_args_exact_shape() {
        let args = oras_push_args("oras", "/tmp/w/oci", "[fd5a:1f02::1]:5000/acme/app:abc");
        assert_eq!(
            args,
            vec![
                "oras",
                "copy",
                "--from-oci-layout",
                "/tmp/w/oci:build",
                "--to-plain-http",
                "[fd5a:1f02::1]:5000/acme/app:abc",
            ]
        );
    }

    /// The two-step push drives the runner twice — skopeo first (daemon →
    /// layout), oras second (layout → registry) — and succeeds when both do.
    #[tokio::test]
    async fn push_runs_skopeo_then_oras() {
        use std::sync::{Arc, Mutex};

        let captured: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
        let cap2 = captured.clone();
        let runner: CommandRunner = Arc::new(move |args: Vec<String>| {
            cap2.lock().unwrap().push(args);
            Box::pin(async { Ok(()) })
        });

        let reff = "[fd5a::1]:5000/myapp:latest";
        let res = push_to_registry("skopeo", "oras", "tbf-build-u", reff, "/w/oci", &runner).await;
        assert!(res.is_ok(), "both steps Ok → push Ok; got {res:?}");

        let calls = captured.lock().unwrap();
        assert_eq!(calls.len(), 2, "exactly two steps");
        assert_eq!(calls[0][0], "skopeo");
        assert_eq!(calls[0][1], "copy");
        assert!(calls[0].iter().any(|a| a.starts_with("oci:/w/oci")));
        assert_eq!(calls[1][0], "oras");
        assert!(calls[1].contains(&reff.to_owned()));
    }

    /// A step-1 (skopeo) failure short-circuits: oras must NOT run, and the
    /// captured stderr survives in Err.
    #[tokio::test]
    async fn push_short_circuits_on_skopeo_failure() {
        use std::sync::{Arc, Mutex};

        let captured: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
        let cap2 = captured.clone();
        let runner: CommandRunner = Arc::new(move |args: Vec<String>| {
            cap2.lock().unwrap().push(args.clone());
            Box::pin(async move {
                if args[0] == "skopeo" {
                    Err("daemon not reachable".to_owned())
                } else {
                    Ok(())
                }
            })
        });

        let res = push_to_registry("skopeo", "oras", "t", "reg/app:v1", "/w/oci", &runner).await;
        let err = res.unwrap_err();
        assert!(err.contains("daemon not reachable"), "got {err}");
        assert_eq!(captured.lock().unwrap().len(), 1, "oras must not run");
    }

    /// A step-2 (oras) failure surfaces the registry diagnostic — the build
    /// needs the real reason (e.g. `unauthorized`), not a bare bool.
    #[tokio::test]
    async fn push_returns_oras_failure_detail() {
        use std::sync::Arc;
        let runner: CommandRunner = Arc::new(|args: Vec<String>| {
            Box::pin(async move {
                if args[0] == "oras" {
                    Err("name unknown: repository not found".to_owned())
                } else {
                    Ok(())
                }
            })
        });
        let res = push_to_registry("skopeo", "oras", "t", "reg/app:v1", "/w/oci", &runner).await;
        assert!(res.unwrap_err().contains("name unknown"));
    }

    /// The production tool runner treats `args[0]` as the binary: an empty
    /// argv is a contained error, not a panic.
    #[tokio::test]
    async fn tool_runner_rejects_empty_argv() {
        let runner = production_tool_runner();
        let err = (runner)(vec![]).await.unwrap_err();
        assert!(err.contains("empty argv"));
    }
}

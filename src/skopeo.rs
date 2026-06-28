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

use base64::Engine as _;

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
/// When `registry_config_dir` is `Some(dir)`, prepends `--to-registry-config <dir>`
/// so oras reads destination-registry push credentials from `<dir>/config.json`
/// (docker-format auth). `oras copy` is a two-endpoint command: the destination flag
/// is `--to-registry-config`, NOT the plain `--registry-config` (which only exists on
/// single-endpoint commands like `oras resolve`). Callers that do not need auth pass
/// `None` (anonymous; today's default).
///
/// # Example
/// ```
/// # use tabbify_supervisor::skopeo::oras_push_args;
/// let args = oras_push_args("oras", "/tmp/oci", "[fd5a::1]:5000/acme/u:abc", None);
/// assert_eq!(args[0], "oras");
/// assert_eq!(args[1], "copy");
/// assert!(args.contains(&"--from-oci-layout".to_owned()));
/// assert!(args.contains(&"--to-plain-http".to_owned()));
/// ```
#[must_use]
pub fn oras_push_args(
    oras_bin: &str,
    layout_dir: &str,
    reff: &str,
    registry_config_dir: Option<&str>,
) -> Vec<String> {
    let mut args = vec![oras_bin.to_owned(), "copy".to_owned()];
    if let Some(dir) = registry_config_dir {
        args.push("--to-registry-config".to_owned());
        args.push(dir.to_owned());
    }
    args.push("--from-oci-layout".to_owned());
    args.push(format!("{layout_dir}:{LAYOUT_TAG}"));
    args.push("--to-plain-http".to_owned());
    args.push(reff.to_owned());
    args
}

/// Write a docker-format `config.json` containing a single-registry auth entry
/// to `out_dir/config.json`. The file is consumed by oras via the directional
/// registry-config flags: `--to-registry-config <dir>` for pushes (via
/// [`oras_push_args`]), `--from-registry-config <dir>` for pulls (via
/// [`crate::oras::oras_copy_to_oci_layout_args`]), and `--registry-config <dir>`
/// for single-endpoint commands like `oras resolve` (via
/// [`crate::oras::oras_resolve_args`]). All forms read credentials from
/// `<dir>/config.json`.
///
/// The auth value is `base64("x:<token>")`, matching the docker credential
/// convention for token-based authentication (no username, token as password).
///
/// # Errors
/// Directory creation or file write failure.
pub fn write_registry_config(
    token: &str,
    registry_host: &str,
    out_dir: &std::path::Path,
) -> std::io::Result<()> {
    let auth = base64::engine::general_purpose::STANDARD.encode(format!("x:{token}"));
    // The registry host is a bracketed IPv6 address like `[fd5a::1]:5000`.
    // JSON string values permit `[`, `]`, and `:` verbatim, so a format!-built
    // string is correct here — no serde escaping needed for these characters.
    let json = format!(
        "{{\"auths\":{{\"{}\":{{\"auth\":\"{}\"}}}}}}",
        registry_host, auth
    );
    std::fs::create_dir_all(out_dir)?;
    std::fs::write(out_dir.join("config.json"), json.as_bytes())
}

/// Push the built image to the registry: docker daemon → OCI layout
/// (skopeo, local transports) → registry (oras, IPv6-capable refs).
///
/// Both steps go through the SAME injected `runner`, whose argv carries the
/// binary as `args[0]` (see [`production_tool_runner`]). Returns `Ok(())`
/// iff both steps exit successfully; `Err(stderr)` carries the captured
/// diagnostic of the FAILED step so the build runner bails with the real
/// reason (e.g. `unauthorized` / `name unknown`) instead of a bare ref.
///
/// `registry_config_dir`: when `Some(dir)`, threads `--to-registry-config <dir>`
/// into the oras step so pushes are authenticated. Pass `None` for anonymous
/// registry access (today's default behaviour; all existing callers use `None`).
pub async fn push_to_registry(
    skopeo_bin: &str,
    oras_bin: &str,
    local_tag: &str,
    reff: &str,
    layout_dir: &str,
    runner: &CommandRunner,
    registry_config_dir: Option<&str>,
) -> Result<(), String> {
    (runner)(skopeo_to_layout_args(skopeo_bin, local_tag, layout_dir)).await?;
    (runner)(oras_push_args(oras_bin, layout_dir, reff, registry_config_dir)).await
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
            let Some((bin, rest)) = args.split_first() else {
                return Err("tool runner: empty argv".to_owned());
            };
            let bin = bin.clone();
            let args: Vec<String> = rest.to_vec();
            // `oras` (push to the relay-only mesh registry) is retried on
            // transient failure — a large blob can break mid-transfer over the
            // DERP relay (the registry proxy then 502s); the local `skopeo`
            // daemon→layout step is deterministic and runs once. Every spawn
            // carries a valid `HOME` so `oras` never aborts "$HOME is not
            // defined" on a clean install.
            let attempts = crate::tool_exec::attempts_for(&bin);
            let mut last_err = String::from("tool runner: no attempt ran");
            for attempt in 1..=attempts {
                match Command::new(&bin)
                    .args(&args)
                    .env("HOME", crate::tool_exec::tool_home())
                    .stdin(std::process::Stdio::null())
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::piped())
                    .output()
                    .await
                {
                    Ok(out) if out.status.success() => return Ok(()),
                    Ok(out) => {
                        let stderr = String::from_utf8_lossy(&out.stderr);
                        let stderr = stderr.trim();
                        let code = out
                            .status
                            .code()
                            .map_or_else(|| "signal".to_owned(), |c| c.to_string());
                        let argv = args.join(" ");
                        last_err = if stderr.is_empty() {
                            format!("`{bin} {argv}` exited with status {code}")
                        } else {
                            format!("`{bin} {argv}` exited with status {code}: {stderr}")
                        };
                    }
                    Err(e) => {
                        last_err = format!("failed to spawn `{bin} {}`: {e}", args.join(" "));
                    }
                }
                if attempt < attempts {
                    tokio::time::sleep(crate::tool_exec::retry_backoff(attempt)).await;
                }
            }
            Err(last_err)
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

    /// Step-2 argv without auth: binary-prefixed oras copy, layout source,
    /// plain-http registry destination carrying the bracketed-IPv6 reff VERBATIM.
    #[test]
    fn oras_push_args_exact_shape_anonymous() {
        let args = oras_push_args("oras", "/tmp/w/oci", "[fd5a:1f02::1]:5000/acme/app:abc", None);
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

    /// With `registry_config_dir = Some(dir)`, `--to-registry-config <dir>` is
    /// prepended after `copy` so oras loads push credentials for the destination
    /// registry. (`oras copy` is two-endpoint; the destination flag is
    /// `--to-registry-config`, not the plain `--registry-config`.)
    #[test]
    fn oras_push_args_prepends_registry_config_when_some() {
        let args = oras_push_args(
            "oras",
            "/tmp/w/oci",
            "[fd5a::1]:5000/acme/app:abc",
            Some("/tmp/oras-cfg"),
        );
        assert_eq!(args[0], "oras");
        assert_eq!(args[1], "copy");
        assert_eq!(args[2], "--to-registry-config");
        assert_eq!(args[3], "/tmp/oras-cfg");
        assert!(args.contains(&"--from-oci-layout".to_owned()));
        assert!(args.contains(&"--to-plain-http".to_owned()));
    }

    /// `write_registry_config` creates `config.json` with the correct JSON
    /// auth structure: `base64("x:<token>")` keyed by `registry_host`.
    #[test]
    fn write_registry_config_creates_correct_config_json() {
        use base64::Engine as _;
        let dir = tempfile::tempdir().unwrap();
        write_registry_config("mytoken", "[fd5a::1]:5000", dir.path()).unwrap();
        let path = dir.path().join("config.json");
        let content = std::fs::read_to_string(&path).unwrap();
        // Must be valid JSON and carry the auths key.
        let v: serde_json::Value = serde_json::from_str(&content)
            .expect("config.json must be valid JSON");
        let auth_val = &v["auths"]["[fd5a::1]:5000"]["auth"];
        let encoded = auth_val.as_str().expect("auth must be a string");
        let decoded = String::from_utf8(
            base64::engine::general_purpose::STANDARD.decode(encoded).unwrap(),
        )
        .unwrap();
        assert_eq!(decoded, "x:mytoken", "auth must be base64(\"x:<token>\")");
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
        let res =
            push_to_registry("skopeo", "oras", "tbf-build-u", reff, "/w/oci", &runner, None)
                .await;
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

        let res =
            push_to_registry("skopeo", "oras", "t", "reg/app:v1", "/w/oci", &runner, None).await;
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
        let res =
            push_to_registry("skopeo", "oras", "t", "reg/app:v1", "/w/oci", &runner, None).await;
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

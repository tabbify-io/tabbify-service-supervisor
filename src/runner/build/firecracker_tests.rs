//! Tests for [`super`] — generic Firecracker runtime-build (OCI → ext4 +
//! PID-1 init render).
#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::sync::{Arc, Mutex};

use super::oci_fixtures::{make_tar, write_min_oci_layout};
use super::{
    Entrypoint, FcBuildRunner, OciExec, build_rootfs_ext4, cached_rootfs_path, render_init,
};

/// `oci-spec` links and parses an OCI image config's entrypoint/cmd/env/
/// workdir. This proves the dependency is wired before we build on it.
#[test]
fn oci_spec_parses_image_config_json() {
    let json = r#"{
        "architecture": "amd64",
        "os": "linux",
        "config": {
            "Entrypoint": ["/app/server"],
            "Cmd": ["--port", "8080"],
            "Env": ["RUST_LOG=info", "PORT=8080"],
            "WorkingDir": "/app"
        },
        "rootfs": { "type": "layers", "diff_ids": [] }
    }"#;
    let cfg: oci_spec::image::ImageConfiguration =
        serde_json::from_str(json).unwrap();
    let inner = cfg.config().as_ref().unwrap();
    assert_eq!(
        inner.entrypoint().as_ref().unwrap(),
        &vec!["/app/server".to_owned()]
    );
    assert_eq!(
        inner.cmd().as_ref().unwrap(),
        &vec!["--port".to_owned(), "8080".to_owned()]
    );
    assert_eq!(inner.working_dir().as_ref().unwrap(), "/app");
    assert!(
        inner
            .env()
            .as_ref()
            .unwrap()
            .contains(&"RUST_LOG=info".to_owned())
    );
}

/// `build_rootfs_ext4` must untar the layout's layers into a staging dir, then
/// `mkfs.ext4 -d <staging> <out>` (the rootless `-d` content path). No docker
/// export anywhere in the argv.
#[tokio::test]
async fn build_rootfs_unpacks_layers_then_mkfs_with_d_flag() {
    let tmp = tempfile::tempdir().unwrap();
    let out_dir = tmp.path().join("out");
    let l0 = make_tar(&[("bin/server", b"elf")]);
    let cfg = serde_json::json!({
        "architecture":"amd64","os":"linux","config":{"Entrypoint":["/bin/server"]},
        "rootfs":{"type":"layers","diff_ids":["sha256:l0"]}
    });
    let layout = write_min_oci_layout(&out_dir, &cfg, &[("sha256:l0", &l0)]);
    let config = super::read_oci_config_from_layout(&layout).unwrap();

    let calls: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
    let calls2 = calls.clone();
    // Real tar (host) for unpack; fake mkfs that touches the output file.
    let real = super::production_fc_build_runner();
    let runner: super::FcBuildRunner = Arc::new(move |argv: Vec<String>| {
        calls2.lock().unwrap().push(argv.clone());
        let real = real.clone();
        Box::pin(async move {
            if argv.first().map(String::as_str) == Some("mkfs.ext4") {
                if let Some(out) = argv.iter().find(|a| a.ends_with("rootfs.ext4")) {
                    std::fs::write(out, b"\0").unwrap();
                }
                (true, Vec::new())
            } else {
                (real)(argv).await
            }
        })
    });

    let rootfs = super::build_rootfs_ext4(&layout, &config, &out_dir, 64, &runner)
        .await
        .expect("build rootfs");
    assert_eq!(rootfs, out_dir.join("rootfs.ext4"));
    assert!(rootfs.is_file());

    let recorded = calls.lock().unwrap().clone();
    assert!(
        !recorded.iter().any(|c| c.iter().any(|a| a == "export")),
        "docker export must be gone; got {recorded:?}"
    );
    let mkfs = recorded
        .iter()
        .find(|c| c.first().map(String::as_str) == Some("mkfs.ext4"))
        .expect("must run mkfs.ext4");
    assert!(
        mkfs.contains(&"-d".to_owned()),
        "mkfs must use -d; got {mkfs:?}"
    );
    assert!(
        !mkfs.iter().any(|a| a == "sudo" || a.contains("loop")),
        "rootless + loopless; got {mkfs:?}"
    );
    assert_eq!(
        mkfs.last().map(String::as_str),
        Some(out_dir.join("rootfs.ext4").to_str().unwrap())
    );
}

/// A failing external runner (untar OR mkfs) surfaces a clear error and
/// produces no rootfs.
#[tokio::test]
async fn build_rootfs_errors_when_runner_fails() {
    let tmp = tempfile::tempdir().unwrap();
    let out_dir = tmp.path().join("out");
    let l0 = make_tar(&[("bin/server", b"elf")]);
    let cfg = serde_json::json!({
        "architecture":"amd64","os":"linux","config":{"Entrypoint":["/bin/server"]},
        "rootfs":{"type":"layers","diff_ids":["sha256:l0"]}
    });
    let layout = write_min_oci_layout(&out_dir, &cfg, &[("sha256:l0", &l0)]);
    let config = super::read_oci_config_from_layout(&layout).unwrap();
    let runner: FcBuildRunner = Arc::new(|_| Box::pin(async { (false, Vec::new()) }));
    let err = build_rootfs_ext4(&layout, &config, &out_dir, 64, &runner)
        .await
        .expect_err("must error when a step fails");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("tar") || msg.contains("layer") || msg.contains("mkfs"),
        "error must name the failing step; got: {err}"
    );
}

/// Exec-form entrypoint+cmd renders an init that:
/// - mounts /proc, /sys, /dev,
/// - exports the OCI env,
/// - cd's to the workdir,
/// - exec's the entrypoint argv verbatim (PID 1).
#[test]
fn render_init_exec_form_mounts_env_workdir_and_execs() {
    let exec = OciExec {
        entrypoint: vec!["/app/server".to_owned()],
        cmd: vec!["--port".to_owned(), "8080".to_owned()],
        env: vec!["RUST_LOG=info".to_owned(), "PORT=8080".to_owned()],
        workdir: "/app".to_owned(),
    };
    let init = render_init(&Entrypoint::Exec(exec)).unwrap();

    assert!(init.starts_with("#!"), "must be a shebang script");
    assert!(init.contains("mount -t proc"), "mounts /proc; got:\n{init}");
    assert!(init.contains("mount -t sysfs"), "mounts /sys; got:\n{init}");
    assert!(init.contains("/dev"), "mounts /dev; got:\n{init}");
    // eth0 is configured by the kernel ip= boot-arg; init only verifies it.
    assert!(
        init.contains("ip link show eth0") || init.contains("/sys/class/net/eth0"),
        "must verify eth0 presence; got:\n{init}"
    );
    // Env is exported; the value (no special chars) is single-quoted (FIX 1).
    assert!(
        init.contains("export RUST_LOG='info'"),
        "env exported (single-quoted value); got:\n{init}"
    );
    assert!(init.contains("cd '/app'"), "cd to workdir; got:\n{init}");
    // exec-form: the entrypoint argv is exec'd as PID 1, args appended; each
    // element single-quoted so the shell re-tokenizes back to the exact argv.
    assert!(
        init.contains("exec '/app/server' '--port' '8080'"),
        "must exec entrypoint+cmd as single-quoted tokens; got:\n{init}"
    );
    // No shell-wrapping `sh -c` around the entrypoint (exec-form only).
    assert!(
        !init.contains("sh -c \"/app/server"),
        "exec-form must not shell-wrap the entrypoint; got:\n{init}"
    );
}

/// FIX 1 regression: argv elements containing whitespace, glob chars (`*` `?`),
/// `$`, or quotes must be single-quoted in the rendered init so that the
/// `/bin/sh` running `/init` re-tokenizes them back to the EXACT argv instead of
/// word-splitting / globbing / `$`-expanding them. The same single-quoting must
/// apply to env VALUES.
#[test]
fn render_init_single_quotes_argv_and_env_values() {
    let exec = OciExec {
        entrypoint: vec!["/app/server".to_owned()],
        cmd: vec![
            "--msg".to_owned(),
            "hello world".to_owned(),
            "*.txt".to_owned(),
        ],
        env: vec!["GREETING=hello world".to_owned(), "PATTERN=$HOME/*".to_owned()],
        workdir: "/app".to_owned(),
    };
    let init = render_init(&Entrypoint::Exec(exec)).unwrap();

    // Each argv element is wrapped in single quotes verbatim — the shell cannot
    // word-split, glob, or `$`-expand inside single quotes.
    assert!(
        init.contains("exec '/app/server' '--msg' 'hello world' '*.txt'"),
        "argv elements must be single-quoted so the shell re-tokenizes them \
         exactly; got:\n{init}"
    );
    // A bare (unquoted) "hello world" / "*.txt" would be word-split / globbed.
    assert!(
        !init.contains("exec /app/server --msg hello world"),
        "argv must not be emitted bare; got:\n{init}"
    );
    // Env value with a space must be single-quoted too.
    assert!(
        init.contains("export GREETING='hello world'"),
        "env value with a space must be single-quoted; got:\n{init}"
    );
    assert!(
        init.contains("export PATTERN='$HOME/*'"),
        "env value with $ and glob must be single-quoted (no expansion); \
         got:\n{init}"
    );
}

/// FIX 1 regression: an embedded single quote in an argv element must be escaped
/// using the POSIX `'\''` idiom (close-quote, escaped quote, reopen-quote) so the
/// shell still reconstructs the exact byte sequence.
#[test]
fn render_init_escapes_embedded_single_quote() {
    let exec = OciExec {
        entrypoint: vec!["/bin/echo".to_owned()],
        cmd: vec!["it's fine".to_owned()],
        env: Vec::new(),
        workdir: "/".to_owned(),
    };
    let init = render_init(&Entrypoint::Exec(exec)).unwrap();
    // it's fine  ->  'it'\''s fine'
    assert!(
        init.contains(r#"exec '/bin/echo' 'it'\''s fine'"#),
        "embedded single quote must use the POSIX '\\'' idiom; got:\n{init}"
    );
}

/// Shell-form (empty entrypoint, no parseable argv) is DEFERRED (D3): render
/// must return a clear "shell-form not yet supported" error, not silently
/// guess a shell.
#[test]
fn render_init_shell_form_returns_clear_error() {
    let err = render_init(&Entrypoint::ShellForm).unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("shell-form") && msg.contains("not") && msg.contains("support"),
        "must clearly say shell-form is unsupported; got: {err}"
    );
}

/// `Entrypoint::from_oci` derives exec-form from a typed OCI config; an image
/// with NO entrypoint AND no cmd is treated as shell-form (deferred).
#[test]
fn entrypoint_from_oci_classifies_exec_vs_shell_form() {
    let json = r#"{
        "architecture":"amd64","os":"linux",
        "config":{"Entrypoint":["/bin/app"],"Cmd":["serve"],
                  "Env":["A=1"],"WorkingDir":"/srv"},
        "rootfs":{"type":"layers","diff_ids":[]}
    }"#;
    let cfg: oci_spec::image::ImageConfiguration =
        serde_json::from_str(json).unwrap();
    match Entrypoint::from_oci(&cfg) {
        Entrypoint::Exec(e) => {
            assert_eq!(e.entrypoint, vec!["/bin/app".to_owned()]);
            assert_eq!(e.cmd, vec!["serve".to_owned()]);
            assert_eq!(e.workdir, "/srv");
        }
        Entrypoint::ShellForm => panic!("should be exec-form"),
    }

    let empty = r#"{"architecture":"amd64","os":"linux",
        "config":{},"rootfs":{"type":"layers","diff_ids":[]}}"#;
    let cfg2: oci_spec::image::ImageConfiguration =
        serde_json::from_str(empty).unwrap();
    assert!(matches!(Entrypoint::from_oci(&cfg2), Entrypoint::ShellForm));
}

/// The cache path is keyed by the IMMUTABLE digest (sha256:…), not the tag,
/// and sanitizes the `:` so it's a valid single path segment.
#[test]
fn cached_rootfs_path_is_keyed_by_digest_under_data_dir() {
    let data_dir = Path::new("/var/lib/tabbify");
    let p = cached_rootfs_path(
        data_dir,
        "0191e7c2-1111-7222-8333-444455556666",
        "sha256:deadbeefcafe",
    );
    assert_eq!(
        p,
        Path::new(
            "/var/lib/tabbify/apps/0191e7c2-1111-7222-8333-444455556666/fc/sha256-deadbeefcafe/rootfs.ext4"
        )
    );
}

/// Two different digests for the same app yield distinct cache dirs (a new
/// build never clobbers the old rootfs — immutable-by-digest).
#[test]
fn cached_rootfs_path_differs_per_digest() {
    let d = Path::new("/data");
    let a = cached_rootfs_path(d, "app", "sha256:aaaa");
    let b = cached_rootfs_path(d, "app", "sha256:bbbb");
    assert_ne!(a, b);
    assert!(a.parent().unwrap().ends_with("sha256-aaaa"));
}

/// `rootfs_is_cached` is true iff the digest-keyed rootfs.ext4 exists.
#[test]
fn rootfs_is_cached_reflects_presence() {
    let tmp = tempfile::tempdir().unwrap();
    let digest = "sha256:abc123";
    assert!(!super::rootfs_is_cached(tmp.path(), "app", digest));
    let p = cached_rootfs_path(tmp.path(), "app", digest);
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(&p, b"\0").unwrap();
    assert!(super::rootfs_is_cached(tmp.path(), "app", digest));
}

use crate::fetcher::FetchedApp;
use crate::manifest::{AppManifest, AppMeta, Lifecycle, LifecycleMode, Routes, Runtime};
use bytes::Bytes;

fn fc_fetched(digest: &str) -> FetchedApp {
    FetchedApp {
        version: 3,
        manifest: AppManifest {
            app: AppMeta {
                id: None,
                name: "vm-app".to_owned(),
                version: String::new(),
                kind: "headless".to_owned(),
                description: String::new(),
            },
            lifecycle: Lifecycle {
                mode: LifecycleMode::AlwaysOn,
                idle_timeout_sec: 300,
            },
            runtime: Runtime {
                r#type: "firecracker".to_owned(),
                entry: "rootfs.ext4".to_owned(),
                fuel_per_request: 0,
                memory_mb: 128,
                vcpus: Some(2),
                kernel: None,
                registry_ref: Some(format!("[fd5a::1]:5000/acme/vm@{digest}")),
            },
            routes: Routes::default(),
        },
        wasm: Bytes::new(),
        cached_path: std::path::PathBuf::from("/cache/apps/u/v3/rootfs.ext4"),
    }
}

/// When the digest-keyed rootfs is ALREADY cached, `run_firecracker_build`
/// must NOT run any conversion command (no docker export / mkfs) — it
/// reuses the cached rootfs. We assert the conversion runner is untouched;
/// the actual VM boot is exercised only by the KVM-gated fc-7 test, so here
/// we stop at "would boot with this rootfs" by checking the cache hit path
/// via `rootfs_is_cached` before calling.
#[tokio::test]
async fn run_fc_build_skips_conversion_on_cache_hit() {
    let tmp = tempfile::tempdir().unwrap();
    let digest = "sha256:cached00";
    let fetched = fc_fetched(digest);

    // Pre-seed the digest-keyed cache so conversion is unnecessary.
    let cached = super::cached_rootfs_path(tmp.path(), "uuid-cache", digest);
    std::fs::create_dir_all(cached.parent().unwrap()).unwrap();
    std::fs::write(&cached, b"\0").unwrap();

    // DOCKER-LESS: the pull + config-read now live in `run_firecracker_build`,
    // so `resolve_rootfs` takes the already-pulled `layout`/`config`. On a cache
    // hit it returns before touching either, so we hand it a minimal real layout
    // (and a config parsed from it) that is simply never read.
    let layout_tmp = tempfile::tempdir().unwrap();
    let cfg = serde_json::json!({
        "architecture":"amd64","os":"linux","config":{"Entrypoint":["/x"]},
        "rootfs":{"type":"layers","diff_ids":[]}
    });
    let layout = write_min_oci_layout(layout_tmp.path(), &cfg, &[]);
    let config = super::read_oci_config_from_layout(&layout).unwrap();

    let called = std::sync::Arc::new(std::sync::Mutex::new(false));
    let called2 = called.clone();
    let runner: super::FcBuildRunner = std::sync::Arc::new(move |_argv| {
        *called2.lock().unwrap() = true;
        Box::pin(async { (true, Vec::new()) })
    });

    // resolve_rootfs is the conversion-or-cache step extracted from
    // run_firecracker_build so it's testable without a real VM boot.
    let rootfs = super::resolve_rootfs(
        "uuid-cache",
        &fetched,
        &layout,
        &config,
        digest,
        tmp.path(),
        &runner,
    )
    .await
    .unwrap();

    assert_eq!(rootfs, cached, "cache hit must return the cached rootfs");
    assert!(
        !*called.lock().unwrap(),
        "no conversion command may run on a cache hit"
    );
}

/// On a cache MISS the conversion runs DOCKER-LESS — pull the OCI layout, read
/// its config from the layout, untar its layers, then `mkfs.ext4` — and the
/// resulting rootfs lands at the digest-keyed path. No `docker inspect`/`export`.
///
/// `resolve_rootfs` pulls the layout into `<digest-dir>/oci`; we pre-stage a
/// real layout there (the fake `oras copy` is a no-op success) and use the real
/// host `tar` for the layer unpack, faking only `mkfs.ext4` to touch the output.
#[tokio::test]
async fn run_fc_build_converts_on_cache_miss() {
    let tmp = tempfile::tempdir().unwrap();
    let digest = "sha256:fresh01";
    let fetched = fc_fetched(digest);
    let target = super::cached_rootfs_path(tmp.path(), "uuid-miss", digest);

    // Stage a real OCI layout where `pull_oci_layout` would have left it.
    let l0 = make_tar(&[("bin/server", b"elf")]);
    let cfg = serde_json::json!({
        "architecture":"amd64","os":"linux",
        "config":{"Entrypoint":["/bin/server"],"Env":["PATH=/usr/bin"],"WorkingDir":"/app"},
        "rootfs":{"type":"layers","diff_ids":["sha256:l0"]}
    });
    let work = target.parent().unwrap().to_path_buf();
    let layout = write_min_oci_layout(&work, &cfg, &[("sha256:l0", &l0)]);
    let config = super::read_oci_config_from_layout(&layout).unwrap();

    let real = super::production_fc_build_runner();
    let runner: super::FcBuildRunner = std::sync::Arc::new(move |argv: Vec<String>| {
        let real = real.clone();
        Box::pin(async move {
            if argv.first().map(String::as_str) == Some("mkfs.ext4") {
                if let Some(out) = argv.iter().find(|a| a.ends_with("rootfs.ext4")) {
                    std::fs::write(out, b"\0").unwrap();
                }
                (true, Vec::new())
            } else {
                (real)(argv).await // real host tar for the layer unpack
            }
        })
    });

    let rootfs = super::resolve_rootfs(
        "uuid-miss",
        &fetched,
        &layout,
        &config,
        digest,
        tmp.path(),
        &runner,
    )
    .await
    .unwrap();
    assert_eq!(rootfs, target);
    assert!(rootfs.is_file());
}

/// On a cache MISS `run_firecracker_build` must NOT shell the local docker
/// daemon: the conversion is DOCKER-LESS (oras layout + manual unpack), so no
/// `docker pull`/`docker tag` may be issued. The VM boot at the end fails in a
/// daemonless test env, but every conversion-stage argv is recorded BEFORE the
/// boot, so we assert no recorded argv starts with `docker` regardless of the
/// final boot Result. Guards against a redundant `docker pull` lingering in the
/// FC hot path after the conversion stopped consuming the local daemon image.
#[tokio::test]
async fn run_fc_build_issues_no_docker_on_cache_miss() {
    let tmp = tempfile::tempdir().unwrap();
    let digest = "sha256:fresh02";
    let fetched = fc_fetched(digest);
    // Every FcConfig field carries a clap default, so an arg-less parse yields a
    // usable config without standing up a real Firecracker host.
    let fc = <crate::config::FcConfig as clap::Parser>::parse_from(["fc"]);
    let target = super::cached_rootfs_path(tmp.path(), "uuid-nodocker", digest);

    // Stage a real OCI layout where `pull_oci_layout` would have left it, so the
    // (faked, no-op) `oras copy` is satisfied and the real host `tar` unpacks it.
    let l0 = make_tar(&[("bin/server", b"elf")]);
    let cfg = serde_json::json!({
        "architecture":"amd64","os":"linux",
        "config":{"Entrypoint":["/app/server"],"Env":["PATH=/usr/bin"],"WorkingDir":"/app"},
        "rootfs":{"type":"layers","diff_ids":["sha256:l0"]}
    });
    let layout_dir = target.parent().unwrap().join("oci");
    std::fs::create_dir_all(&layout_dir).unwrap();
    write_min_oci_layout(&layout_dir, &cfg, &[("sha256:l0", &l0)]);

    let calls: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
    let calls2 = calls.clone();
    let target2 = target.clone();
    let real = super::production_fc_build_runner();
    let runner: super::FcBuildRunner = Arc::new(move |argv: Vec<String>| {
        calls2.lock().unwrap().push(argv.clone());
        let target3 = target2.clone();
        let real = real.clone();
        Box::pin(async move {
            match argv.first().map(String::as_str) {
                Some("mkfs.ext4") => {
                    std::fs::create_dir_all(target3.parent().unwrap()).ok();
                    if let Some(out) = argv.iter().find(|a| a.ends_with("rootfs.ext4")) {
                        std::fs::write(out, b"\0").unwrap();
                    }
                    (true, Vec::new())
                }
                Some("oras") => (true, Vec::new()),
                _ => (real)(argv).await,
            }
        })
    });

    // The VM boot at the end has no real Firecracker/KVM here, so this errors;
    // the docker-or-not assertion below holds on the recorded argv regardless.
    let _ = super::run_firecracker_build("uuid-nodocker", &fetched, &fc, tmp.path(), &runner).await;

    let recorded = calls.lock().unwrap().clone();
    assert!(
        !recorded
            .iter()
            .any(|c| c.first().map(String::as_str) == Some("docker")),
        "FC conversion is docker-less; no argv may start with `docker`; got {recorded:?}"
    );
}

/// `pull_oci_layout` pulls the ref into `<out>/oci` via the oras seam: argv[0]
/// is the `oras` binary, and the argv is the probe-proven layout-producing form
/// `oras copy --from-plain-http <ref> --to-oci-layout <out>/oci` — NOT the
/// empty-layout `oras pull -o` form and NOT the `--plain-http` flag. It does NOT
/// shell docker.
#[tokio::test]
async fn pull_oci_layout_uses_oras_copy_to_oci_layout() {
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("work");
    let reff = "[fd5a::1]:5000/acme/vm@sha256:fresh01";

    let calls: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
    let calls2 = calls.clone();
    let runner: super::FcBuildRunner = Arc::new(move |argv: Vec<String>| {
        calls2.lock().unwrap().push(argv);
        Box::pin(async { (true, Vec::new()) })
    });

    let layout = super::pull_oci_layout(reff, &out, &runner)
        .await
        .expect("pull must succeed");
    assert_eq!(layout, out.join("oci"), "layout dir is <out>/oci");

    let recorded = calls.lock().unwrap().clone();
    let pull = recorded.first().expect("must issue one oras copy");
    assert_eq!(pull.first().map(String::as_str), Some("oras"),
        "argv[0] must be the oras binary (FcBuildRunner spawns argv[0]); got {pull:?}");
    assert!(pull.contains(&"copy".to_owned()),
        "must be an `oras copy` (probe-proven layout form), not `oras pull`; got {pull:?}");
    assert!(pull.contains(&"--to-oci-layout".to_owned()),
        "must copy into an OCI layout; got {pull:?}");
    assert!(pull.contains(&"--from-plain-http".to_owned()),
        "mesh registry source is plain http; must use --from-plain-http; got {pull:?}");
    assert!(!pull.contains(&"--plain-http".to_owned()),
        "--plain-http is not the copy SOURCE flag; got {pull:?}");
    assert!(!pull.contains(&"pull".to_owned()) && !pull.contains(&"-o".to_owned()),
        "must NOT be the empty-layout `oras pull -o` form; got {pull:?}");
    assert!(pull.contains(&reff.to_owned()), "must carry the ref; got {pull:?}");
    assert!(pull.iter().any(|a| a.ends_with("oci")),
        "must target the layout dir <out>/oci; got {pull:?}");
}

/// A failing oras copy surfaces a clear error naming the pull step.
#[tokio::test]
async fn pull_oci_layout_errors_when_oras_fails() {
    let tmp = tempfile::tempdir().unwrap();
    let runner: super::FcBuildRunner = Arc::new(|_| Box::pin(async { (false, Vec::new()) }));
    let err = super::pull_oci_layout("reg/img@sha256:x", tmp.path(), &runner)
        .await
        .expect_err("must error when oras pull fails");
    assert!(err.to_string().to_lowercase().contains("oras"),
        "error must name the oras pull step; got: {err}");
}

/// `read_oci_config_from_layout` resolves index → manifest → config blob and
/// parses the exec config (entrypoint/env/workdir), WITHOUT docker inspect.
#[test]
fn read_oci_config_from_layout_parses_entrypoint() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = serde_json::json!({
        "architecture":"amd64","os":"linux",
        "config":{"Entrypoint":["/app/server"],"Cmd":["--port","8080"],
                  "Env":["RUST_LOG=info"],"WorkingDir":"/app"},
        "rootfs":{"type":"layers","diff_ids":["sha256:aaaa"]}
    });
    let layout = write_min_oci_layout(tmp.path(), &cfg, &[("sha256:aaaa", b"layer0")]);
    let parsed = super::read_oci_config_from_layout(&layout).expect("read config");
    let inner = parsed.config().as_ref().unwrap();
    assert_eq!(inner.entrypoint().as_ref().unwrap(), &vec!["/app/server".to_owned()]);
    assert_eq!(inner.working_dir().as_ref().unwrap(), "/app");
    assert_eq!(parsed.rootfs().diff_ids(), &vec!["sha256:aaaa".to_owned()]);
}

/// A layout with no image manifest in index.json errors clearly.
#[test]
fn read_oci_config_from_layout_errors_on_empty_index() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("index.json"),
        br#"{"schemaVersion":2,"manifests":[]}"#).unwrap();
    let err = super::read_oci_config_from_layout(tmp.path()).unwrap_err();
    assert!(err.to_string().to_lowercase().contains("manifest"),
        "must name the missing manifest; got: {err}");
}

/// `unpack_oci_layers` untars layers in order and applies OCI whiteouts:
/// - `.wh.<name>` removes `<name>` carried by an earlier layer,
/// - `.wh..wh..opq` clears the directory's earlier contents,
/// and the `.wh.*` markers themselves never survive into the staging tree.
#[tokio::test]
async fn unpack_oci_layers_applies_whiteouts_in_order() {
    let tmp = tempfile::tempdir().unwrap();
    // Layer 0: a/keep.txt, a/drop.txt, b/old.txt
    let l0 = make_tar(&[("a/keep.txt", b"k"), ("a/drop.txt", b"d"), ("b/old.txt", b"o")]);
    // Layer 1: whiteout a/drop.txt + opaque b/ + b/new.txt
    let l1 = make_tar(&[("a/.wh.drop.txt", b""), ("b/.wh..wh..opq", b""), ("b/new.txt", b"n")]);
    let cfg = serde_json::json!({
        "architecture":"amd64","os":"linux",
        "config":{"Entrypoint":["/x"]},
        "rootfs":{"type":"layers","diff_ids":["sha256:l0","sha256:l1"]}
    });
    let layout = write_min_oci_layout(tmp.path(), &cfg,
        &[("sha256:l0", &l0), ("sha256:l1", &l1)]);
    let config = super::read_oci_config_from_layout(&layout).unwrap();
    let staging = tmp.path().join("stage");

    // Real `tar` via the runner (shells the host tar binary).
    let runner = super::production_fc_build_runner();
    super::unpack_oci_layers(&layout, &config, &staging, &runner)
        .await
        .expect("unpack must succeed");

    assert!(staging.join("a/keep.txt").is_file(), "kept file survives");
    assert!(!staging.join("a/drop.txt").exists(), ".wh.drop.txt must delete it");
    assert!(!staging.join("b/old.txt").exists(), "opaque dir clears earlier b/ contents");
    assert!(staging.join("b/new.txt").is_file(), "new file in opaque layer survives");
    assert!(!staging.join("a/.wh.drop.txt").exists(), "wh marker must not survive");
    assert!(!staging.join("b/.wh..wh..opq").exists(), "opq marker must not survive");
}

/// Regression: an opaque marker hides entries from LOWER layers but MUST keep
/// entries the SAME layer re-adds, even when that re-added path already existed
/// in an earlier layer. A `prior`-membership test alone would wrongly delete the
/// freshly written file; the per-layer written set must protect it.
#[tokio::test]
async fn unpack_oci_layers_opaque_keeps_same_layer_readd() {
    let tmp = tempfile::tempdir().unwrap();
    // Layer 0: b/keep.txt (old content) + b/old.txt
    let l0 = make_tar(&[("b/keep.txt", b"old"), ("b/old.txt", b"o")]);
    // Layer 1: opaque b/ (hides lower b/old.txt) AND re-adds b/keep.txt.
    // b/keep.txt existed in layer 0, so it IS in `prior`, but layer 1 just
    // wrote it, so it MUST survive the opaque clear.
    let l1 = make_tar(&[("b/.wh..wh..opq", b""), ("b/keep.txt", b"new")]);
    let cfg = serde_json::json!({
        "architecture":"amd64","os":"linux",
        "config":{"Entrypoint":["/x"]},
        "rootfs":{"type":"layers","diff_ids":["sha256:l0","sha256:l1"]}
    });
    let layout = write_min_oci_layout(tmp.path(), &cfg,
        &[("sha256:l0", &l0), ("sha256:l1", &l1)]);
    let config = super::read_oci_config_from_layout(&layout).unwrap();
    let staging = tmp.path().join("stage");

    let runner = super::production_fc_build_runner();
    super::unpack_oci_layers(&layout, &config, &staging, &runner)
        .await
        .expect("unpack must succeed");

    assert!(!staging.join("b/old.txt").exists(),
        "opaque dir clears the lower layer's b/old.txt");
    assert!(staging.join("b/keep.txt").is_file(),
        "same-layer re-add of b/keep.txt must survive the opaque clear");
    assert_eq!(
        std::fs::read(staging.join("b/keep.txt")).unwrap(),
        b"new",
        "the surviving b/keep.txt must be the layer-1 content, not the lower one",
    );
}

/// A layer count that disagrees with rootfs.diff_ids errors loudly (corrupt
/// layout) rather than silently unpacking a partial rootfs.
#[tokio::test]
async fn unpack_oci_layers_errors_on_diffid_count_mismatch() {
    let tmp = tempfile::tempdir().unwrap();
    let l0 = make_tar(&[("f", b"x")]);
    let cfg = serde_json::json!({
        "architecture":"amd64","os":"linux","config":{"Entrypoint":["/x"]},
        "rootfs":{"type":"layers","diff_ids":["sha256:a","sha256:b"]}  // 2 != 1 layer
    });
    let layout = write_min_oci_layout(tmp.path(), &cfg, &[("sha256:a", &l0)]);
    let config = super::read_oci_config_from_layout(&layout).unwrap();
    let runner = super::production_fc_build_runner();
    let err = super::unpack_oci_layers(&layout, &config, &tmp.path().join("s"), &runner)
        .await
        .unwrap_err();
    assert!(err.to_string().to_lowercase().contains("layer"),
        "must name the layer/diff_id mismatch; got: {err}");
}

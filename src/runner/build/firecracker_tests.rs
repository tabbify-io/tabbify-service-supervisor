//! Tests for [`super`] — generic Firecracker runtime-build (OCI → ext4 +
//! PID-1 init render).
#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::sync::{Arc, Mutex};

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

/// `build_rootfs_ext4` must:
/// 1. `docker export` the image's filesystem into a staging dir (rootless),
/// 2. invoke `mkfs.ext4 -d <staging> <out>` (the `-d` content path — no loop,
///    no root),
/// returning the path to the produced rootfs.ext4.
#[tokio::test]
async fn build_rootfs_runs_export_then_mkfs_with_d_flag() {
    let tmp = tempfile::tempdir().unwrap();
    let out_dir = tmp.path().join("out");

    let calls: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
    let calls2 = calls.clone();
    let out_dir2 = out_dir.clone();

    // Fake runner: records argv. For the `mkfs.ext4` call, touch the output
    // file so the function's post-condition (rootfs exists) holds. Returns
    // `(exit_ok, stdout)`; fc-1 commands ignore stdout so it stays empty.
    let runner: FcBuildRunner = Arc::new(move |argv: Vec<String>| {
        calls2.lock().unwrap().push(argv.clone());
        let out_dir3 = out_dir2.clone();
        Box::pin(async move {
            if argv.first().map(String::as_str) == Some("mkfs.ext4") {
                std::fs::create_dir_all(&out_dir3).ok();
                // rootfs.ext4 is the mkfs output path (NOT the trailing size arg)
                if let Some(out) = argv.iter().find(|a| a.ends_with("rootfs.ext4")) {
                    std::fs::write(out, b"\0").unwrap();
                }
            }
            (true, Vec::new())
        })
    });

    let rootfs = build_rootfs_ext4(
        "tbf-img-acme-app-v3", // local docker image tag to export
        &out_dir,
        64, // size_mib hint
        &runner,
    )
    .await
    .expect("build rootfs");

    assert_eq!(rootfs, out_dir.join("rootfs.ext4"));
    assert!(rootfs.is_file(), "rootfs.ext4 must exist on disk");

    let recorded = calls.lock().unwrap().clone();
    // First external call: a `docker export` (via `docker create` + export).
    assert!(
        recorded.iter().any(|c| c.iter().any(|a| a == "export")),
        "must run a docker export; got {recorded:?}"
    );
    // Second: mkfs.ext4 with the `-d <staging>` content-population flag and
    // NO loop device / NO sudo.
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
        "mkfs path must be rootless + loopless; got {mkfs:?}"
    );
    // The mkfs output path is the produced rootfs.ext4.
    assert_eq!(
        mkfs.last().map(String::as_str),
        Some(out_dir.join("rootfs.ext4").to_str().unwrap())
    );
}

/// A failing external runner (export OR mkfs) surfaces a clear error and
/// produces no rootfs.
#[tokio::test]
async fn build_rootfs_errors_when_runner_fails() {
    let tmp = tempfile::tempdir().unwrap();
    let runner: FcBuildRunner = Arc::new(|_| Box::pin(async { (false, Vec::new()) }));
    let err = build_rootfs_ext4("img", &tmp.path().join("out"), 64, &runner)
        .await
        .expect_err("must error when a step fails");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("export") || msg.contains("mkfs"),
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
    assert!(
        init.contains("export RUST_LOG=info"),
        "env exported; got:\n{init}"
    );
    assert!(init.contains("cd /app"), "cd to workdir; got:\n{init}");
    // exec-form: the entrypoint argv is exec'd as PID 1, args appended.
    assert!(
        init.contains("exec /app/server --port 8080"),
        "must exec entrypoint+cmd verbatim; got:\n{init}"
    );
    // No shell-wrapping `sh -c` around the entrypoint (exec-form only).
    assert!(
        !init.contains("sh -c \"/app/server"),
        "exec-form must not shell-wrap the entrypoint; got:\n{init}"
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

    let called = std::sync::Arc::new(std::sync::Mutex::new(false));
    let called2 = called.clone();
    let runner: super::FcBuildRunner = std::sync::Arc::new(move |_argv| {
        *called2.lock().unwrap() = true;
        Box::pin(async { (true, Vec::new()) })
    });

    // resolve_rootfs is the conversion-or-cache step extracted from
    // run_firecracker_build so it's testable without a real VM boot.
    let rootfs = super::resolve_rootfs("uuid-cache", &fetched, digest, tmp.path(), &runner)
        .await
        .unwrap();

    assert_eq!(rootfs, cached, "cache hit must return the cached rootfs");
    assert!(
        !*called.lock().unwrap(),
        "no conversion command may run on a cache hit"
    );
}

/// On a cache MISS the conversion runs in the REAL order — `docker inspect`
/// (OCI config) → `docker export` → `tar` → `mkfs.ext4` — and the resulting
/// rootfs lands at the digest-keyed path.
///
/// The fake runner must produce BOTH side-effects, the same structural way:
/// - for `docker inspect` it returns a minimal-but-valid OCI image config as
///   STDOUT (this is what `read_oci_config` parses — bug-fix: previously the
///   miss test seeded only `mkfs.ext4`, so `read_oci_config` ran first
///   against a runner returning `true` with EMPTY stdout → parse failed),
/// - for `mkfs.ext4` it writes the rootfs.ext4 file on disk.
/// This exercises read_oci_config → render_init → build_rootfs_ext4_inner →
/// mkfs end-to-end without a real docker/mkfs.
#[tokio::test]
async fn run_fc_build_converts_on_cache_miss() {
    let tmp = tempfile::tempdir().unwrap();
    let digest = "sha256:fresh01";
    let fetched = fc_fetched(digest);
    let target = super::cached_rootfs_path(tmp.path(), "uuid-miss", digest);
    let target2 = target.clone();

    // Minimal valid OCI image config with an exec-form entrypoint so
    // render_init succeeds (shell-form would be rejected, D3). Serialized as
    // the `{{json .Config}}` shape `docker inspect` prints to STDOUT.
    let oci_config_json = serde_json::to_vec(&serde_json::json!({
        "Entrypoint": ["/app/server"],
        "Cmd": serde_json::Value::Null,
        "Env": ["PATH=/usr/bin"],
        "WorkingDir": "/app"
    }))
    .unwrap();

    let runner: super::FcBuildRunner = std::sync::Arc::new(move |argv: Vec<String>| {
        let target3 = target2.clone();
        let oci = oci_config_json.clone();
        Box::pin(async move {
            match argv.first().map(String::as_str) {
                // `docker inspect` → OCI config on STDOUT (NOT a file).
                Some("docker") if argv.iter().any(|a| a == "inspect") => (true, oci),
                // `mkfs.ext4` → produce the rootfs file at the cache path.
                Some("mkfs.ext4") => {
                    std::fs::create_dir_all(target3.parent().unwrap()).ok();
                    if let Some(out) = argv.iter().find(|a| a.ends_with("rootfs.ext4")) {
                        std::fs::write(out, b"\0").unwrap();
                    }
                    (true, Vec::new())
                }
                // export / tar succeed with no stdout.
                _ => (true, Vec::new()),
            }
        })
    });

    let rootfs = super::resolve_rootfs("uuid-miss", &fetched, digest, tmp.path(), &runner)
        .await
        .unwrap();
    assert_eq!(rootfs, target);
    assert!(rootfs.is_file());
}

/// The registry pull+tag step of `run_firecracker_build` must issue argv whose
/// FIRST element is the `docker` binary — the [`super::FcBuildRunner`] contract
/// (and [`super::production_fc_build_runner`]) spawns `Command::new(argv[0])`,
/// so the program MUST be argv[0]. The `docker::protocol::{pull_args, tag_args}`
/// builders return argv WITHOUT the binary (`["pull", reff]` / `["tag", reff,
/// vtag]`) because they're consumed by the docker module's runner which bakes
/// `docker` in via `Command::new(docker_bin).args(args)`. Feeding those raw into
/// the FC runner would spawn nonexistent `pull`/`tag` executables in production.
/// This asserts the FC pull+tag step prepends `docker` so it spawns
/// `docker pull <reff>` and `docker tag <reff> <vtag>`.
#[tokio::test]
async fn pull_and_tag_argv_has_docker_as_program() {
    let reff = "[fd5a::1]:5000/acme/vm@sha256:fresh01";
    let vtag = "tbf-img-uuid-pull-v3";

    let calls: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
    let calls2 = calls.clone();
    let runner: super::FcBuildRunner = Arc::new(move |argv: Vec<String>| {
        calls2.lock().unwrap().push(argv);
        Box::pin(async { (true, Vec::new()) })
    });

    super::pull_and_tag(reff, vtag, &runner)
        .await
        .expect("pull+tag must succeed");

    let recorded = calls.lock().unwrap().clone();
    let pull = recorded
        .iter()
        .find(|c| c.iter().any(|a| a == "pull"))
        .expect("must issue a docker pull");
    assert_eq!(
        pull.first().map(String::as_str),
        Some("docker"),
        "pull argv[0] must be the docker binary (FcBuildRunner spawns argv[0]); got {pull:?}"
    );
    assert_eq!(pull, &vec!["docker".to_owned(), "pull".to_owned(), reff.to_owned()]);

    let tag = recorded
        .iter()
        .find(|c| c.iter().any(|a| a == "tag"))
        .expect("must issue a docker tag");
    assert_eq!(
        tag.first().map(String::as_str),
        Some("docker"),
        "tag argv[0] must be the docker binary (FcBuildRunner spawns argv[0]); got {tag:?}"
    );
    assert_eq!(
        tag,
        &vec!["docker".to_owned(), "tag".to_owned(), reff.to_owned(), vtag.to_owned()]
    );
}

/// A failing `docker pull` surfaces a clear error naming the pull step (so a
/// cache-miss conversion can never silently proceed against a missing image).
#[tokio::test]
async fn pull_and_tag_errors_when_pull_fails() {
    let runner: super::FcBuildRunner = Arc::new(|_argv| Box::pin(async { (false, Vec::new()) }));
    let err = super::pull_and_tag("reg/img@sha256:x", "tbf-img-u-v1", &runner)
        .await
        .expect_err("must error when pull fails");
    assert!(
        err.to_string().to_lowercase().contains("pull"),
        "error must name the failing pull step; got: {err}"
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

/// Write a minimal spec-compliant OCI layout under `dir`: a config blob, an
/// image manifest referencing it (+ given layer descriptors), and an index.json
/// pointing at the manifest. Returns the layout dir. `layers` = (digest, bytes).
fn write_min_oci_layout(
    dir: &Path,
    config_json: &serde_json::Value,
    layers: &[(&str, &[u8])],
) -> std::path::PathBuf {
    use sha2::{Digest as _, Sha256};
    let blobs = dir.join("blobs").join("sha256");
    std::fs::create_dir_all(&blobs).unwrap();
    let put = |bytes: &[u8]| -> String {
        let hex = format!("{:x}", Sha256::digest(bytes));
        std::fs::write(blobs.join(&hex), bytes).unwrap();
        hex
    };
    let cfg_bytes = serde_json::to_vec(config_json).unwrap();
    let cfg_hex = put(&cfg_bytes);
    let layer_descs: Vec<serde_json::Value> = layers.iter().map(|(d, b)| {
        let hex = put(b);
        serde_json::json!({
            "mediaType": "application/vnd.oci.image.layer.v1.tar",
            "digest": format!("sha256:{hex}"), "size": b.len(),
            "annotations": {"diffid": *d}
        })
    }).collect();
    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {"mediaType":"application/vnd.oci.image.config.v1+json",
                   "digest": format!("sha256:{cfg_hex}"), "size": cfg_bytes.len()},
        "layers": layer_descs
    });
    let man_bytes = serde_json::to_vec(&manifest).unwrap();
    let man_hex = put(&man_bytes);
    let index = serde_json::json!({
        "schemaVersion": 2,
        "manifests": [{"mediaType":"application/vnd.oci.image.manifest.v1+json",
                       "digest": format!("sha256:{man_hex}"), "size": man_bytes.len()}]
    });
    std::fs::write(dir.join("index.json"), serde_json::to_vec(&index).unwrap()).unwrap();
    std::fs::write(dir.join("oci-layout"), br#"{"imageLayoutVersion":"1.0.0"}"#).unwrap();
    dir.to_path_buf()
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

//! Tests for [`super`] — generic Firecracker runtime-build (OCI → ext4 +
//! PID-1 init render).
#![allow(clippy::unwrap_used)]

use std::path::Path;
use std::sync::{Arc, Mutex};

use super::oci_fixtures::{make_tar, write_min_oci_layout};
use super::{
    Entrypoint, FcBuildRunner, OciExec, build_rootfs_ext4, cached_rootfs_path, ext4_geometry,
    exposed_tcp_ports, extract_layer_blob, measure_tree, merge_extra_env, render_init,
};

/// Parse an OCI `ImageConfiguration` from a JSON `config` object body (the
/// `{...}` that goes under `"config"`), so the ExposedPorts tests can assert
/// `exposed_tcp_ports` against realistic image configs concisely.
fn image_config_with(config_body: serde_json::Value) -> oci_spec::image::ImageConfiguration {
    let json = serde_json::json!({
        "architecture": "amd64",
        "os": "linux",
        "config": config_body,
        "rootfs": { "type": "layers", "diff_ids": [] },
    });
    serde_json::from_value(json).unwrap()
}

/// TIER 2 parsing — a single `{"80/tcp": {}}` ExposedPort resolves to `[80]`
/// (exactly the `FROM nginx` / `EXPOSE 80` case that used to fail the :8080 probe).
#[test]
fn exposed_port_single_tcp_resolves() {
    let cfg = image_config_with(serde_json::json!({ "ExposedPorts": { "80/tcp": {} } }));
    assert_eq!(exposed_tcp_ports(&cfg), vec![80]);
}

/// Multiple ExposedPorts → ALL TCP ports are returned, SORTED ascending
/// (order-independent: in a non-test build the set is HashMap-backed / unordered).
/// This REPLACES the old lowest-only heuristic: the launch probes them ALL
/// (first-answering-wins) instead of hammering the dead lowest port.
#[test]
fn exposed_port_returns_all_sorted() {
    let cfg = image_config_with(
        serde_json::json!({ "ExposedPorts": { "8443/tcp": {}, "80/tcp": {}, "443/tcp": {} } }),
    );
    assert_eq!(exposed_tcp_ports(&cfg), vec![80, 443, 8443]);
}

/// The nginx-base regression case: an image inherits `EXPOSE 80` from its base
/// AND declares its real `EXPOSE 8730` — BOTH ports are returned so the launch
/// probes the port the app actually listens on, not just the base-inherited 80.
#[test]
fn exposed_port_base_inherited_plus_app_port_both_returned() {
    let cfg = image_config_with(
        serde_json::json!({ "ExposedPorts": { "80/tcp": {}, "8730/tcp": {} } }),
    );
    assert_eq!(exposed_tcp_ports(&cfg), vec![80, 8730]);
}

/// An entry with NO protocol suffix defaults to TCP (per the OCI spec), so a bare
/// `"3000"` is a valid app port.
#[test]
fn exposed_port_bare_number_defaults_to_tcp() {
    let cfg = image_config_with(serde_json::json!({ "ExposedPorts": { "3000": {} } }));
    assert_eq!(exposed_tcp_ports(&cfg), vec![3000]);
}

/// A UDP-only ExposedPort is NOT a serveable TCP/HTTP app port → EMPTY (the
/// caller then falls back to the 8080 default rather than probing a UDP port).
#[test]
fn exposed_port_udp_only_is_empty() {
    let cfg = image_config_with(serde_json::json!({ "ExposedPorts": { "53/udp": {} } }));
    assert_eq!(exposed_tcp_ports(&cfg), Vec::<u16>::new());
}

/// Mixed TCP + UDP: the UDP entry is skipped, only the TCP port(s) are returned —
/// a UDP port numerically below the TCP one must NOT appear.
#[test]
fn exposed_port_mixed_tcp_udp_ignores_udp() {
    let cfg = image_config_with(
        serde_json::json!({ "ExposedPorts": { "53/udp": {}, "8080/tcp": {} } }),
    );
    assert_eq!(exposed_tcp_ports(&cfg), vec![8080]);
}

/// No `ExposedPorts` key at all ⇒ EMPTY (tier-3 fallback to 8080 downstream).
#[test]
fn exposed_port_absent_is_empty() {
    let cfg = image_config_with(serde_json::json!({ "Entrypoint": ["/app/server"] }));
    assert_eq!(exposed_tcp_ports(&cfg), Vec::<u16>::new());
}

/// An image with NO `config` block at all ⇒ EMPTY.
#[test]
fn exposed_port_no_config_block_is_empty() {
    let json = serde_json::json!({
        "architecture": "amd64",
        "os": "linux",
        "rootfs": { "type": "layers", "diff_ids": [] },
    });
    let cfg: oci_spec::image::ImageConfiguration = serde_json::from_value(json).unwrap();
    assert_eq!(exposed_tcp_ports(&cfg), Vec::<u16>::new());
}

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
    let cfg: oci_spec::image::ImageConfiguration = serde_json::from_str(json).unwrap();
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
    // ATOMIC write: mkfs targets a temp file in the SAME dir; only a
    // successful conversion is renamed onto the final `rootfs.ext4` (so a
    // crashed mkfs never leaves a partial valid-looking cache entry).
    let target = mkfs.last().map(String::as_str).unwrap();
    assert!(
        target.starts_with(out_dir.join(".rootfs.ext4.").to_str().unwrap())
            && target.ends_with(".tmp"),
        "mkfs must target the atomic temp; got {target}"
    );
    // The publish must land on the real name.
    assert!(out_dir.join("rootfs.ext4").is_file());
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
    let init = render_init(&Entrypoint::Exec(exec), &[], None).unwrap();

    assert!(init.starts_with("#!"), "must be a shebang script");
    assert!(init.contains("mount -t proc"), "mounts /proc; got:\n{init}");
    assert!(init.contains("mount -t sysfs"), "mounts /sys; got:\n{init}");
    assert!(init.contains("/dev"), "mounts /dev; got:\n{init}");
    // Core device nodes are created if devtmpfs is unavailable, so git (repo
    // create) can read /dev/urandom instead of failing with ENOENT.
    assert!(
        init.contains("mknod -m 666 /dev/urandom c 1 9"),
        "creates /dev/urandom fallback; got:\n{init}"
    );
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

/// FIX 4 regression (proven LIVE): minimal OCI images (e.g. `busybox`) ship NO
/// /proc /sys /dev mountpoints — a container runtime normally provides them — so
/// `render_init` must `mkdir -p` them BEFORE mounting, and the pseudo-fs mounts
/// must be BEST-EFFORT (`|| true`) so a missing/already-mounted fs can NEVER kill
/// PID 1 and panic the guest ("Attempted to kill init"). A busybox httpd image
/// only served once /proc /sys existed in the rootfs.
#[test]
fn render_init_creates_pseudo_fs_mountpoints_and_mounts_best_effort() {
    let exec = OciExec {
        entrypoint: vec!["busybox".to_owned(), "httpd".to_owned()],
        cmd: vec![],
        env: vec![],
        workdir: "/".to_owned(),
    };
    let init = render_init(&Entrypoint::Exec(exec), &[], None).unwrap();
    assert!(
        init.contains("mkdir -p /proc /sys /dev"),
        "must create pseudo-fs mountpoints (minimal images lack them); got:\n{init}"
    );
    let mkdir_at = init
        .find("mkdir -p /proc /sys /dev")
        .expect("mkdir present");
    let proc_at = init.find("mount -t proc").expect("proc mount present");
    assert!(
        mkdir_at < proc_at,
        "mkdir must precede the mounts; got:\n{init}"
    );
    assert!(
        init.contains("mount -t proc proc /proc 2>/dev/null || true"),
        "proc mount must be best-effort; got:\n{init}"
    );
    assert!(
        init.contains("mount -t sysfs sysfs /sys 2>/dev/null || true"),
        "sysfs mount must be best-effort; got:\n{init}"
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
        env: vec![
            "GREETING=hello world".to_owned(),
            "PATTERN=$HOME/*".to_owned(),
        ],
        workdir: "/app".to_owned(),
    };
    let init = render_init(&Entrypoint::Exec(exec), &[], None).unwrap();

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
    let init = render_init(&Entrypoint::Exec(exec), &[], None).unwrap();
    // it's fine  ->  'it'\''s fine'
    assert!(
        init.contains(r#"exec '/bin/echo' 'it'\''s fine'"#),
        "embedded single quote must use the POSIX '\\'' idiom; got:\n{init}"
    );
}

/// FIX 3 regression: OCI/Docker auto-create a missing WorkingDir. With `set -e`,
/// a bare `cd <workdir>` to a non-existent dir kills PID 1 at boot. The init must
/// `mkdir -p` the workdir before `cd`, and the workdir must be single-quoted
/// (FIX 1) so a path with special chars survives.
#[test]
fn render_init_creates_workdir_before_cd() {
    let exec = OciExec {
        entrypoint: vec!["/srv/app".to_owned()],
        cmd: Vec::new(),
        env: Vec::new(),
        workdir: "/var/My App".to_owned(),
    };
    let init = render_init(&Entrypoint::Exec(exec), &[], None).unwrap();
    assert!(
        init.contains("mkdir -p '/var/My App'"),
        "must mkdir -p the workdir (single-quoted) before cd; got:\n{init}"
    );
    assert!(
        init.contains("cd '/var/My App'"),
        "must cd into the single-quoted workdir; got:\n{init}"
    );
    // mkdir must come before cd.
    let mkdir_at = init.find("mkdir -p '/var/My App'").unwrap();
    let cd_at = init.find("cd '/var/My App'").unwrap();
    assert!(mkdir_at < cd_at, "mkdir -p must precede cd; got:\n{init}");
}

/// Shell-form (empty entrypoint, no parseable argv) is DEFERRED (D3): render
/// must return a clear "shell-form not yet supported" error, not silently
/// guess a shell.
#[test]
fn render_init_shell_form_returns_clear_error() {
    let err = render_init(&Entrypoint::ShellForm, &[], None).unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("shell-form") && msg.contains("not") && msg.contains("support"),
        "must clearly say shell-form is unsupported; got: {err}"
    );
}

/// The generic wrapper `render_init` produces is itself injected at `/init`
/// (`inject_init`). An image whose OWN entrypoint is `/init` makes the wrapper
/// `exec /init` re-exec itself forever — a silent boot recursion that never
/// readies (observed live: the workspace image hung invisibly in a 30s respawn
/// loop with zero console output). `render_init` must reject `/init` LOUDLY at
/// conversion time instead of shipping a workspace that hangs.
#[test]
fn render_init_rejects_reserved_init_entrypoint() {
    let exec = OciExec {
        entrypoint: vec!["/init".to_owned()],
        cmd: vec![],
        env: vec![],
        workdir: "/".to_owned(),
    };
    let err = render_init(&Entrypoint::Exec(exec), &[], None).unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("/init") && msg.contains("collide"),
        "must clearly flag the /init collision; got: {err}"
    );
}

/// A distinct init path (the fix the workspace + devbox images use) must NOT
/// trip the `/init` collision guard and must exec that path as PID 1.
#[test]
fn render_init_allows_distinct_init_path() {
    let exec = OciExec {
        entrypoint: vec!["/usr/local/bin/tabbify-workspace-init".to_owned()],
        cmd: vec![],
        env: vec![],
        workdir: "/".to_owned(),
    };
    let init = render_init(&Entrypoint::Exec(exec), &[], None).unwrap();
    assert!(
        init.contains("exec '/usr/local/bin/tabbify-workspace-init'"),
        "must exec the distinct init path; got:\n{init}"
    );
}

// ── extra_env: deploy-time vars baked into the guest /init ───────────────

/// `merge_extra_env` — the EXACT primitive `resolve_rootfs` calls before
/// `render_init` — appends deploy-time entries AFTER the OCI image's vars
/// (so extras win on key collision: POSIX, last export wins) and emits the
/// extras in SORTED key order (`HashMap` iteration is random-seeded per
/// process; unsorted extras would make the rendered `/init` — and the rootfs
/// bytes — nondeterministic across builds). The exact-sequence assertion pins
/// both contracts: insertion is deliberately NON-alphabetical so the unsorted
/// impl fails this test with high probability.
#[test]
fn extra_env_merged_after_oci_env_and_exported_in_order() {
    let mut exec = OciExec {
        entrypoint: vec!["/bin/app".to_owned()],
        cmd: vec![],
        env: vec!["A=1".to_owned(), "B=oci".to_owned()],
        workdir: "/".to_owned(),
    };
    // Five extra keys inserted in NON-alphabetical order; B collides with OCI B.
    let extra: std::collections::HashMap<String, String> = [
        ("Z".to_owned(), "z".to_owned()),
        ("B".to_owned(), "override".to_owned()),
        ("Q".to_owned(), "q".to_owned()),
        ("D".to_owned(), "d".to_owned()),
        ("M".to_owned(), "m".to_owned()),
    ]
    .into_iter()
    .collect();
    // The REAL production merge — the same fn `resolve_rootfs` invokes.
    merge_extra_env(&mut exec.env, &extra);

    let init = render_init(&Entrypoint::Exec(exec), &[], None).unwrap();

    // EXACT export sequence: OCI vars first (insertion order), then ALL extras
    // in sorted key order. B appears twice — OCI first, extra last (so the
    // extra definition wins in POSIX sh).
    let exports: Vec<&str> = init.lines().filter(|l| l.starts_with("export ")).collect();
    assert_eq!(
        exports,
        vec![
            "export A='1'",
            "export B='oci'",
            "export B='override'",
            "export D='d'",
            "export M='m'",
            "export Q='q'",
            "export Z='z'",
        ],
        "exports must be: OCI vars in order, then extras in sorted key order; got:\n{init}"
    );
}

/// Extra env values with special shell characters (spaces, $, quotes) are
/// single-quoted in the rendered init, just like OCI env values — no injection.
/// Drives the same `merge_extra_env` → `render_init` pipeline as production.
#[test]
fn extra_env_values_are_single_quoted_in_init() {
    let mut exec = OciExec {
        entrypoint: vec!["/bin/app".to_owned()],
        cmd: vec![],
        env: vec![],
        workdir: "/".to_owned(),
    };
    let extra: std::collections::HashMap<String, String> =
        [("KEY".to_owned(), "ssh-ed25519 AAAA spaced key".to_owned())]
            .into_iter()
            .collect();
    merge_extra_env(&mut exec.env, &extra);

    let init = render_init(&Entrypoint::Exec(exec), &[], None).unwrap();
    assert!(
        init.contains("export KEY='ssh-ed25519 AAAA spaced key'"),
        "extra env value with spaces must be single-quoted in init; got:\n{init}"
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
    let cfg: oci_spec::image::ImageConfiguration = serde_json::from_str(json).unwrap();
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
    let cfg2: oci_spec::image::ImageConfiguration = serde_json::from_str(empty).unwrap();
    assert!(matches!(Entrypoint::from_oci(&cfg2), Entrypoint::ShellForm));
}

/// The rootfs cache env-fingerprint for a NO-env / no-cap build — the common test
/// case (`resolve_rootfs` / `run_firecracker_build` called with `extra_env = None`,
/// no cap-files). The per-uuid cache path nests this fingerprint UNDER the digest
/// dir (#106), so any test that stages or asserts a per-uuid cache path must key it
/// by the SAME value `resolve_rootfs` computes internally for a no-env build.
fn no_env_hash() -> String {
    super::rootfs_env_fingerprint(None, &[])
}

/// The cache path is keyed by the IMMUTABLE digest (sha256:…), not the tag,
/// sanitizes the `:` so it's a valid single path segment, and nests the
/// deploy-env fingerprint UNDER the digest dir (#106).
#[test]
fn cached_rootfs_path_is_keyed_by_digest_under_data_dir() {
    let data_dir = Path::new("/var/lib/tabbify");
    let eh = no_env_hash();
    let p = cached_rootfs_path(
        data_dir,
        "0191e7c2-1111-7222-8333-444455556666",
        "sha256:deadbeefcafe",
        &eh,
    );
    assert_eq!(
        p,
        Path::new(&format!(
            "/var/lib/tabbify/apps/0191e7c2-1111-7222-8333-444455556666/fc/sha256-deadbeefcafe/{eh}/rootfs.ext4"
        ))
    );
}

/// Two different digests for the same app yield distinct cache dirs (a new
/// build never clobbers the old rootfs — immutable-by-digest).
#[test]
fn cached_rootfs_path_differs_per_digest() {
    let d = Path::new("/data");
    let eh = no_env_hash();
    let a = cached_rootfs_path(d, "app", "sha256:aaaa", &eh);
    let b = cached_rootfs_path(d, "app", "sha256:bbbb", &eh);
    assert_ne!(a, b);
    // `<digest-sanitized>` is the GRANDPARENT now — the env fingerprint nests
    // under it (`…/fc/<digest>/<env_hash>/rootfs.ext4`).
    assert!(
        a.parent()
            .unwrap()
            .parent()
            .unwrap()
            .ends_with("sha256-aaaa")
    );
}

/// A CHANGED deploy env on the SAME uuid+digest lands at a DIFFERENT cache path
/// (#106) so the rootfs is re-baked instead of served stale; an identical env
/// yields the SAME path (a cache HIT). This is the core of the workspace add_repo
/// / forge-rewrite fix.
#[test]
fn cached_rootfs_path_differs_per_env_hash() {
    let d = Path::new("/data");
    let (uuid, digest) = ("ws-stable", "sha256:cafe");

    let empty = super::rootfs_env_fingerprint(None, &[]);
    let with_forge = super::rootfs_env_fingerprint(
        Some(&std::collections::HashMap::from([(
            "TABBIFY_FORGE_URL".to_owned(),
            "http://forge".to_owned(),
        )])),
        &[],
    );
    // Adding a cap-file (an add_repo `<repo>.url`) must ALSO change the fingerprint
    // even though the effective env is unchanged — the add_repo case lives in the
    // cap-file NAME set, not in `extra_env`.
    let with_repo =
        super::rootfs_env_fingerprint(None, &[("tetris.url".to_owned(), "http://g".to_owned())]);

    assert_ne!(empty, with_forge, "an env change must change the fingerprint");
    assert_ne!(
        empty, with_repo,
        "an add_repo cap-file must change the fingerprint (its NAME set changed)"
    );
    assert_ne!(with_forge, with_repo);

    // Same uuid+digest, different env_hash ⇒ different cache path (miss → re-bake).
    assert_ne!(
        cached_rootfs_path(d, uuid, digest, &empty),
        cached_rootfs_path(d, uuid, digest, &with_repo),
    );
    // Identical env ⇒ identical path (hit). A cap-file VALUE change with the SAME
    // name set does NOT churn the fingerprint (values are excluded).
    assert_eq!(
        cached_rootfs_path(d, uuid, digest, &with_repo),
        cached_rootfs_path(
            d,
            uuid,
            digest,
            &super::rootfs_env_fingerprint(
                None,
                &[("tetris.url".to_owned(), "http://DIFFERENT".to_owned())]
            )
        ),
        "same cap-file NAME set (only the value rotated) must stay a cache HIT"
    );
}

/// `split_env_and_caps` REMOVES the reserved `CAP_FILES_ENV` key from the
/// effective env and decodes it into `(name, value)` cap-files, leaving the other
/// vars intact. Traversal-unsafe cap names are dropped.
#[test]
fn split_env_and_caps_extracts_cap_files() {
    use std::collections::HashMap;
    let caps_json = r#"{"apartami.url":"http://g/a","../evil":"nope","forge-admin.token":"s"}"#;
    let env = HashMap::from([
        ("TABBIFY_FORGE_URL".to_owned(), "http://forge".to_owned()),
        (crate::api::CAP_FILES_ENV.to_owned(), caps_json.to_owned()),
    ]);
    let (eff, mut caps) = super::split_env_and_caps(Some(&env));
    // The reserved key is stripped; the ordinary var survives.
    assert!(!eff.contains_key(crate::api::CAP_FILES_ENV));
    assert_eq!(eff.get("TABBIFY_FORGE_URL").map(String::as_str), Some("http://forge"));
    // The unsafe `../evil` name is dropped; the two safe caps remain.
    caps.sort();
    let names: Vec<&str> = caps.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, vec!["apartami.url", "forge-admin.token"]);
}

/// `effective_env_hash` derives the SAME #106 fingerprint the rootfs bake uses:
/// it splits out `CAP_FILES_ENV` first, so its value equals
/// `rootfs_env_fingerprint(effective_env, cap_files)`. This is the tie that makes
/// the orchestrator's force-cold-on-env-change comparison agree with what the
/// runner actually bakes into `/init` (else a warm/cold decision could diverge
/// from the rootfs cache key).
#[test]
fn effective_env_hash_matches_the_split_fingerprint() {
    use std::collections::HashMap;
    let caps_json = r#"{"apartami.url":"http://g/a","forge-admin.token":"secret"}"#;
    let env = HashMap::from([
        ("TABBIFY_FORGE_URL".to_owned(), "http://forge".to_owned()),
        (crate::api::CAP_FILES_ENV.to_owned(), caps_json.to_owned()),
    ]);
    let (eff, caps) = super::split_env_and_caps(Some(&env));
    let manual = super::rootfs_env_fingerprint(Some(&eff), &caps);
    assert_eq!(super::effective_env_hash(Some(&env)), manual);

    // An env-less deploy hashes to the same fixed constant either way.
    assert_eq!(super::effective_env_hash(None), no_env_hash());
    assert_eq!(
        super::effective_env_hash(Some(&HashMap::new())),
        no_env_hash(),
        "an empty map must hash identically to None (the empty constant)"
    );
}

/// #108 CORE: adding a repo cap to `CAP_FILES_ENV` (a workspace `add_repo`)
/// CHANGES `effective_env_hash` even though every other var — and the image — is
/// unchanged. This is exactly the signal the orchestrator uses to force a cold
/// respawn instead of a stale warm swap. A cap VALUE rotation with the SAME name
/// set does NOT change it (values are excluded from the fingerprint).
#[test]
fn effective_env_hash_changes_when_add_repo_appends_a_cap() {
    use std::collections::HashMap;
    let base_var = ("TABBIFY_WORKSPACE_UUID".to_owned(), "ws-1".to_owned());

    let before = HashMap::from([
        base_var.clone(),
        (
            crate::api::CAP_FILES_ENV.to_owned(),
            r#"{"apartami.url":"http://g/a"}"#.to_owned(),
        ),
    ]);
    // add_repo merges a NEW `<repo>.url` cap into the CAP_FILES_ENV map.
    let after = HashMap::from([
        base_var.clone(),
        (
            crate::api::CAP_FILES_ENV.to_owned(),
            r#"{"apartami.url":"http://g/a","tetris.url":"http://g/t"}"#.to_owned(),
        ),
    ]);
    assert_ne!(
        super::effective_env_hash(Some(&before)),
        super::effective_env_hash(Some(&after)),
        "add_repo appending a new clone cap MUST change the env fingerprint (→ force cold)"
    );

    // Same cap NAME set, rotated VALUE ⇒ unchanged fingerprint (no needless cold).
    let after_value_rotated = HashMap::from([
        base_var,
        (
            crate::api::CAP_FILES_ENV.to_owned(),
            r#"{"apartami.url":"http://g/a-ROTATED"}"#.to_owned(),
        ),
    ]);
    assert_eq!(
        super::effective_env_hash(Some(&before)),
        super::effective_env_hash(Some(&after_value_rotated)),
        "a cap VALUE rotation (same NAME set) must NOT force a cold respawn"
    );
}

/// `rootfs_is_cached` is true iff the digest+env-keyed rootfs.ext4 exists.
#[test]
fn rootfs_is_cached_reflects_presence() {
    let tmp = tempfile::tempdir().unwrap();
    let digest = "sha256:abc123";
    let eh = no_env_hash();
    assert!(!super::rootfs_is_cached(tmp.path(), "app", digest, &eh));
    let p = cached_rootfs_path(tmp.path(), "app", digest, &eh);
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(&p, b"\0").unwrap();
    assert!(super::rootfs_is_cached(tmp.path(), "app", digest, &eh));
}

// ── GLOBAL digest-shared rootfs cache ───────────────────────────────────────────

/// The GLOBAL cache path is keyed by DIGEST only — NOT the uuid. Same digest ⇒
/// same shared file regardless of which uuid needs it (the dev-session win: a
/// fresh uuid every start reuses one rootfs).
#[test]
fn global_rootfs_path_is_keyed_by_digest_not_uuid() {
    let d = Path::new("/data");
    assert_eq!(
        super::global_rootfs_path(d, "sha256:abcd"),
        super::global_rootfs_path(d, "sha256:abcd")
    );
    assert!(
        super::global_rootfs_path(d, "sha256:abcd")
            .ends_with("rootfs-cache/sha256-abcd/rootfs.ext4")
    );
    assert_ne!(
        super::global_rootfs_path(d, "sha256:abcd"),
        super::global_rootfs_path(d, "sha256:ef01")
    );
}

/// `publish_rootfs_to_global` then `link_global_rootfs_to_uuid` shares ONE inode
/// across uuids: a build for uuid-A populates the global cache; uuid-B (fresh,
/// never built) hard-links it — no rebuild, same content, same inode.
#[tokio::test]
async fn global_cache_publish_then_link_shares_one_inode() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path();
    let digest = "sha256:deadbeef";

    // uuid-A "built" its per-uuid rootfs (env-free → nested under the no-env hash).
    let eh = no_env_hash();
    let a = cached_rootfs_path(data, "uuid-a", digest, &eh);
    std::fs::create_dir_all(a.parent().unwrap()).unwrap();
    std::fs::write(&a, b"ROOTFS-CONTENT").unwrap();

    super::publish_rootfs_to_global(data, digest, &a).await;
    assert!(
        super::global_rootfs_is_cached(data, digest),
        "publish must populate the global cache"
    );

    // uuid-B (fresh) gets it WITHOUT a build, via hard link.
    assert!(!super::rootfs_is_cached(data, "uuid-b", digest, &eh));
    let linked = super::link_global_rootfs_to_uuid(data, "uuid-b", digest, &eh)
        .await
        .expect("global hit must materialize B's per-uuid rootfs");
    assert_eq!(std::fs::read(&linked).unwrap(), b"ROOTFS-CONTENT");

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        assert_eq!(
            std::fs::metadata(&a).unwrap().ino(),
            std::fs::metadata(&linked).unwrap().ino(),
            "global cache must SHARE an inode (hard link), not copy"
        );
    }
}

/// `link_global_rootfs_to_uuid` returns `None` on a global MISS so the caller
/// falls back to pull + build (never a wrong/empty rootfs).
#[tokio::test]
async fn global_cache_link_misses_cleanly() {
    let tmp = tempfile::tempdir().unwrap();
    assert!(
        super::link_global_rootfs_to_uuid(tmp.path(), "uuid-x", "sha256:absent", &no_env_hash())
            .await
            .is_none()
    );
}

/// `evict_global_rootfs_cache` bounds the cache to KEEP entries (so it can never
/// fill the worker disk — a past root-fs-full caused a full outage).
#[tokio::test]
async fn global_cache_eviction_bounds_entry_count() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path();
    let total = super::GLOBAL_ROOTFS_CACHE_KEEP + 3;
    for i in 0..total {
        let p = super::global_rootfs_path(data, &format!("sha256:{i:064x}"));
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, b"\0").unwrap();
    }
    super::evict_global_rootfs_cache(data).await;
    let remaining = std::fs::read_dir(data.join("rootfs-cache"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.path().is_dir())
        .count();
    assert_eq!(
        remaining,
        super::GLOBAL_ROOTFS_CACHE_KEEP,
        "eviction must bound the cache to KEEP entries"
    );
}

// ── Global OCI-layout cache (#57) ─────────────────────────────────────────────

/// Stage a minimal but valid OCI layout dir (the `<work>/oci` shape that
/// `pull_oci_layout` produces): `index.json` (the hit marker) + a blob.
fn stage_fake_layout(oci_root: &Path) {
    std::fs::create_dir_all(oci_root.join("blobs").join("sha256")).unwrap();
    std::fs::write(oci_root.join("index.json"), br#"{"manifests":[]}"#).unwrap();
    std::fs::write(
        oci_root.join("oci-layout"),
        br#"{"imageLayoutVersion":"1.0.0"}"#,
    )
    .unwrap();
    std::fs::write(oci_root.join("blobs").join("sha256").join("abc"), b"BLOB").unwrap();
}

/// `lookup_global_layout` MISSES cleanly when nothing is cached (the caller then
/// pulls) — never a half-built layout.
#[tokio::test]
async fn layout_cache_lookup_misses_cleanly() {
    let tmp = tempfile::tempdir().unwrap();
    assert!(
        super::lookup_global_layout(tmp.path(), "sha256:absent")
            .await
            .is_none()
    );
}

/// An entry dir that exists but lacks `oci/index.json` (a torn/partial publish)
/// is treated as a MISS, so a corrupt layout never feeds a build.
#[tokio::test]
async fn layout_cache_entry_without_index_is_a_miss() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path();
    std::fs::create_dir_all(super::global_oci_layout_entry(data, "sha256:half").join("oci")).unwrap();
    assert!(
        super::lookup_global_layout(data, "sha256:half")
            .await
            .is_none()
    );
}

/// Publish-then-lookup: a build for uuid-A seeds the global LAYOUT cache; a fresh
/// uuid-B finds the SAME layout (no pull), sharing inodes (hard link, not copy)
/// — the dev-FC pull-skip that #57 restores.
#[tokio::test]
async fn layout_cache_publish_then_lookup_shares_inode() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path();
    let digest = "sha256:layoutbeef";

    // uuid-A's freshly-pulled layout (per-uuid work dir).
    let src = data.join("apps").join("uuid-a").join("fc").join(".pull").join("oci");
    stage_fake_layout(&src);

    super::publish_layout_to_global(data, digest, "uuid-a", &src).await;

    let hit = super::lookup_global_layout(data, digest)
        .await
        .expect("publish must populate the global layout cache");
    assert!(hit.join("index.json").is_file());
    assert_eq!(
        std::fs::read(hit.join("blobs").join("sha256").join("abc")).unwrap(),
        b"BLOB"
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        assert_eq!(
            std::fs::metadata(src.join("blobs").join("sha256").join("abc"))
                .unwrap()
                .ino(),
            std::fs::metadata(hit.join("blobs").join("sha256").join("abc"))
                .unwrap()
                .ino(),
            "layout publish must HARD-LINK blobs (share inode), not duplicate the image"
        );
    }
}

/// A second publish for the same digest (a later/concurrent build) is a no-op,
/// not an error or a corruption — the cache stays valid.
#[tokio::test]
async fn layout_cache_publish_is_idempotent() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path();
    let digest = "sha256:dup";
    let src = data.join("apps").join("uuid-a").join("fc").join(".pull").join("oci");
    stage_fake_layout(&src);

    super::publish_layout_to_global(data, digest, "uuid-a", &src).await;
    super::publish_layout_to_global(data, digest, "uuid-b", &src).await;

    assert!(super::lookup_global_layout(data, digest).await.is_some());
}

/// Eviction bounds the layout cache to KEEP entries — the layout cache must never
/// fill the worker disk (a past rootfs-full caused a full outage).
#[tokio::test]
async fn layout_cache_eviction_bounds_entry_count() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path();
    let total = super::GLOBAL_LAYOUT_CACHE_KEEP + 3;
    for i in 0..total {
        let oci = super::global_oci_layout_entry(data, &format!("sha256:{i:064x}")).join("oci");
        std::fs::create_dir_all(&oci).unwrap();
        std::fs::write(oci.join("index.json"), b"{}").unwrap();
    }
    super::evict_global_layout_cache(data).await;
    let remaining = std::fs::read_dir(data.join("oci-layout-cache"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.path().is_dir())
        .count();
    assert_eq!(
        remaining,
        super::GLOBAL_LAYOUT_CACHE_KEEP,
        "eviction must bound the layout cache to KEEP entries"
    );
}

use crate::fetcher::FetchedApp;
use crate::manifest::{AppManifest, AppMeta, Lifecycle, LifecycleMode, Routes, Runtime};
use bytes::Bytes;

fn fc_fetched(digest: &str) -> FetchedApp {
    fc_fetched_ref(&format!("[fd5a::1]:5000/acme/vm@{digest}"))
}

/// Like [`fc_fetched`] but takes the FULL `registry_ref` verbatim, so a test can
/// stage a TAG ref (`…/vm:latest`, no `@<digest>`) as well as a digest ref.
fn fc_fetched_ref(registry_ref: &str) -> FetchedApp {
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
                port: None,
                kernel: None,
                registry_ref: Some(registry_ref.to_owned()),
                stateful: false,
                data_mount: None,
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
    let cached = super::cached_rootfs_path(tmp.path(), "uuid-cache", digest, &no_env_hash());
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
        None,
        &[],
    )
    .await
    .unwrap();

    assert_eq!(rootfs, cached, "cache hit must return the cached rootfs");
    assert!(
        !*called.lock().unwrap(),
        "no conversion command may run on a cache hit"
    );
}

/// The host OCI arch name for the test machine, and an arch name guaranteed to
/// MISMATCH it. Used so the architecture-guard tests are portable across an
/// amd64 CI runner and an arm64 dev box: the matching fixture uses the host
/// arch, the mismatching fixture uses the "other" one.
fn host_and_other_arch() -> (&'static str, &'static str) {
    match super::host_oci_arch() {
        "arm64" => ("arm64", "amd64"),
        host => (host, "arm64"),
    }
}

/// Architecture guard: a cache-miss conversion of an image whose architecture
/// does NOT match the host must FAIL FAST — before the slow layer unpack +
/// `mkfs.ext4` — with an error naming BOTH the image arch and the host arch.
/// The external runner must never be invoked (no `oras`/`tar`/`mkfs`).
#[tokio::test]
async fn resolve_rootfs_rejects_arch_mismatch_before_conversion() {
    let tmp = tempfile::tempdir().unwrap();
    let digest = "sha256:archmism";
    let fetched = fc_fetched(digest);
    let target = super::cached_rootfs_path(tmp.path(), "uuid-arch", digest, &no_env_hash());

    let (_host, other) = host_and_other_arch();
    // An image built for the "other" (non-host) architecture must be rejected.
    let l0 = make_tar(&[("bin/server", b"elf")]);
    let cfg = serde_json::json!({
        "architecture": other, "os": "linux",
        "config": {"Entrypoint": ["/bin/server"]},
        "rootfs": {"type": "layers", "diff_ids": ["sha256:l0"]}
    });
    let work = target.parent().unwrap().to_path_buf();
    let layout = write_min_oci_layout(&work, &cfg, &[("sha256:l0", &l0)]);
    let config = super::read_oci_config_from_layout(&layout).unwrap();

    // The guard must short-circuit BEFORE any external command: assert the
    // runner is never invoked.
    let called: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
    let called2 = called.clone();
    let runner: super::FcBuildRunner = Arc::new(move |_argv| {
        *called2.lock().unwrap() = true;
        Box::pin(async { (true, Vec::new()) })
    });

    let err = super::resolve_rootfs(
        "uuid-arch",
        &fetched,
        &layout,
        &config,
        digest,
        tmp.path(),
        &runner,
        None,
        &[],
    )
    .await
    .expect_err("arch mismatch must fail fast");

    let msg = err.to_string();
    let host = super::host_oci_arch();
    assert!(
        msg.contains(other),
        "error must name the image arch {other:?}; got: {err}"
    );
    assert!(
        msg.contains(host),
        "error must name the host arch {host:?}; got: {err}"
    );
    assert!(
        !*called.lock().unwrap(),
        "no conversion command may run on an arch mismatch (fail fast before unpack/mkfs)"
    );
    assert!(
        !target.is_file(),
        "no rootfs may be produced on an arch mismatch"
    );
}

/// `host_oci_arch` maps the host `std::env::consts::ARCH` to the OCI arch name
/// (`x86_64 -> amd64`, `aarch64 -> arm64`), matching what the guard compares
/// `config.architecture()` against.
#[test]
fn host_oci_arch_maps_known_targets() {
    let h = super::host_oci_arch();
    match std::env::consts::ARCH {
        "x86_64" => assert_eq!(h, "amd64"),
        "aarch64" => assert_eq!(h, "arm64"),
        // On any other host the mapping falls back to the raw ARCH string.
        other => assert_eq!(h, other),
    }
}

/// A matching architecture must NOT be rejected by the guard: a cache-miss
/// conversion of a host-arch image proceeds into the unpack/mkfs path (here the
/// `mkfs.ext4` is faked) and produces the rootfs.
#[tokio::test]
async fn resolve_rootfs_allows_matching_host_arch() {
    let tmp = tempfile::tempdir().unwrap();
    let digest = "sha256:archok01";
    let fetched = fc_fetched(digest);
    let target = super::cached_rootfs_path(tmp.path(), "uuid-archok", digest, &no_env_hash());

    let (host, _other) = host_and_other_arch();
    let l0 = make_tar(&[("bin/server", b"elf")]);
    let cfg = serde_json::json!({
        "architecture": host, "os": "linux",
        "config": {"Entrypoint": ["/bin/server"], "WorkingDir": "/app"},
        "rootfs": {"type": "layers", "diff_ids": ["sha256:l0"]}
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
                (real)(argv).await
            }
        })
    });

    let rootfs = super::resolve_rootfs(
        "uuid-archok",
        &fetched,
        &layout,
        &config,
        digest,
        tmp.path(),
        &runner,
        None,
        &[],
    )
    .await
    .expect("matching host arch must convert");
    assert_eq!(rootfs, target);
    assert!(rootfs.is_file());
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
    let target = super::cached_rootfs_path(tmp.path(), "uuid-miss", digest, &no_env_hash());

    // Stage a real OCI layout where `pull_oci_layout` would have left it. The
    // image is built for the HOST arch so the architecture guard lets it
    // through (this test exercises the conversion path, not the guard).
    let l0 = make_tar(&[("bin/server", b"elf")]);
    let cfg = serde_json::json!({
        "architecture": super::host_oci_arch(),"os":"linux",
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
        None,
        &[],
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
    use sha2::{Digest as _, Sha256};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let tmp = tempfile::tempdir().unwrap();
    let hexf = |b: &[u8]| format!("{:x}", Sha256::digest(b));

    // The image the mock registry serves, built for the HOST arch so the
    // architecture guard lets it through. The real host `tar` unpacks the layer.
    let l0 = make_tar(&[("bin/server", b"elf")]);
    let cfg = serde_json::json!({
        "architecture": super::host_oci_arch(),"os":"linux",
        "config":{"Entrypoint":["/app/server"],"Env":["PATH=/usr/bin"],"WorkingDir":"/app"},
        "rootfs":{"type":"layers","diff_ids":["sha256:l0"]}
    });
    let config_bytes = serde_json::to_vec(&cfg).unwrap();
    let (config_hex, layer_hex) = (hexf(&config_bytes), hexf(&l0));
    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                   "digest": format!("sha256:{config_hex}"), "size": config_bytes.len()},
        "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar",
                    "digest": format!("sha256:{layer_hex}"), "size": l0.len()}],
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_hex = hexf(&manifest_bytes);
    let digest = format!("sha256:{manifest_hex}");

    let server = MockServer::start().await;
    for (p, body) in [
        (format!("/v2/acme/vm/manifests/{digest}"), manifest_bytes.clone()),
        (format!("/v2/acme/vm/blobs/sha256:{config_hex}"), config_bytes.clone()),
        (format!("/v2/acme/vm/blobs/sha256:{layer_hex}"), l0.clone()),
    ] {
        Mock::given(method("GET"))
            .and(path(p))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
            .mount(&server)
            .await;
    }

    // A DIGEST ref whose host is the mock registry.
    let host = server.uri().strip_prefix("http://").unwrap().to_owned();
    let fetched = fc_fetched_ref(&format!("{host}/acme/vm@{digest}"));
    // Every FcConfig field carries a clap default, so an arg-less parse yields a
    // usable config without standing up a real Firecracker host.
    let fc = <crate::config::FcConfig as clap::Parser>::parse_from(["fc"]);
    let target = super::cached_rootfs_path(tmp.path(), "uuid-nodocker", &digest, &no_env_hash());

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
    let _ = super::run_firecracker_build(
        "uuid-nodocker",
        &fetched,
        &fc,
        tmp.path(),
        &runner,
        false,
        None,
        None,
    )
    .await;

    let recorded = calls.lock().unwrap().clone();
    assert!(
        !recorded
            .iter()
            .any(|c| c.first().map(String::as_str) == Some("docker")),
        "FC conversion is docker-less; no argv may start with `docker`; got {recorded:?}"
    );
    // The docker-less pull+convert actually ran (mkfs happened on the cache miss).
    assert!(
        recorded
            .iter()
            .any(|c| c.first().map(String::as_str) == Some("mkfs.ext4")),
        "the cache-miss conversion must reach mkfs.ext4; got {recorded:?}"
    );
}

/// `pull_oci_layout` pulls the image over plain HTTP (the resumable
/// `crate::oci_pull` puller) into `<out>/oci`, producing the spec-compliant OCI
/// LAYOUT the downstream readers consume: `oci-layout` + `index.json` +
/// `blobs/sha256/<hex>` for the manifest, config, and every layer. The registry
/// is a local mock HTTP server; the ref host points at it.
#[tokio::test]
async fn pull_oci_layout_produces_layout_over_http() {
    use sha2::{Digest as _, Sha256};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    let hex = |b: &[u8]| format!("{:x}", Sha256::digest(b));

    // A tiny single-platform image: config blob + one layer blob.
    let config = b"opaque-config".to_vec();
    let layer = b"opaque-layer-bytes".to_vec();
    let (config_hex, layer_hex) = (hex(&config), hex(&layer));
    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                   "digest": format!("sha256:{config_hex}"), "size": config.len()},
        "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar",
                    "digest": format!("sha256:{layer_hex}"), "size": layer.len()}],
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_hex = hex(&manifest_bytes);

    for (p, body) in [
        (format!("/v2/acme/app/manifests/sha256:{manifest_hex}"), manifest_bytes.clone()),
        (format!("/v2/acme/app/blobs/sha256:{config_hex}"), config.clone()),
        (format!("/v2/acme/app/blobs/sha256:{layer_hex}"), layer.clone()),
    ] {
        Mock::given(method("GET"))
            .and(path(p))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
            .mount(&server)
            .await;
    }

    let host = server.uri().strip_prefix("http://").unwrap().to_owned();
    let reff = format!("{host}/acme/app@sha256:{manifest_hex}");
    let tmp = tempfile::tempdir().unwrap();
    let out = tmp.path().join("work");

    let layout = super::pull_oci_layout(&reff, &out, None)
        .await
        .expect("resumable http pull must succeed");
    assert_eq!(layout, out.join("oci"), "layout dir is <out>/oci");
    assert!(layout.join("oci-layout").is_file(), "oci-layout marker present");
    assert!(layout.join("index.json").is_file(), "index.json present");
    let blobs = layout.join("blobs").join("sha256");
    for h in [&manifest_hex, &config_hex, &layer_hex] {
        assert!(blobs.join(h).is_file(), "blob {h} must be present");
    }
}

/// An UNREACHABLE registry (connection refused) makes the resumable pull error
/// out after its bounded no-progress budget, with a message naming the pull step.
#[tokio::test(start_paused = true)]
async fn pull_oci_layout_errors_when_registry_unreachable() {
    let tmp = tempfile::tempdir().unwrap();
    // Port 1 is never listening → every GET is refused → no blob ever converges.
    let err = super::pull_oci_layout("127.0.0.1:1/acme/app@sha256:abc", tmp.path(), None)
        .await
        .expect_err("an unreachable registry must error");
    assert!(
        err.to_string().to_lowercase().contains("pull"),
        "error must name the pull step; got: {err}"
    );
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
    assert_eq!(
        inner.entrypoint().as_ref().unwrap(),
        &vec!["/app/server".to_owned()]
    );
    assert_eq!(inner.working_dir().as_ref().unwrap(), "/app");
    assert_eq!(parsed.rootfs().diff_ids(), &vec!["sha256:aaaa".to_owned()]);
}

/// A layout with no image manifest in index.json errors clearly.
#[test]
fn read_oci_config_from_layout_errors_on_empty_index() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(
        tmp.path().join("index.json"),
        br#"{"schemaVersion":2,"manifests":[]}"#,
    )
    .unwrap();
    let err = super::read_oci_config_from_layout(tmp.path()).unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("manifest"),
        "must name the missing manifest; got: {err}"
    );
}

/// `unpack_oci_layers` untars layers in order and applies OCI whiteouts:
/// - `.wh.<name>` removes `<name>` carried by an earlier layer,
/// - `.wh..wh..opq` clears the directory's earlier contents,
/// and the `.wh.*` markers themselves never survive into the staging tree.
#[tokio::test]
async fn unpack_oci_layers_applies_whiteouts_in_order() {
    let tmp = tempfile::tempdir().unwrap();
    // Layer 0: a/keep.txt, a/drop.txt, b/old.txt
    let l0 = make_tar(&[
        ("a/keep.txt", b"k"),
        ("a/drop.txt", b"d"),
        ("b/old.txt", b"o"),
    ]);
    // Layer 1: whiteout a/drop.txt + opaque b/ + b/new.txt
    let l1 = make_tar(&[
        ("a/.wh.drop.txt", b""),
        ("b/.wh..wh..opq", b""),
        ("b/new.txt", b"n"),
    ]);
    let cfg = serde_json::json!({
        "architecture":"amd64","os":"linux",
        "config":{"Entrypoint":["/x"]},
        "rootfs":{"type":"layers","diff_ids":["sha256:l0","sha256:l1"]}
    });
    let layout = write_min_oci_layout(tmp.path(), &cfg, &[("sha256:l0", &l0), ("sha256:l1", &l1)]);
    let config = super::read_oci_config_from_layout(&layout).unwrap();
    let staging = tmp.path().join("stage");

    // Real `tar` via the runner (shells the host tar binary).
    super::unpack_oci_layers(&layout, &config, &staging)
        .await
        .expect("unpack must succeed");

    assert!(staging.join("a/keep.txt").is_file(), "kept file survives");
    assert!(
        !staging.join("a/drop.txt").exists(),
        ".wh.drop.txt must delete it"
    );
    assert!(
        !staging.join("b/old.txt").exists(),
        "opaque dir clears earlier b/ contents"
    );
    assert!(
        staging.join("b/new.txt").is_file(),
        "new file in opaque layer survives"
    );
    assert!(
        !staging.join("a/.wh.drop.txt").exists(),
        "wh marker must not survive"
    );
    assert!(
        !staging.join("b/.wh..wh..opq").exists(),
        "opq marker must not survive"
    );
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
    let layout = write_min_oci_layout(tmp.path(), &cfg, &[("sha256:l0", &l0), ("sha256:l1", &l1)]);
    let config = super::read_oci_config_from_layout(&layout).unwrap();
    let staging = tmp.path().join("stage");

    super::unpack_oci_layers(&layout, &config, &staging)
        .await
        .expect("unpack must succeed");

    assert!(
        !staging.join("b/old.txt").exists(),
        "opaque dir clears the lower layer's b/old.txt"
    );
    assert!(
        staging.join("b/keep.txt").is_file(),
        "same-layer re-add of b/keep.txt must survive the opaque clear"
    );
    assert_eq!(
        std::fs::read(staging.join("b/keep.txt")).unwrap(),
        b"new",
        "the surviving b/keep.txt must be the layer-1 content, not the lower one",
    );
}

/// FIX 2 regression: an upper layer that turns a lower-layer regular FILE into a
/// DIRECTORY at the same path must succeed. The dirs-overlay loop must first
/// remove a colliding non-directory at the target (mirroring the files loop's
/// guard) before `create_dir_all`, or the whole conversion aborts with
/// NotADirectory / AlreadyExists.
#[tokio::test]
async fn unpack_oci_layers_replaces_lower_file_with_upper_dir() {
    let tmp = tempfile::tempdir().unwrap();
    // Layer 0: regular file "foo".
    let l0 = make_tar(&[("foo", b"i am a file")]);
    // Layer 1: directory "foo/" containing "foo/bar".
    let l1 = make_tar(&[("foo/bar", b"i am under a dir")]);
    let cfg = serde_json::json!({
        "architecture":"amd64","os":"linux",
        "config":{"Entrypoint":["/x"]},
        "rootfs":{"type":"layers","diff_ids":["sha256:l0","sha256:l1"]}
    });
    let layout = write_min_oci_layout(tmp.path(), &cfg, &[("sha256:l0", &l0), ("sha256:l1", &l1)]);
    let config = super::read_oci_config_from_layout(&layout).unwrap();
    let staging = tmp.path().join("stage");

    super::unpack_oci_layers(&layout, &config, &staging)
        .await
        .expect("file-to-dir replacement across layers must succeed");

    assert!(
        staging.join("foo").is_dir(),
        "lower-layer file 'foo' must become a directory from the upper layer"
    );
    assert!(
        staging.join("foo/bar").is_file(),
        "the upper layer's foo/bar must materialize"
    );
}

/// FIX 4 regression: the layer-unpack tar argv must carry the right
/// decompression flag derived from the layer's media type — `tar` autodetect is
/// unreliable (busybox/older tar cannot autodetect zstd), so the flag must be
/// explicit. Covers BOTH the OCI media types and the Docker v2s2 equivalents
/// real images ship with.
#[test]
fn tar_decompress_flag_branches_on_media_type() {
    use oci_spec::image::MediaType;

    // gzip → -z (both OCI and Docker spellings).
    assert_eq!(
        super::tar_decompress_flag(&MediaType::ImageLayerGzip),
        Some("-z")
    );
    assert_eq!(
        super::tar_decompress_flag(&MediaType::from(
            "application/vnd.docker.image.rootfs.diff.tar.gzip"
        )),
        Some("-z")
    );
    // zstd → --zstd (both OCI and Docker spellings).
    assert_eq!(
        super::tar_decompress_flag(&MediaType::ImageLayerZstd),
        Some("--zstd")
    );
    assert_eq!(
        super::tar_decompress_flag(&MediaType::from(
            "application/vnd.docker.image.rootfs.diff.tar.zstd"
        )),
        Some("--zstd")
    );
    // plain tar → no flag (let tar read the raw archive).
    assert_eq!(super::tar_decompress_flag(&MediaType::ImageLayer), None);
    assert_eq!(
        super::tar_decompress_flag(&MediaType::from(
            "application/vnd.docker.image.rootfs.diff.tar"
        )),
        None
    );
}

/// FIX 4: the assembled untar argv must include the media-type-derived
/// decompression flag (`-z` for gzip, `--zstd` for zstd) ahead of `-f <blob>`.
#[test]
fn unpack_tar_argv_includes_decompress_flag() {
    let blob = Path::new("/blobs/sha256/abc");
    let out = Path::new("/stage/layer-0");

    let gz = super::unpack_tar_argv("-z", blob, out);
    assert_eq!(gz.first().map(String::as_str), Some("tar"));
    assert!(
        gz.contains(&"-z".to_owned()),
        "gzip layer must pass -z; got {gz:?}"
    );
    // The decompress flag must precede -f so tar reads the blob compressed.
    let z_at = gz.iter().position(|a| a == "-z").unwrap();
    let f_at = gz.iter().position(|a| a == "-f").unwrap();
    assert!(z_at < f_at, "decompress flag must precede -f; got {gz:?}");

    let zstd = super::unpack_tar_argv("--zstd", blob, out);
    assert!(
        zstd.contains(&"--zstd".to_owned()),
        "zstd layer must pass --zstd; got {zstd:?}"
    );

    // Plain tar: empty flag yields no spurious arg, still `-f <blob> -C <out>`.
    let plain = super::unpack_tar_argv("", blob, out);
    assert!(
        !plain.iter().any(|a| a == "-z" || a == "--zstd"),
        "plain tar must carry no decompress flag; got {plain:?}"
    );
    assert!(plain.contains(&"-f".to_owned()) && plain.contains(&"-C".to_owned()));
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
    let err = super::unpack_oci_layers(&layout, &config, &tmp.path().join("s"))
        .await
        .unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("layer"),
        "must name the layer/diff_id mismatch; got: {err}"
    );
}

use super::oci_fixtures::{
    MEDIA_TAR_GZIP, MEDIA_TAR_ZSTD, make_tar_gzip, make_tar_gzip_modes, make_tar_zstd,
    write_min_oci_layout_typed,
};

/// PORTABLE (all-OS) prerequisite for the linux-gated real-conversion test: the
/// new compressed-layer fixtures must (a) emit REAL gzip/zstd bytes a host `tar`
/// could inflate, (b) stage a spec-compliant layout whose layer descriptors
/// carry the gzip/zstd OCI media type, and (c) be wired so the PRODUCTION
/// `tar_decompress_flag` selects the matching `-z`/`--zstd` flag for those
/// layers. This compiles + runs everywhere (so macOS CI exercises the fixtures);
/// the actual `tar -z` unpack + real `mkfs.ext4` is the linux-gated `#[ignore]`
/// test below.
#[test]
fn compressed_layer_fixtures_stage_real_oci_layout() {
    use oci_spec::image::MediaType;

    // (a) The gzip fixture is REAL gzip: a gzip member starts with 0x1f 0x8b and
    //     inflates back to a tar whose header names the entry. zstd starts with
    //     its 0x28 0xb5 0x2f 0xfd magic.
    let gz = make_tar_gzip(&[("bin/server", b"elf-bytes")]);
    assert_eq!(
        &gz[..2],
        &[0x1f, 0x8b],
        "gzip layer must carry the gzip magic"
    );
    let zst = make_tar_zstd(&[("bin/server", b"elf-bytes")]);
    assert_eq!(
        &zst[..4],
        &[0x28, 0xb5, 0x2f, 0xfd],
        "zstd layer must carry the zstd magic"
    );
    // gzip inflates to the original tar (whose ustar magic + entry name survive).
    let inflated = {
        use std::io::Read as _;
        let mut dec = flate2::read::GzDecoder::new(gz.as_slice());
        let mut out = Vec::new();
        dec.read_to_end(&mut out).unwrap();
        out
    };
    assert!(
        inflated.windows(7).any(|w| w == b"ustar\0\0") || inflated.len() >= 512,
        "inflated gzip must be a tar archive"
    );
    assert!(
        inflated.windows(10).any(|w| w == b"bin/server"),
        "inflated gzip tar must contain the staged entry path"
    );

    // (b) A layout staged with the gzip media type carries it on the descriptor.
    let tmp = tempfile::tempdir().unwrap();
    let cfg = serde_json::json!({
        "architecture": super::host_oci_arch(), "os": "linux",
        "config": {"Entrypoint": ["/bin/server"]},
        "rootfs": {"type": "layers", "diff_ids": ["sha256:l0"]}
    });
    let layout =
        write_min_oci_layout_typed(tmp.path(), &cfg, &[("sha256:l0", &gz, MEDIA_TAR_GZIP)]);
    let index = oci_spec::image::ImageIndex::from_file(layout.join("index.json")).unwrap();
    let man_desc = index.manifests().first().unwrap();
    let blob = layout
        .join("blobs")
        .join(man_desc.digest().algorithm().as_ref())
        .join(man_desc.digest().digest());
    let manifest = oci_spec::image::ImageManifest::from_file(blob).unwrap();
    let layer_mt = manifest.layers()[0].media_type().to_string();
    assert_eq!(
        layer_mt, MEDIA_TAR_GZIP,
        "the staged layer descriptor must carry the gzip OCI media type"
    );

    // (c) The production decompress-flag selector keys off that media type.
    assert_eq!(
        super::tar_decompress_flag(&MediaType::from(MEDIA_TAR_GZIP)),
        Some("-z"),
        "gzip layer media type must select tar -z"
    );
    assert_eq!(
        super::tar_decompress_flag(&MediaType::from(MEDIA_TAR_ZSTD)),
        Some("--zstd"),
        "zstd layer media type must select tar --zstd"
    );

    // The explicit-mode gzip fixture (used for the 0o755 /init + entrypoint in
    // the linux test) also emits real gzip.
    let gz_modes = make_tar_gzip_modes(&[("init", b"#!/bin/sh\n", 0o755)]);
    assert_eq!(&gz_modes[..2], &[0x1f, 0x8b]);
}

/// REAL end-to-end conversion (linux-gated, `#[ignore]`): drive the PRODUCTION
/// [`super::build_rootfs_ext4`] with the PRODUCTION
/// [`super::production_fc_build_runner`] against a layout whose layers carry the
/// gzip OCI media type — so the real host `tar -z` inflates the layers and the
/// real `mkfs.ext4 -F -d` (e2fsprogs) populates the ext4 — then inspect the
/// produced `rootfs.ext4` with `dumpe2fs`/`debugfs` to assert it is a VALID ext4
/// containing `/init` at mode 0755, the entrypoint binary, and that an OCI
/// whiteout from an upper layer was applied (the deleted file is absent).
///
/// Gated to Linux + e2fsprogs (`mkfs.ext4`/`dumpe2fs`/`debugfs`) and `#[ignore]`
/// so it runs only on the FC node (ThinkPad / CI), never on the macOS dev box.
#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "requires Linux + e2fsprogs (mkfs.ext4/dumpe2fs/debugfs)"]
async fn real_conversion_gzip_layers_and_mkfs_ext4_produces_valid_rootfs() {
    // Two GZIP layers exercising the real `tar -z` unpack + an OCI whiteout:
    //  - layer 0: an executable /init (0o755), the entrypoint binary /bin/server
    //    (0o755), and a file /etc/drop.me that an upper layer must delete.
    //  - layer 1: a `.wh.drop.me` whiteout under /etc removing the lower file.
    let init_script = b"#!/bin/sh\nexec /bin/server\n";
    let server_bin = b"\x7fELF-fake-entrypoint-binary";
    let l0 = make_tar_gzip_modes(&[
        ("init", init_script, 0o755),
        ("bin/server", server_bin, 0o755),
        ("etc/drop.me", b"delete me", 0o644),
    ]);
    let l1 = make_tar_gzip(&[("etc/.wh.drop.me", b"")]);

    let cfg = serde_json::json!({
        "architecture": super::host_oci_arch(), "os": "linux",
        "config": {"Entrypoint": ["/bin/server"], "WorkingDir": "/"},
        "rootfs": {"type": "layers", "diff_ids": ["sha256:l0", "sha256:l1"]}
    });

    let tmp = tempfile::tempdir().unwrap();
    let out_dir = tmp.path().join("out");
    let layout = write_min_oci_layout_typed(
        &out_dir,
        &cfg,
        &[
            ("sha256:l0", &l0, MEDIA_TAR_GZIP),
            ("sha256:l1", &l1, MEDIA_TAR_GZIP),
        ],
    );
    let config = super::read_oci_config_from_layout(&layout).unwrap();

    // PRODUCTION runner: real host `tar -z` unpack + real `mkfs.ext4 -F -d`.
    let runner = super::production_fc_build_runner();
    let rootfs = super::build_rootfs_ext4(&layout, &config, &out_dir, 64, &runner)
        .await
        .expect("real OCI gzip-layer -> ext4 conversion must succeed");
    assert!(rootfs.is_file(), "rootfs.ext4 must exist");

    // 1. dumpe2fs proves it's a valid ext4 superblock (the filesystem magic is
    //    parseable by e2fsprogs).
    let dump = std::process::Command::new("dumpe2fs")
        .arg(&rootfs)
        .output()
        .expect("dumpe2fs must run on the FC node");
    assert!(
        dump.status.success(),
        "dumpe2fs must accept the produced image as a valid ext4; stderr: {}",
        String::from_utf8_lossy(&dump.stderr)
    );
    let dump_out = String::from_utf8_lossy(&dump.stdout);
    assert!(
        dump_out.contains("Filesystem volume name")
            || dump_out.contains("Inode count")
            || dump_out.to_lowercase().contains("ext"),
        "dumpe2fs output must describe an ext filesystem; got:\n{dump_out}"
    );

    // 2. debugfs introspects the contents WITHOUT mounting (rootless): `stat`
    //    each path and read the directory listings.
    let debugfs = |cmd: &str| -> String {
        let out = std::process::Command::new("debugfs")
            .args(["-R", cmd])
            .arg(&rootfs)
            .output()
            .expect("debugfs must run on the FC node");
        // debugfs prints diagnostics to stderr; the answer is on stdout.
        String::from_utf8_lossy(&out.stdout).into_owned()
    };

    // /init exists at mode 0755 (the injected PID-1 init must be executable).
    let stat_init = debugfs("stat /init");
    assert!(
        stat_init.contains("Mode:  0755") || stat_init.contains("0100755"),
        "/init must be present at mode 0755; debugfs stat /init:\n{stat_init}"
    );

    // The entrypoint binary survived the gzip-layer unpack at mode 0755.
    let stat_server = debugfs("stat /bin/server");
    assert!(
        stat_server.contains("Mode:  0755") || stat_server.contains("0100755"),
        "/bin/server entrypoint must survive at mode 0755; debugfs stat /bin/server:\n{stat_server}"
    );

    // 3. The OCI whiteout was applied: /etc/drop.me from the lower layer is GONE.
    let ls_etc = debugfs("ls -l /etc");
    assert!(
        !ls_etc.contains("drop.me"),
        "the upper layer's .wh.drop.me whiteout must have removed /etc/drop.me; \
         debugfs ls -l /etc:\n{ls_etc}"
    );
    // ...and the whiteout marker itself never leaked into the rootfs.
    assert!(
        !ls_etc.contains(".wh."),
        "no .wh. whiteout marker may survive into the rootfs; debugfs ls -l /etc:\n{ls_etc}"
    );
}

/// `ext4_geometry`: a small app whose content is far under the RAM hint keeps
/// the hint as the floor, and gets the minimum inode table.
#[test]
fn ext4_geometry_small_app_floored_by_hint() {
    let (mib, inodes) = ext4_geometry(5 * 1024 * 1024, 200, 2048);
    assert_eq!(
        mib, 2048,
        "tiny content must not shrink below the caller hint"
    );
    assert_eq!(inodes, 262_144, "inode floor protects small-file images");
}

/// A large dind-class rootfs (content well above the RAM hint, tens of
/// thousands of files) must GROW the image past the hint AND expand the inode
/// table — this is the exact case that made `mkfs.ext4 -d` fail intermittently.
#[test]
fn ext4_geometry_large_rootfs_grows_size_and_inodes() {
    // 2000 MiB content, 200k files, 2048 MiB hint.
    let (mib, inodes) = ext4_geometry(2000 * 1024 * 1024, 200_000, 2048);
    assert_eq!(
        mib,
        2000 * 3 / 2 + 512,
        "size = 1.5x content + 512 MiB slack"
    );
    assert!(mib > 2048, "must exceed the RAM hint for a large rootfs");
    assert_eq!(
        inodes, 400_000,
        "inodes = 2x the file count when above the floor"
    );
}

/// Saturating arithmetic must not panic on absurd inputs.
#[test]
fn ext4_geometry_saturates() {
    let (mib, inodes) = ext4_geometry(u64::MAX, u64::MAX, 1);
    assert!(mib >= 512 && inodes >= 262_144);
}

/// `measure_tree` sums file bytes and counts every entry (dirs + files).
#[tokio::test]
async fn measure_tree_sums_bytes_and_counts_entries() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir(root.join("sub")).unwrap();
    std::fs::write(root.join("a.txt"), b"hello").unwrap(); // 5 bytes
    std::fs::write(root.join("sub/b.bin"), vec![0u8; 100]).unwrap(); // 100 bytes
    let (bytes, count) = measure_tree(root).await.unwrap();
    assert_eq!(bytes, 105, "byte sum of both files");
    assert_eq!(count, 3, "two files + one subdir");
}

/// THE symlink-mangling regression: an ABSOLUTE symlink target (`/bin/busybox`)
/// must be preserved verbatim by the in-process extractor. busybox tar (which
/// the NixOS/Alpine runner resolves on PATH) strips the leading `/` -> the
/// broken `bin/busybox` -> `/bin/bin/busybox`, which breaks the guest /init.
#[tokio::test]
async fn extract_layer_preserves_absolute_symlink_targets() {
    let tmp = tempfile::tempdir().unwrap();
    let tgz = tmp.path().join("layer.tar.gz");
    {
        let f = std::fs::File::create(&tgz).unwrap();
        let enc = flate2::write::GzEncoder::new(f, flate2::Compression::fast());
        let mut b = tar::Builder::new(enc);
        // bin/busybox regular file
        let data = b"ELF";
        let mut h = tar::Header::new_gnu();
        h.set_path("bin/busybox").unwrap();
        h.set_size(data.len() as u64);
        h.set_mode(0o755);
        h.set_entry_type(tar::EntryType::Regular);
        h.set_cksum();
        b.append(&h, &data[..]).unwrap();
        // bin/sh -> /bin/busybox (ABSOLUTE target)
        let mut hs = tar::Header::new_gnu();
        hs.set_size(0);
        hs.set_mode(0o777);
        hs.set_entry_type(tar::EntryType::Symlink);
        b.append_link(&mut hs, "bin/sh", "/bin/busybox").unwrap();
        b.into_inner().unwrap().finish().unwrap();
    }
    let dest = tmp.path().join("out");
    std::fs::create_dir_all(&dest).unwrap();
    extract_layer_blob(&tgz, &oci_spec::image::MediaType::ImageLayerGzip, &dest).unwrap();
    let target = std::fs::read_link(dest.join("bin/sh")).unwrap();
    assert_eq!(
        target.to_str().unwrap(),
        "/bin/busybox",
        "absolute symlink target must stay verbatim (busybox tar would mangle it to bin/busybox)"
    );
}

/// `read_manifest_digest_from_layout` derives the IMMUTABLE image digest from a
/// pulled OCI layout: it reads `index.json` and returns `manifests[0].digest`
/// (the `sha256:…` of the image manifest blob), matching the same descriptor
/// `read_oci_config_from_layout` resolves. This is the cache key for a TAG ref
/// (no `@<digest>`), where the digest is unknown until the layout is pulled.
#[test]
fn read_manifest_digest_from_layout_returns_index_manifest_digest() {
    let tmp = tempfile::tempdir().unwrap();
    let l0 = make_tar(&[("bin/server", b"elf")]);
    let cfg = serde_json::json!({
        "architecture": super::host_oci_arch(), "os": "linux",
        "config": {"Entrypoint": ["/bin/server"]},
        "rootfs": {"type": "layers", "diff_ids": ["sha256:l0"]}
    });
    let layout = write_min_oci_layout(tmp.path(), &cfg, &[("sha256:l0", &l0)]);

    // The expected digest is exactly the descriptor the index points at — the
    // SAME one the config-read path resolves the manifest blob from.
    let index = oci_spec::image::ImageIndex::from_file(layout.join("index.json")).unwrap();
    let expected = index.manifests().first().unwrap().digest().to_string();

    let digest = super::read_manifest_digest_from_layout(&layout).expect("derive digest");
    assert_eq!(
        digest, expected,
        "must return index.json manifests[0].digest verbatim (sha256:…)"
    );
    assert!(
        digest.starts_with("sha256:"),
        "derived digest must be a full algo-prefixed digest; got {digest:?}"
    );
}

/// A layout with an empty `index.json` (no manifest descriptor) errors clearly
/// rather than fabricating a bogus cache key.
#[test]
fn read_manifest_digest_from_layout_errors_on_empty_index() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(
        tmp.path().join("index.json"),
        br#"{"schemaVersion":2,"manifests":[]}"#,
    )
    .unwrap();
    let err = super::read_manifest_digest_from_layout(tmp.path()).unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("manifest"),
        "must name the missing manifest; got: {err}"
    );
}

/// THE #1 e2e blocker: a TAG `registry_ref` (NO `@<digest>`) must NOT bail.
/// `run_firecracker_build` must pull the layout first, DERIVE the immutable
/// digest from the layout's `index.json`, and then convert with THAT digest as
/// the cache key — the produced rootfs must land at the digest-keyed path
/// (fc-3 invariant), not at any tag-derived path.
///
/// The registry is a local mock HTTP server (the resumable `crate::oci_pull`
/// puller talks plain HTTP), the ref host points at it, the `oras resolve` is a
/// no-op failure so the digest is DERIVED from the pulled layout, the real host
/// `tar` does the unpack, and only `mkfs.ext4` is faked. The final VM boot has no
/// Firecracker/KVM here so it errors, but conversion (incl. the digest-keyed
/// `mkfs.ext4` argv) runs BEFORE the boot and is asserted on the recorded argv.
#[tokio::test]
async fn run_fc_build_derives_digest_from_layout_for_tag_ref() {
    use sha2::{Digest as _, Sha256};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let tmp = tempfile::tempdir().unwrap();
    let uuid = "uuid-tagref";
    let hexf = |b: &[u8]| format!("{:x}", Sha256::digest(b));

    // The image the mock registry serves: config blob + one layer blob.
    let l0 = make_tar(&[("bin/server", b"elf")]);
    let cfg = serde_json::json!({
        "architecture": super::host_oci_arch(), "os": "linux",
        "config": {"Entrypoint": ["/app/server"], "Env": ["PATH=/usr/bin"], "WorkingDir": "/app"},
        "rootfs": {"type": "layers", "diff_ids": ["sha256:l0"]}
    });
    let config_bytes = serde_json::to_vec(&cfg).unwrap();
    let (config_hex, layer_hex) = (hexf(&config_bytes), hexf(&l0));
    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                   "digest": format!("sha256:{config_hex}"), "size": config_bytes.len()},
        "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar",
                    "digest": format!("sha256:{layer_hex}"), "size": l0.len()}],
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_hex = hexf(&manifest_bytes);

    let server = MockServer::start().await;
    for (p, body) in [
        ("/v2/acme/vm/manifests/latest".to_owned(), manifest_bytes.clone()),
        (format!("/v2/acme/vm/blobs/sha256:{config_hex}"), config_bytes.clone()),
        (format!("/v2/acme/vm/blobs/sha256:{layer_hex}"), l0.clone()),
    ] {
        Mock::given(method("GET"))
            .and(path(p))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
            .mount(&server)
            .await;
    }

    // A TAG ref (no `@sha256:…`) whose host is the mock registry.
    let host = server.uri().strip_prefix("http://").unwrap().to_owned();
    let fetched = fc_fetched_ref(&format!("{host}/acme/vm:latest"));
    let fc = <crate::config::FcConfig as clap::Parser>::parse_from(["fc"]);

    // The immutable digest the build MUST derive from the layout + key the cache by.
    let expected_digest = format!("sha256:{manifest_hex}");
    let target = super::cached_rootfs_path(tmp.path(), uuid, &expected_digest, &no_env_hash());

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
                // `oras resolve` for the tag returns NO digest, so the build
                // DERIVES the immutable digest from the freshly-pulled layout.
                Some("oras") => (false, Vec::new()),
                _ => (real)(argv).await, // real host tar for the unpack
            }
        })
    });

    // Tag ref must NOT bail; the boot at the end errors (no KVM) — irrelevant to
    // the digest-derivation assertion below.
    let _ =
        super::run_firecracker_build(uuid, &fetched, &fc, tmp.path(), &runner, false, None, None)
            .await;

    let recorded = calls.lock().unwrap().clone();
    // The conversion ran and wrote the rootfs to the DIGEST-keyed path.
    let mkfs = recorded
        .iter()
        .find(|c| c.first().map(String::as_str) == Some("mkfs.ext4"))
        .expect("tag ref must reach conversion (mkfs.ext4), not bail");
    let sanitized = expected_digest.replace(':', "-");
    assert!(
        mkfs.iter().any(|a| a.contains(&sanitized)),
        "rootfs must be keyed by the DERIVED immutable digest {sanitized:?} (fc-3); got {mkfs:?}"
    );
    assert!(
        target.is_file(),
        "the digest-keyed rootfs must be produced at {target:?}"
    );
}

#[tokio::test]
async fn fresh_tag_pull_dir_clears_a_stale_pull_layout() {
    // The TAG-ref pull dir is REUSED across deploys. `oras copy --to-oci-layout`
    // ACCUMULATES manifests in index.json, and `read_manifest_digest_from_layout`
    // reads manifests[0] (the OLDEST) — so without clearing, every redeploy
    // resolved to the FIRST-ever digest and `rootfs_is_cached` served the stale
    // rootfs (the app stayed on its original version forever). The pull dir must
    // be cleared before each pull so manifests[0] is the tag's CURRENT image.
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path();
    let uuid = "0191e7c2-0000-7000-8000-000000000001";

    // Simulate a leftover OCI layout from a previous deploy.
    let pull = data_dir
        .join("apps")
        .join(uuid)
        .join("fc")
        .join(".pull")
        .join("oci");
    std::fs::create_dir_all(&pull).unwrap();
    std::fs::write(pull.join("index.json"), b"{\"stale\":true}").unwrap();
    assert!(pull.join("index.json").exists());

    let work = super::fresh_tag_pull_dir(data_dir, uuid).await.unwrap();

    assert_eq!(
        work,
        data_dir.join("apps").join(uuid).join("fc").join(".pull")
    );
    assert!(
        !work.join("oci").join("index.json").exists(),
        "a stale .pull layout must be cleared before the next tag pull"
    );
}

// ── resumable image pull (survive a relay EOF on a large blob) ───────────────

/// A flaky relay that EOFs `oras copy` a few times must NOT fail the whole
/// #68: the GLOBAL digest-shared rootfs cache must NOT serve an env-baked rootfs
/// to a different uuid. A rootfs published globally for digest D (carrying
/// uuid-A's baked env — its git cap / secrets) must NOT be linked into a
/// NOT-globally-cacheable uuid (whose own rootfs bakes ITS env) — else that uuid
/// inherits uuid-A's env (the dev-cap mismatch + secrets leak). An env-FREE
/// (globally-cacheable) lookup still hits the global cache.
#[tokio::test]
async fn global_cache_skipped_for_env_baked_rootfs() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path();
    let digest = "sha256:deadbeefcafef00d";

    // uuid-A published an env-baked rootfs to the global cache.
    let built = tmp.path().join("uuidA.ext4");
    std::fs::write(&built, b"rootfs-with-uuidA-env").unwrap();
    super::publish_rootfs_to_global(data, digest, &built).await;
    assert!(
        super::global_rootfs_is_cached(data, digest),
        "global publish must land"
    );

    // NOT globally cacheable (this uuid bakes its OWN env) → must MISS the global.
    assert!(
        super::lookup_cached_rootfs(data, "uuid-B", digest, false, &no_env_hash())
            .await
            .is_none(),
        "an env-baked rootfs must not be served from the global cache"
    );
    // Env-free (globally cacheable) → may use the global cache.
    assert!(
        super::lookup_cached_rootfs(data, "uuid-C", digest, true, &no_env_hash())
            .await
            .is_some(),
        "an env-free rootfs may use the global cache"
    );
}

/// `wipe_oci_layout` is idempotent on a missing dir and removes an existing one.
#[tokio::test]
async fn wipe_oci_layout_tolerates_missing_and_removes_existing() {
    let tmp = tempfile::tempdir().unwrap();
    let layout = tmp.path().join("oci");
    // Missing dir → Ok (idempotent).
    super::wipe_oci_layout(&layout).await.unwrap();
    // Existing dir with content → removed.
    std::fs::create_dir_all(layout.join("blobs")).unwrap();
    std::fs::write(layout.join("index.json"), b"{}").unwrap();
    super::wipe_oci_layout(&layout).await.unwrap();
    assert!(!layout.exists(), "wipe removes the layout dir");
}

// ---- §12 S1 cap-file writer (Task 9) ----------------------------------------

#[test]
fn cap_files_init_is_empty_for_regular_apps() {
    assert_eq!(super::render_cap_files_init(&[]), "");
}

#[test]
fn cap_files_init_writes_0600_broker_owned_files() {
    let files = vec![("app.url".to_owned(), "http://10.0.0.1:9000/git/abc".to_owned())];
    let rendered = super::render_cap_files_init(&files);
    assert!(rendered.contains("mkdir -p /run/tabbify/caps"));
    assert!(rendered.contains("umask 077"));
    assert!(rendered.contains("/run/tabbify/caps/app.url"));
    assert!(rendered.contains("chmod 0600 '/run/tabbify/caps/app.url'"));
    assert!(rendered.contains(&format!("chown -R {}", super::BROKER_UID)));
    // The cap value is single-quoted, not `export`ed (never an env var).
    assert!(rendered.contains("'http://10.0.0.1:9000/git/abc'"));
    assert!(!rendered.contains("export "));
}

#[test]
fn safe_cap_name_rejects_traversal() {
    assert!(super::safe_cap_name("app.url"));
    assert!(super::safe_cap_name("forge-admin.token"));
    // §12 S6: the authorized-keys cap rides the same off-env cap-file channel.
    assert!(super::safe_cap_name("authkeys.cap"));
    assert!(!super::safe_cap_name("../escape"));
    assert!(!super::safe_cap_name("a/b"));
    assert!(!super::safe_cap_name(""));
    assert!(!super::safe_cap_name(".."));
}

/// §12 S6: the authkeys cap (the :8732 bearer token) is materialized via the
/// SAME 0600 broker-owned cap-file writer — so the AGENT uid cannot read it
/// (only the broker, which validates incoming :8732 requests, can). The token
/// is single-quoted and NEVER `export`ed into agent env.
#[test]
fn cap_files_init_writes_authkeys_cap_0600_broker_owned() {
    let files = vec![("authkeys.cap".to_owned(), "deadbeefCAPTOKEN".to_owned())];
    let rendered = super::render_cap_files_init(&files);
    assert!(rendered.contains("/run/tabbify/caps/authkeys.cap"));
    assert!(rendered.contains("chmod 0600 '/run/tabbify/caps/authkeys.cap'"));
    assert!(rendered.contains(&format!("chown -R {}", super::BROKER_UID)));
    assert!(rendered.contains("'deadbeefCAPTOKEN'"));
    assert!(!rendered.contains("export "));
}

/// `render_init` with cap-files emits the 0600 broker-owned writer lines AFTER
/// the env exports and BEFORE the workdir/exec — proving the §12 S1 channel is
/// wired into the actual PID-1 init (not just the standalone renderer).
#[test]
fn render_init_includes_cap_files_before_exec() {
    let exec = OciExec {
        entrypoint: vec!["/app".to_owned()],
        cmd: vec![],
        env: vec!["PATH=/usr/bin".to_owned()],
        workdir: "/srv".to_owned(),
    };
    let caps = vec![("app.url".to_owned(), "http://h/git/cap".to_owned())];
    let init = render_init(&Entrypoint::Exec(exec), &caps, None).unwrap();
    let cap_pos = init
        .find("/run/tabbify/caps/app.url")
        .expect("cap-file line must be present");
    let exec_pos = init.find("exec '/app'").expect("exec line must be present");
    assert!(cap_pos < exec_pos, "cap-files must be written BEFORE exec");
    // The cap content is NOT an env export.
    assert!(!init.contains("export TABBIFY_CAP_FILES"));
}

/// Task 4/6: when `data_mount` is `Some(path)`, `render_init` emits a robust
/// data-disk mount AFTER the /dev mount and BEFORE the entrypoint exec:
/// a `mkdir -p <path>`, a wait loop (`[ ! -b /dev/vdb ]`) that gives the block
/// device up to ~5s to appear (early `/init` can outrun devtmpfs probing), then
/// `mount -t ext4 /dev/vdb <path>` with a bare-mount fs-type-auto-detect
/// fallback and, on failure, a WARN to stderr instead of a silent `|| true`
/// (no error swallowing — the generous-logging rule).
///
/// The path is POSIX single-quoted (Task 6) so a space / glob / `$` cannot
/// mis-tokenize the line. When `None`, none of these lines is emitted and the
/// output is byte-identical to the no-data-mount path (cache-key safety).
#[test]
fn render_init_mounts_data_disk_when_some_skips_when_none() {
    let exec = OciExec {
        entrypoint: vec!["/app/forge".to_owned()],
        cmd: vec![],
        env: vec![],
        workdir: "/".to_owned(),
    };

    // Some(path): both mount lines must be present, ordered correctly. The path
    // is shell single-quoted in the emitted script (Task 6).
    let mount_path = "/var/lib/tabbify-forge";
    let quoted = format!("'{mount_path}'");
    let init_some = render_init(
        &Entrypoint::Exec(exec.clone()),
        &[],
        Some(mount_path),
    )
    .unwrap();
    assert!(
        init_some.contains(&format!("mkdir -p {quoted}")),
        "must emit mkdir -p for the (quoted) data mount path; got:\n{init_some}"
    );
    // Recovery: when the /dev fallback lands on an empty tmpfs (a second
    // devtmpfs mount is refused), the kernel-created /dev/vdb node is dropped;
    // recreate it from sysfs (authoritative major:minor) before waiting/mounting.
    assert!(
        init_some.contains("[ -r /sys/block/vdb/dev ]")
            && init_some.contains("mknod /dev/vdb b $(cat /sys/block/vdb/dev | tr ':' ' ')"),
        "must recreate /dev/vdb from sysfs when the node is missing; got:\n{init_some}"
    );
    // Wait loop: give /dev/vdb up to ~5s to appear before mounting.
    assert!(
        init_some.contains("[ ! -b /dev/vdb ]"),
        "must emit the block-device wait loop; got:\n{init_some}"
    );
    // Explicit ext4 mount (primary), with the path single-quoted.
    assert!(
        init_some.contains(&format!("mount -t ext4 /dev/vdb {quoted}")),
        "must emit mount -t ext4 /dev/vdb <quoted-path>; got:\n{init_some}"
    );
    // On failure, WARN to stderr — no silent swallowing.
    assert!(
        init_some.contains("tabbify-init: WARN data disk /dev/vdb failed to mount at"),
        "must emit a WARN log on mount failure; got:\n{init_some}"
    );
    // The old silent-swallow form must be gone.
    assert!(
        !init_some.contains(&format!("mount /dev/vdb {quoted} 2>/dev/null || true")),
        "must NOT silently swallow the mount error with `|| true`; got:\n{init_some}"
    );
    // The data mount must appear AFTER the /dev mount.
    let dev_mount_pos = init_some
        .find("mount -t devtmpfs")
        .expect("devtmpfs mount must be present");
    let data_mkdir_pos = init_some
        .find(&format!("mkdir -p {quoted}"))
        .expect("data mkdir must be present");
    assert!(
        dev_mount_pos < data_mkdir_pos,
        "data mkdir must come AFTER the /dev mount; got:\n{init_some}"
    );
    // The data mount must appear BEFORE the entrypoint exec.
    let exec_pos = init_some
        .find("exec '/app/forge'")
        .expect("exec line must be present");
    assert!(
        data_mkdir_pos < exec_pos,
        "data mkdir must come BEFORE exec; got:\n{init_some}"
    );

    // None: neither line must appear — byte-identical to the no-data-mount path.
    let init_none = render_init(&Entrypoint::Exec(exec), &[], None).unwrap();
    assert!(
        !init_none.contains("mount /dev/vdb"),
        "None must not emit mount /dev/vdb; got:\n{init_none}"
    );
    assert!(
        !init_none.contains(&format!("mkdir -p {quoted}")),
        "None must not emit mkdir -p for the data path; got:\n{init_none}"
    );
}

/// Task 6: a `data_mount` carrying a shell metachar (space / `$`) is POSIX
/// single-quoted so the re-tokenizing `/bin/sh` treats it as ONE literal path —
/// it can never word-split or `$`-expand into a wrong mount target.
#[test]
fn render_init_shell_quotes_a_metachar_data_mount() {
    let exec = OciExec {
        entrypoint: vec!["/app".to_owned()],
        cmd: vec![],
        env: vec![],
        workdir: "/".to_owned(),
    };
    let weird = "/var/lib/tab bify$HOME";
    let init = render_init(&Entrypoint::Exec(exec), &[], Some(weird)).unwrap();
    // Single-quoted verbatim: `mount -t ext4 /dev/vdb '/var/lib/tab bify$HOME' ...`.
    assert!(
        init.contains(&format!("mount -t ext4 /dev/vdb '{weird}'")),
        "metachar mount path must be single-quoted verbatim; got:\n{init}"
    );
    // The RAW (unquoted) form must NOT appear as a bare mount arg.
    assert!(
        !init.contains(&format!("mount -t ext4 /dev/vdb {weird} ")),
        "must never emit the unquoted metachar path; got:\n{init}"
    );
}

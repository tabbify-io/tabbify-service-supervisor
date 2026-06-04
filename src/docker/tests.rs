//! Tests for [`super`] — the docker BUILD/PUSH seam: the daemon gate, image
//! naming, the registry `push`/`tag` argv builders + the `push_image` mirror,
//! the `rmi` purge argv, and the build-runner docker path end-to-end (build +
//! skopeo push diagnostic propagation).
#![allow(clippy::unwrap_used)]

use super::protocol::{docker_available_with, rmi_args, tag_args, versioned_image_tag};

#[test]
fn docker_gate_reflects_the_injected_probe() {
    assert!(docker_available_with(|| true));
    assert!(!docker_available_with(|| false));
}

// ---- versioned image tag (purge target) ----------------------------------

/// Content-stable image tag keyed by uuid + version: `tbf-img-<uuid>-v<N>`.
/// Two builds of the same uuid at different versions must yield different tags.
#[test]
fn versioned_image_tag_encodes_uuid_and_version() {
    let uuid = "0191e7c2-1111-7222-8333-444455556666";
    assert_eq!(
        versioned_image_tag(uuid, 3),
        "tbf-img-0191e7c2-1111-7222-8333-444455556666-v3"
    );
}

#[test]
fn versioned_image_tag_differs_across_versions() {
    let uuid = "abc123";
    assert_ne!(versioned_image_tag(uuid, 1), versioned_image_tag(uuid, 2));
}

#[test]
fn versioned_image_tag_sanitizes_uuid() {
    // Upper-case and slashes in the id must be lower-cased / replaced with '-'.
    assert_eq!(versioned_image_tag("My/App", 1), "tbf-img-my-app-v1");
}

// ---- purge targets versioned image tag -----------------------------------

/// `purge_image` must target the `tbf-img-<uuid>-v<N>` versioned tag (not
/// the old generic tag). Verified by composing `rmi_args(versioned_image_tag)`
/// and confirming the resulting argv matches the purge contract.
#[test]
fn purge_rmi_targets_versioned_image_tag() {
    let uuid = "0191e7c2-1111-7222-8333-444455556666";
    let version = 7_u64;
    let tag = versioned_image_tag(uuid, version);
    // purge_image calls rmi_args(&tag) — verify the args are correct.
    assert_eq!(
        rmi_args(&tag),
        vec![
            "rmi".to_owned(),
            "-f".to_owned(),
            "tbf-img-0191e7c2-1111-7222-8333-444455556666-v7".to_owned(),
        ],
        "purge must pass the versioned image tag to docker rmi"
    );
}

#[test]
fn rmi_args_force_remove_image_by_tag() {
    assert_eq!(
        rmi_args("tabbify-app-x"),
        vec!["rmi", "-f", "tabbify-app-x"]
    );
}

// ---- push_args / tag_args (registry argv) -------------------------------

/// `push_args` must produce `["push", <ref>]`.
#[test]
fn push_args_returns_correct_argv() {
    assert_eq!(
        super::protocol::push_args("[fd5a::1]:5000/acme/app:sha"),
        vec!["push", "[fd5a::1]:5000/acme/app:sha"]
    );
}

#[test]
fn tag_args_returns_correct_argv() {
    assert_eq!(
        tag_args("[fd5a::1]:5000/acme/app:abc", "tbf-img-uuid-v3"),
        vec!["tag", "[fd5a::1]:5000/acme/app:abc", "tbf-img-uuid-v3"]
    );
}

// ---- push_image seam (injected runner) -----------------------------------

/// When the runner succeeds for both tag and push, `push_image` must issue
/// `["tag", <local_tag>, <ref>]` then `["push", <ref>]` in that order and
/// return `true`.
#[tokio::test]
async fn push_image_issues_tag_then_push_on_success() {
    use std::sync::{Arc, Mutex};

    let issued: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
    let issued2 = issued.clone();

    let runner: super::CommandRunner = Arc::new(move |args: Vec<String>| {
        issued2.lock().unwrap().push(args);
        Box::pin(async { Ok(()) })
    });

    let result = super::push_image_for_test(
        "docker",
        "tbf-img-uuid-v1",
        "[fd5a::1]:5000/acme/app:sha",
        &runner,
    )
    .await;

    assert!(result.is_ok(), "both tag + push succeed → must return Ok");

    let cmds = issued.lock().unwrap();
    assert_eq!(cmds.len(), 2, "must issue exactly 2 commands (tag + push)");
    assert_eq!(
        cmds[0],
        vec![
            "tag".to_owned(),
            "tbf-img-uuid-v1".to_owned(),
            "[fd5a::1]:5000/acme/app:sha".to_owned(),
        ],
        "first command must be docker tag <local_tag> <ref>"
    );
    assert_eq!(
        cmds[1],
        vec!["push".to_owned(), "[fd5a::1]:5000/acme/app:sha".to_owned()],
        "second command must be docker push <ref>"
    );
}

/// When the runner fails on the tag step, `push_image` must return `Err`
/// and NOT issue the push command.
#[tokio::test]
async fn push_image_returns_err_and_skips_push_on_tag_failure() {
    use std::sync::{Arc, Mutex};

    let call_count: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
    let cc = call_count.clone();

    let runner: super::CommandRunner = Arc::new(move |_args: Vec<String>| {
        *cc.lock().unwrap() += 1;
        Box::pin(async { Err("tag failed".to_owned()) }) // tag fails
    });

    let result = super::push_image_for_test(
        "docker",
        "tbf-img-uuid-v2",
        "[fd5a::1]:5000/acme/app:sha",
        &runner,
    )
    .await;

    assert!(result.is_err(), "tag fails → must return Err");
    assert_eq!(
        *call_count.lock().unwrap(),
        1,
        "must issue only the tag command (push must NOT be called on tag failure)"
    );
}

/// When the push step fails, `push_image` must surface the runner's exact
/// stderr text in the returned `Err` (so the build runner can bail with the
/// real registry diagnostic instead of just the image ref). The tag step
/// succeeds; the push step returns `Err("unauthorized: authentication
/// required")`, and that exact text must appear in the propagated error.
#[tokio::test]
async fn push_image_surfaces_push_stderr_in_err() {
    use crate::runtime::BoxFut;
    use std::sync::Arc;

    let runner: super::CommandRunner = Arc::new(move |args: Vec<String>| {
        // tag succeeds; push fails with a registry auth error.
        let fut: BoxFut<'static, Result<(), String>> =
            if args.first().map(String::as_str) == Some("push") {
                Box::pin(async { Err("unauthorized: authentication required".to_owned()) })
            } else {
                Box::pin(async { Ok(()) })
            };
        fut
    });

    let result = super::push_image_for_test(
        "docker",
        "tbf-img-uuid-v9",
        "[fd5a::1]:5000/acme/app:sha",
        &runner,
    )
    .await;

    let err = result.expect_err("push failure → push_image must return Err");
    assert!(
        err.contains("unauthorized: authentication required"),
        "push_image Err must surface the runner's exact stderr; got: {err}"
    );
}

/// The build runner's docker path must bail with the registry stderr (not just
/// the image ref) when the supervisor-side `skopeo` push fails — confirming the
/// diagnostic survives all the way to the `run_docker_build` caller.
#[tokio::test]
async fn run_docker_build_bails_with_push_stderr() {
    use crate::build_backend::HostDockerBackend;
    use crate::git::GitRun;
    use crate::runner::build::{BuildJob, BuildKind, run_build};
    use crate::runtime::BoxFut;
    use std::sync::Arc;

    let dir = tempfile::tempdir().unwrap();

    // Clone creates the src dir + a Dockerfile so the docker path proceeds.
    // Only the `git init -q <dest>` step carries the dest as its last arg —
    // the later steps end with the repo URL / git ref / `FETCH_HEAD`, and a
    // fake that mistook those for dests used to litter the crate root with
    // `FETCH_HEAD/`, `abc123/`, `https:/…` dirs on every test run.
    let git: GitRun = Arc::new(move |args: Vec<String>, _env| {
        let dest = (args.first().map(String::as_str) == Some("init"))
            .then(|| args.last().cloned())
            .flatten();
        Box::pin(async move {
            if let Some(dest) = dest {
                std::fs::create_dir_all(&dest).ok();
                std::fs::write(format!("{dest}/Dockerfile"), "FROM scratch\n").unwrap();
            }
            Ok(())
        })
    });

    // Build backend succeeds (image built locally).
    let build_runner: super::CommandRunner = Arc::new(|_args| Box::pin(async { Ok(()) }));
    let backend = HostDockerBackend::with_runner("docker".to_owned(), build_runner);

    // Tool runner (argv[0] = binary): the oras registry leg fails with the
    // auth stderr; the skopeo daemon→layout leg succeeds.
    let skopeo_runner: super::CommandRunner = Arc::new(|args: Vec<String>| {
        let fut: BoxFut<'static, Result<(), String>> =
            if args.first().map(String::as_str) == Some("oras") {
                Box::pin(async { Err("unauthorized: authentication required".to_owned()) })
            } else {
                Box::pin(async { Ok(()) })
            };
        fut
    });

    let job = BuildJob {
        repo_url: "https://github.com/acme/app".into(),
        git_ref: "abc123".into(),
        tenant: "acme".into(),
        app_uuid: "u".into(),
        registry_ula: "[fd5a::1]:5000".into(),
        clone_token: None,
        push_token: None,
        build_kind: BuildKind::Docker,
        build_cmd: None,
        artifact_path: None,
    };

    let err = run_build(&job, &backend, &git, &skopeo_runner, "skopeo", "oras", dir.path())
        .await
        .expect_err("push failure → run_build must bail")
        .to_string();

    assert!(
        err.contains("unauthorized: authentication required"),
        "run_build must bail with the registry stderr, not just the ref; got: {err}"
    );
}

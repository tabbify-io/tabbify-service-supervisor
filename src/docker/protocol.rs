//! Pure-helper layer for the docker BUILD/PUSH path in [`super`]: the
//! availability-probe seam, deterministic image naming, the registry `pull` /
//! `push` / `tag` argv builders, and the `rmi` purge argv.
//!
//! Everything in this file is a pure function so it can be unit-tested without
//! invoking a real `docker` binary or daemon.

/// [`super::docker_available`] with an injectable probe — lets tests assert
/// the gate logic without a real Docker daemon.
pub fn docker_available_with(check: impl Fn() -> bool) -> bool {
    check()
}

/// `docker rmi -f <tag>` argv (sans the leading binary): force-remove a built
/// image. Used on PURGE ([`super::purge_image`]) to reclaim disk.
pub fn rmi_args(tag: &str) -> Vec<String> {
    vec!["rmi".to_owned(), "-f".to_owned(), tag.to_owned()]
}

/// Content-stable image tag keyed by uuid + version: `tbf-img-<uuid>-v<N>`.
/// The tag changes when the app version changes, so a new push never reuses a
/// stale image. Used by [`super::purge_image`] to target the right image.
pub fn versioned_image_tag(uuid: &str, version: u64) -> String {
    let sanitized: String = uuid
        .chars()
        .map(|c| {
            let c = c.to_ascii_lowercase();
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-') {
                c
            } else {
                '-'
            }
        })
        .collect();
    format!("tbf-img-{sanitized}-v{version}")
}

/// `docker push <ref>` argv (sans the leading binary): push the locally-tagged
/// image to the mesh OCI registry by its full ref (host:port/name:tag).
/// Used by the build-runner after `docker build` to publish the image into
/// the mesh registry so supervisors on other nodes can pull it.
///
/// Called via [`super::push_image`] → [`crate::build_backend::push_to_registry`]
/// and directly by the P3.4 `run_build` orchestration.
pub fn push_args(reff: &str) -> Vec<String> {
    vec!["push".to_owned(), reff.to_owned()]
}

/// `docker tag <ref> <vtag>` argv (sans the leading binary): alias an image
/// under another local/registry tag. Used by the push mirror to alias a
/// locally-built tag as the mesh registry ref before `docker push`.
pub fn tag_args(reff: &str, vtag: &str) -> Vec<String> {
    vec!["tag".to_owned(), reff.to_owned(), vtag.to_owned()]
}

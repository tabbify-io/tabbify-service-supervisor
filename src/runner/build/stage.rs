//! Failing-STAGE attribution for the one-shot build pipeline.
//!
//! GitHub-Actions-style observability: when a build fails, the platform must be
//! able to say WHICH step failed (`clone` → `manifest` → `build` → `push`) and
//! WHOSE fault it is (the user's repo/config vs the platform), so the node can
//! surface an actionable message to the deploy owner instead of a generic
//! "build failed".
//!
//! ## How the stage travels
//!
//! 1. [`run_build`](super::run_build) attaches a [`StageFailure`] context to each
//!    pipeline step's error, so the failing stage rides the `anyhow` chain.
//! 2. The one-shot builder binary (`--build-spec` mode) extracts it via
//!    [`failure_marker_from`] and prints a [`BuildFailureMarker`] as the LAST
//!    stdout line (stdout is otherwise reserved for the `ArtifactRef` JSON).
//! 3. The supervisor daemon ([`spawn_build_with`]) parses the marker via
//!    [`parse_failure_marker`] and answers `POST /v1/build` with a structured
//!    [`StagedBuildError`] body: `{error, stage, user_fault, log_tail}`.
//! 4. The node classifies the body and returns the actionable reason (+ stage +
//!    bounded log tail) to the deploy owner via `deploy_status`.
//!
//! [`spawn_build_with`]: crate::orchestrator::Orchestrator::spawn_build_with

use serde::{Deserialize, Serialize};

/// Failing-stage names, shared between the builder child and the daemon.
///
/// Distinct from the PROGRESS vocab (`starting`/`pulling`/`building`/…) derived
/// from the live log tail: these name the pipeline STEP that failed.
pub mod stage_names {
    /// `git clone` + commit-SHA resolution (source acquisition).
    pub const CLONE: &str = "clone";
    /// `tabbify.toml` injection + `[build]` resolution (manifest parse).
    pub const MANIFEST: &str = "manifest";
    /// The image build itself (fc-sandboxed buildkit or host docker).
    pub const BUILD: &str = "build";
    /// The push of the built artifact to the mesh registry.
    pub const PUSH: &str = "push";
    /// The stage could not be attributed (no [`super::StageFailure`] in the
    /// chain — e.g. spec-file read/parse failed before the pipeline started).
    pub const UNKNOWN: &str = "unknown";
}

/// A stage marker attached to a build error's `anyhow` context chain.
///
/// `user_fault = true` means the failure is attributable to the USER's repo or
/// config (their `tabbify.toml`, their `Dockerfile`, their source failing to
/// compile) — content the user wrote themselves, safe and NECESSARY to echo
/// back to them. `false` means platform infrastructure (registry push, spec
/// plumbing) whose detail stays server-side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StageFailure {
    /// One of [`stage_names`].
    pub stage: &'static str,
    /// Whether the failure is the user's own repo/config fault.
    pub user_fault: bool,
}

impl StageFailure {
    /// A user-fault stage marker (the user's repo/config caused the failure).
    #[must_use]
    pub fn user(stage: &'static str) -> Self {
        Self {
            stage,
            user_fault: true,
        }
    }

    /// A platform-fault stage marker (infrastructure caused the failure).
    #[must_use]
    pub fn platform(stage: &'static str) -> Self {
        Self {
            stage,
            user_fault: false,
        }
    }
}

/// Attribute a clone failure by CAUSE, not by stage.
///
/// A missing repo, a bad ref or a rejected credential is the caller's own input
/// — echoing it back is safe and is the only way they can fix it. Marking the
/// whole stage platform-fault turned "you typed a repo that does not exist" into
/// "platform error, report this incident id", which sends people to the
/// operator instead of to their own URL. Anything else (network, git plumbing)
/// stays platform: fail-closed, no server detail leaks on an ambiguous error.
#[must_use]
pub fn clone_failure(error_chain: &str) -> StageFailure {
    const USER_SIGNALS: [&str; 7] = [
        "repository not found",
        "does not appear to be a git repository",
        "could not read username",
        "authentication failed",
        "permission denied",
        "couldn't find remote ref",
        "pathspec",
    ];
    let lowered = error_chain.to_ascii_lowercase();
    // git renders the same cause two ways: bare ("remote: Repository not found")
    // and with the URL spliced in ("fatal: repository 'https://…' not found"),
    // so a single substring cannot cover both.
    let repo_missing = lowered.contains("repository") && lowered.contains("not found");
    if repo_missing || USER_SIGNALS.iter().any(|signal| lowered.contains(signal)) {
        StageFailure::user(stage_names::CLONE)
    } else {
        StageFailure::platform(stage_names::CLONE)
    }
}

#[cfg(test)]
mod clone_attribution_tests {
    use super::*;

    /// A repo the caller typed wrong is THEIR input: it must come back as
    /// user-fault, not as "platform error, report this incident id".
    #[test]
    fn caller_input_failures_are_user_fault() {
        for chain in [
            "'clone' stage: git fetch failed (128): remote: Repository not found.",
            "fatal: repository 'https://github.com/x/y/' not found",
            "does not appear to be a git repository",
            "fatal: Authentication failed for 'https://github.com/x/y'",
            "remote: Permission denied to user",
            "fatal: couldn't find remote ref refs/heads/nope",
            "error: pathspec 'nope' did not match any file(s) known to git",
        ] {
            assert!(
                clone_failure(chain).user_fault,
                "{chain:?} must be attributed to the user"
            );
        }
    }

    /// Anything ambiguous stays platform — fail-closed, so server detail is
    /// never echoed to a caller who cannot act on it anyway.
    #[test]
    fn infrastructure_failures_stay_platform() {
        for chain in [
            "'clone' stage: git fetch failed (128): unable to access: Could not resolve host",
            "git binary not found",
            "connection timed out",
            "disk quota exceeded",
        ] {
            assert!(
                !clone_failure(chain).user_fault,
                "{chain:?} must stay platform-attributed"
            );
        }
    }

    /// Attribution is case-insensitive: git capitalises inconsistently across
    /// versions and remotes.
    #[test]
    fn signal_matching_ignores_case() {
        assert!(clone_failure("REMOTE: REPOSITORY NOT FOUND").user_fault);
    }

    /// The stage name is unchanged by attribution.
    #[test]
    fn stage_stays_clone_either_way() {
        assert_eq!(
            clone_failure("Repository not found").stage,
            stage_names::CLONE
        );
        assert_eq!(
            clone_failure("connection timed out").stage,
            stage_names::CLONE
        );
    }
}

impl std::fmt::Display for StageFailure {
    /// Reads naturally inside the `{:#}` anyhow chain:
    /// `build failed: 'manifest' stage: parse tabbify.toml at …: missing field`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "'{}' stage", self.stage)
    }
}

impl std::error::Error for StageFailure {}

/// The machine-readable failure marker the one-shot builder prints as the LAST
/// stdout line when a build fails.
///
/// Wrapped in a single-key envelope (`{"build_failure": {…}}`) so the daemon's
/// stdout parser can never confuse it with a (success-path) `ArtifactRef` line.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BuildFailureMarker {
    /// One of [`stage_names`].
    pub stage: String,
    /// Whether the failure is the user's own repo/config fault.
    pub user_fault: bool,
    /// Human-readable error chain (`{:#}` rendering).
    pub error: String,
}

/// Single-key JSON envelope for [`BuildFailureMarker`] on the stdout wire.
#[derive(Debug, Serialize, Deserialize)]
struct MarkerEnvelope {
    build_failure: BuildFailureMarker,
}

impl BuildFailureMarker {
    /// Serialize into the single-line stdout envelope.
    #[must_use]
    pub fn to_stdout_line(&self) -> String {
        serde_json::to_string(&MarkerEnvelope {
            build_failure: self.clone(),
        })
        .unwrap_or_else(|_| String::from(r#"{"build_failure":{"stage":"unknown","user_fault":false,"error":"marker serialization failed"}}"#))
    }
}

/// Derive the failure marker from a build error: the FIRST [`StageFailure`]
/// attached to the `anyhow` context chain attributes the stage (anyhow's
/// `downcast_ref` walks context values, which `chain()` items do not); none
/// found ⇒ `unknown`/platform.
#[must_use]
pub fn failure_marker_from(e: &anyhow::Error) -> BuildFailureMarker {
    let staged = e.downcast_ref::<StageFailure>();
    BuildFailureMarker {
        stage: staged.map_or(stage_names::UNKNOWN, |s| s.stage).to_owned(),
        user_fault: staged.is_some_and(|s| s.user_fault),
        error: format!("{e:#}"),
    }
}

/// Parse a [`BuildFailureMarker`] from the builder child's captured stdout: the
/// last non-empty line, when it is a marker envelope. `None` for pre-marker
/// builders (or when the child died before printing one) — the caller falls
/// back to stderr-only attribution.
#[must_use]
pub fn parse_failure_marker(stdout: &[u8]) -> Option<BuildFailureMarker> {
    let text = String::from_utf8_lossy(stdout);
    let last = text.lines().rev().find(|l| !l.trim().is_empty())?;
    serde_json::from_str::<MarkerEnvelope>(last.trim())
        .ok()
        .map(|env| env.build_failure)
}

/// A structured build failure the supervisor daemon returns from
/// `POST /v1/build`: the failing stage, the fault class, the human-readable
/// message, and a bounded tail of the build log.
///
/// Typed (instead of a formatted string) so the API handler can render the
/// structured JSON body `{error, stage, user_fault, log_tail}` the node's
/// classifier consumes; `Display` keeps a self-contained one-liner for legacy
/// nodes that treat the body as opaque text.
#[derive(Debug)]
pub struct StagedBuildError {
    /// One of [`stage_names`].
    pub stage: String,
    /// Whether the failure is the user's own repo/config fault.
    pub user_fault: bool,
    /// Human-readable error (the builder child's error chain or stderr trim).
    pub message: String,
    /// Bounded tail of the captured build output (stdout + stderr) — the last
    /// lines the user needs to see WHY their build failed.
    pub log_tail: String,
}

impl std::fmt::Display for StagedBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "build failed at the '{}' stage: {}",
            self.stage, self.message
        )
    }
}

impl std::error::Error for StagedBuildError {}

/// Bounded tail of raw process output: the last `max_lines` lines, additionally
/// capped at `max_bytes` (a single enormous line cannot flood a response body).
/// Lossy-UTF8 so binary noise in a build log cannot poison the tail.
#[must_use]
pub fn tail_lines(bytes: &[u8], max_lines: usize, max_bytes: usize) -> String {
    let text = String::from_utf8_lossy(bytes);
    let lines: Vec<&str> = text.lines().collect();
    let start = lines.len().saturating_sub(max_lines);
    let tail = lines[start..].join("\n");
    if tail.len() <= max_bytes {
        return tail;
    }
    // Keep the END of the tail (the most recent output is the diagnostic).
    let mut cut = tail.len() - max_bytes;
    while cut < tail.len() && !tail.is_char_boundary(cut) {
        cut += 1;
    }
    format!("…[truncated {cut} bytes]…{}", &tail[cut..])
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use anyhow::Context as _;

    use super::*;

    /// The stage marker survives an anyhow context chain and is extracted by
    /// `failure_marker_from` with the correct stage + fault class.
    #[test]
    fn marker_from_staged_chain() {
        let base: anyhow::Result<()> = Err(anyhow::anyhow!(
            "TOML parse error at line 1: missing field `build`"
        ));
        let err = base
            .context("parse tabbify.toml at /work/src/tabbify.toml")
            .context(StageFailure::user(stage_names::MANIFEST))
            .unwrap_err();

        let marker = failure_marker_from(&err);
        assert_eq!(marker.stage, "manifest");
        assert!(marker.user_fault);
        assert!(
            marker.error.contains("missing field `build`"),
            "marker error must carry the actionable cause: {}",
            marker.error
        );
    }

    /// No StageFailure in the chain ⇒ `unknown` + platform fault (fail-closed:
    /// never claim user fault without attribution).
    #[test]
    fn marker_from_unstaged_chain_is_unknown_platform() {
        let err = anyhow::anyhow!("read build spec /tmp/spec.json: not found");
        let marker = failure_marker_from(&err);
        assert_eq!(marker.stage, "unknown");
        assert!(!marker.user_fault);
    }

    /// The stdout envelope round-trips: `to_stdout_line` → `parse_failure_marker`.
    #[test]
    fn marker_stdout_round_trip() {
        let marker = BuildFailureMarker {
            stage: "build".to_owned(),
            user_fault: true,
            error: "docker build failed: exit 1".to_owned(),
        };
        let stdout = format!("some log line\nanother\n{}\n", marker.to_stdout_line());
        let parsed = parse_failure_marker(stdout.as_bytes()).expect("marker parses");
        assert_eq!(parsed, marker);
    }

    /// A pre-marker stdout (log noise only, or an ArtifactRef success line) does
    /// NOT parse as a failure marker.
    #[test]
    fn parse_rejects_non_marker_lines() {
        assert!(parse_failure_marker(b"just logs\nno marker here\n").is_none());
        assert!(parse_failure_marker(b"").is_none());
        // A success ArtifactRef line must never be mistaken for a failure marker.
        assert!(
            parse_failure_marker(br#"{"reff":"[fd5a::1]:5000/a/b:sha","digest":null}"#).is_none()
        );
    }

    /// `tail_lines` keeps only the last N lines and respects the byte cap by
    /// truncating from the FRONT (newest output is the diagnostic).
    #[test]
    fn tail_lines_bounds_lines_and_bytes() {
        let out = b"line1\nline2\nline3\nline4\n";
        assert_eq!(tail_lines(out, 2, 1000), "line3\nline4");

        let long = "x".repeat(100);
        let bounded = tail_lines(long.as_bytes(), 10, 20);
        assert!(
            bounded.ends_with(&"x".repeat(20)),
            "keeps the end: {bounded}"
        );
        assert!(
            bounded.contains("truncated"),
            "notes the truncation: {bounded}"
        );
    }

    /// `StagedBuildError`'s Display is a self-contained one-liner (legacy nodes
    /// treat the body as opaque text — it must still be actionable).
    #[test]
    fn staged_error_display_names_stage_and_cause() {
        let e = StagedBuildError {
            stage: "manifest".to_owned(),
            user_fault: true,
            message: "parse tabbify.toml: missing field `build`".to_owned(),
            log_tail: String::new(),
        };
        let s = e.to_string();
        assert!(s.contains("'manifest' stage"), "names the stage: {s}");
        assert!(
            s.contains("missing field `build`"),
            "carries the cause: {s}"
        );
    }
}

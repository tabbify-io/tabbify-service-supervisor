//! Non-Linux stub: builds, never boots a VM.

#![cfg(not(target_os = "linux"))]

use std::path::Path;

use anyhow::{Result, bail};
use bytes::Bytes;
use http::{Request, Response};

use super::FcConfig;
use super::port_plan::{PortPlan, resolve_port_plan};
use crate::manifest::Runtime;
use crate::runtime::{AppRuntime, BoxFut, BoxRespFut, RuntimeHealth};

/// Non-Linux stub. Firecracker needs Linux + `/dev/kvm`, so on macOS the
/// supervisor still builds + serves WASM, but any attempt to host a
/// firecracker app fails loudly here.
pub struct FirecrackerRuntime;

impl FirecrackerRuntime {
    /// Always `Err` on non-Linux hosts (no KVM, no tap networking).
    ///
    /// # Errors
    /// Always — firecracker is Linux + `/dev/kvm` only.
    #[allow(clippy::unused_async)]
    pub async fn launch(
        _rootfs: &Path,
        _rt: &Runtime,
        _cfg: &FcConfig,
        _data_disk: Option<&Path>,
    ) -> Result<Self> {
        bail!("firecracker runtime requires Linux + /dev/kvm (host is not Linux)")
    }

    /// [`Self::launch`] with per-uuid pidfile reconciliation. Always `Err`
    /// on non-Linux hosts — the stub mirrors the Linux API surface.
    ///
    /// # Errors
    /// Always — firecracker is Linux + `/dev/kvm` only.
    #[allow(clippy::unused_async, clippy::too_many_arguments)]
    pub async fn launch_with_uuid(
        _rootfs: &Path,
        rt: &Runtime,
        cfg: &FcConfig,
        _uuid: &str,
        _reff: &str,
        _data_dir: &std::path::Path,
        _is_swap: bool,
        _egress_allow: Option<&[String]>,
        is_workspace: bool,
        _env_hash: &str,
        image_exposed_ports: &[u16],
        _snapshot_ref: &str,
        _cap_files: &[(String, String)],
    ) -> Result<Self> {
        // Never boots a VM off Linux, but resolves the port plan through the SAME
        // helper the Linux path uses, so the port signature/logic stays aligned
        // across platforms (the helper compiles on macOS).
        let decision = resolve_port_plan(is_workspace, rt, image_exposed_ports, None, cfg);
        let target = match decision.plan {
            PortPlan::Fixed(p) => format!("guest port {p}"),
            PortPlan::Probe(ports) => format!("one of exposed ports {ports:?}"),
        };
        bail!(
            "firecracker runtime requires Linux + /dev/kvm (host is not Linux; \
             would target {target}, from {})",
            decision.source.describe()
        )
    }
}

impl AppRuntime for FirecrackerRuntime {
    fn handle<'a>(&'a self, _request: Request<Bytes>) -> BoxRespFut<'a> {
        // Unreachable in practice (`launch` never returns `Ok` off Linux),
        // but the trait must be satisfied for the type to exist.
        Box::pin(async {
            Ok(Response::builder()
                .status(http::StatusCode::NOT_IMPLEMENTED)
                .body(Bytes::from_static(
                    b"firecracker not supported on this host",
                ))?)
        })
    }

    /// Firecracker is never available on non-Linux hosts: always Unavailable.
    fn health<'a>(&'a self) -> BoxFut<'a, RuntimeHealth> {
        Box::pin(async {
            RuntimeHealth::Unavailable(
                "firecracker runtime not supported on this host (not Linux)".to_owned(),
            )
        })
    }
}

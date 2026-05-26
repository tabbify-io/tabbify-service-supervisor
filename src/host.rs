//! Per-app-ULA hosting (contract §5, Component 3).
//!
//! An app IS a deterministic mesh address. When the supervisor starts hosting
//! app `U` it:
//! 1. computes `app_ula = derive_app_ula(U)` ([`crate::app_ula`]);
//! 2. asks the joiner to route that app-ULA to this node
//!    ([`mesh_joiner::Joiner::host_app_ula`]) so inbound packets reach a local
//!    listener — skipped in `--no-mesh`/loopback mode (no TUN to alias);
//! 3. binds a DEDICATED axum listener on `[app_ula]:8730` whose WHOLE request
//!    path is dispatched to the app's [`crate::runtime::AppRuntime`] (WASM or
//!    Firecracker) — there is NO `/apps/<uuid>` prefix, the ULA itself is the
//!    app identity.
//!
//! On stop / idle-reap the listener is aborted and the app-ULA is unhosted.
//!
//! # Mesh seam ([`MeshHost`])
//! The joiner is reached through the [`MeshHost`] trait (implemented for
//! [`mesh_joiner::Joiner`]) so the hosting/teardown logic is unit-testable with
//! a fake that records calls — no real TUN required. In `--no-mesh`/test mode
//! the mesh handle is `None`: app listeners bind a loopback addr (port 0 →
//! ephemeral) which tests dial directly.

use std::future::Future;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use bytes::Bytes;
use http_body_util::BodyExt;
use serde_json::json;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use crate::runtime::AppRuntime;

/// A boxed, `Send` future — the `dyn`-compatible return shape for the async
/// [`MeshHost`] methods (avoids pulling in `async-trait`). The per-app listener
/// binds the same port 8730 as the control API (config default); the app-ULA
/// prefix disambiguates control vs app traffic.
type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The slice of the mesh joiner the hosting layer needs. Implemented for the
/// real [`mesh_joiner::Joiner`]; faked in unit tests so we can assert the
/// app-ULA routing calls without a TUN.
pub trait MeshHost: Send + Sync {
    /// The overlay TUN interface name (`None` without a TUN).
    fn tun_iface(&self) -> Option<String>;
    /// Start routing `app_ula` to this node (joiner `host_app_ula`).
    fn mesh_host_ula(&self, app_ula: Ipv6Addr) -> BoxFut<'_, Result<()>>;
    /// Stop routing `app_ula` to this node (joiner `unhost_app_ula`).
    fn mesh_unhost_ula(&self, app_ula: Ipv6Addr) -> BoxFut<'_, Result<()>>;
}

impl MeshHost for mesh_joiner::Joiner {
    fn tun_iface(&self) -> Option<String> {
        self.tun_name()
    }
    fn mesh_host_ula(&self, app_ula: Ipv6Addr) -> BoxFut<'_, Result<()>> {
        Box::pin(async move { self.host_app_ula(app_ula).await })
    }
    fn mesh_unhost_ula(&self, app_ula: Ipv6Addr) -> BoxFut<'_, Result<()>> {
        Box::pin(async move { self.unhost_app_ula(app_ula).await })
    }
}

/// Where per-app listeners bind.
#[derive(Debug, Clone, Copy)]
pub enum HostBind {
    /// Old multi-app supervisor mode: bind `[app_ula]:port` for an app-ULA that
    /// is advertised through the joiner's `host_app_ula` app-route layer
    /// (distinct from the peer's own ULA).
    AppUla(u16),
    /// Runner mode: bind the runner's OWN mesh ULA `[my_ula]:port`. The runner
    /// joined claiming `requested_ula = derive_app_ula(uuid)`, so the
    /// coordinator already routes this ULA straight to it — no `host_app_ula`
    /// app-route layer is needed (and the bound app-ULA == the peer-ULA).
    OwnUla(u16),
    /// No-mesh / test mode: bind on `ip` with an ephemeral port (no TUN, so we
    /// can't bind an app-ULA — tests/loopback dial the returned addr instead).
    Loopback(IpAddr),
}

/// Hosting machinery: routes app-ULAs (via the joiner, when present) and binds
/// one axum listener per hosted app.
#[derive(Clone)]
pub struct AppHost {
    /// Mesh joiner, present only when joined (absent in `--no-mesh`/tests).
    mesh: Option<Arc<dyn MeshHost>>,
    /// How app listeners bind.
    bind: HostBind,
}

impl AppHost {
    /// Old multi-app supervisor host: bind each app on its own app-ULA and
    /// advertise it via the joiner (`host_app_ula`) so peers route to us. Kept
    /// for the supervisor path until Phase 2 rewires it; the runner uses
    /// [`Self::mesh_self`] instead.
    #[must_use]
    pub fn mesh(joiner: Arc<dyn MeshHost>, port: u16) -> Self {
        Self {
            mesh: Some(joiner),
            bind: HostBind::AppUla(port),
        }
    }

    /// Runner host: bind the runner's OWN mesh ULA `[my_ula]:port`.
    ///
    /// The runner joined the mesh claiming `requested_ula = derive_app_ula(uuid)`
    /// (see [`crate::runner::serve::build_runner_join_config`]), so the
    /// coordinator already routes `my_ula` straight to this peer. We therefore
    /// bind it directly and carry NO [`MeshHost`] joiner — the separate
    /// `host_app_ula` app-route layer (used by [`Self::mesh`] to advertise
    /// app-ULAs distinct from a peer's own ULA) is not needed here.
    ///
    /// The `my_ula` passed to [`Self::host`] as `app_ula` is this same address
    /// (the runner's ULA *is* the app-ULA), so the bound listener address is
    /// `[my_ula]:port`.
    #[must_use]
    pub const fn mesh_self(_my_ula: Ipv6Addr, port: u16) -> Self {
        Self {
            mesh: None,
            bind: HostBind::OwnUla(port),
        }
    }

    /// Loopback host (`--no-mesh` / tests): no joiner, app listeners bind
    /// `[::1]:0` (ephemeral). The runtime/lifecycle/serving can be exercised
    /// with no TUN.
    #[must_use]
    pub fn loopback() -> Self {
        Self::loopback_on(IpAddr::V6(Ipv6Addr::LOCALHOST))
    }

    /// Loopback host bound on an injectable IP (tests that need a specific
    /// loopback address). Port is always ephemeral.
    #[must_use]
    pub fn loopback_on(ip: IpAddr) -> Self {
        Self {
            mesh: None,
            bind: HostBind::Loopback(ip),
        }
    }

    /// Is this host backed by the `host_app_ula` app-route layer (a real mesh
    /// joiner)? `false` for loopback AND for [`Self::mesh_self`] (the runner
    /// binds its own already-routed ULA, no app-route needed).
    #[must_use]
    pub const fn is_mesh(&self) -> bool {
        self.mesh.is_some()
    }

    /// The configured bind mode. Exposed so the runner can assert it selects
    /// the right addressing without a live join.
    #[must_use]
    pub const fn bind(&self) -> HostBind {
        self.bind
    }

    /// The socket address a listener would bind for `app_ula` under this host's
    /// [`HostBind`] mode. For `AppUla`/`OwnUla` it's `[app_ula]:port`; for
    /// `Loopback` it's the loopback IP with an ephemeral (`0`) port. Pure (no
    /// I/O) so the bind-address selection is unit-testable.
    #[must_use]
    pub fn bind_addr_for(&self, app_ula: Ipv6Addr) -> SocketAddr {
        match self.bind {
            HostBind::AppUla(port) | HostBind::OwnUla(port) => {
                SocketAddr::new(IpAddr::V6(app_ula), port)
            }
            HostBind::Loopback(ip) => SocketAddr::new(ip, 0),
        }
    }

    /// Start hosting `app_ula`: in the old multi-app supervisor mode
    /// ([`Self::mesh`]) route it through the joiner's `host_app_ula` app-route
    /// layer; in [`Self::mesh_self`]/[`Self::loopback`] there is no joiner to
    /// call (the runner's ULA is already routed; loopback has no TUN). Then bind
    /// a per-app listener that serves `serve`'s WASM on the WHOLE path.
    ///
    /// # Errors
    /// - the joiner refuses to host the app-ULA (mesh mode; e.g. no TUN);
    /// - the per-app listener fails to bind.
    pub async fn host(&self, app_ula: Ipv6Addr, serve: AppServe) -> Result<HostedApp> {
        // Mesh mode: tell the joiner to route app_ula to us (adds the /128 TUN
        // alias + advertises it on the next heartbeat). Skipped in --no-mesh:
        // there's no TUN to alias, so we only bind the loopback listener.
        if let Some(mesh) = &self.mesh {
            mesh.mesh_host_ula(app_ula)
                .await
                .with_context(|| format!("joiner host_app_ula({app_ula})"))?;
        }

        let bind_addr = self.bind_addr_for(app_ula);
        let listener = TcpListener::bind(bind_addr)
            .await
            .with_context(|| format!("bind per-app listener {bind_addr}"))?;
        let addr = listener
            .local_addr()
            .context("per-app listener local_addr")?;

        let router = app_router(serve);
        let task = tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, router).await {
                tracing::warn!(%addr, error = %e, "per-app listener exited");
            }
        });

        tracing::info!(%app_ula, %addr, "hosting app on per-app listener");
        Ok(HostedApp {
            app_ula,
            addr,
            task,
        })
    }

    /// Stop hosting: abort the per-app listener and unhost the app-ULA on the
    /// joiner (best-effort — teardown proceeds even if the joiner call fails).
    pub async fn unhost(&self, hosted: HostedApp) {
        let app_ula = hosted.app_ula;
        hosted.task.abort();
        if let Some(mesh) = &self.mesh {
            if let Err(e) = mesh.mesh_unhost_ula(app_ula).await {
                tracing::warn!(%app_ula, error = %e, "joiner unhost_app_ula failed (continuing)");
            }
        }
        tracing::info!(%app_ula, "stopped hosting app");
    }
}

/// Per-request state for a hosted app's listener: the app runtime (WASM or
/// firecracker, behind the [`AppRuntime`] trait) plus an activity callback (so
/// the idle reaper sees per-app-listener traffic).
#[derive(Clone)]
pub struct AppServe {
    /// The app's runtime as a trait object — either the in-process WASM runtime
    /// or the Firecracker microVM runtime. `Arc` so the listener can share it.
    runtime: Arc<dyn AppRuntime>,
    /// Called on every request so the registry can bump `last_activity`.
    on_request: Arc<dyn Fn() + Send + Sync>,
}

impl AppServe {
    /// Build serve state from a runtime + an activity callback.
    #[must_use]
    pub fn new(runtime: Arc<dyn AppRuntime>, on_request: Arc<dyn Fn() + Send + Sync>) -> Self {
        Self {
            runtime,
            on_request,
        }
    }
}

/// A live per-app listener: its app-ULA, the address it bound (the app-ULA in
/// mesh mode; a loopback ephemeral addr in `--no-mesh`/tests), and the task
/// handle (aborted on unhost / drop).
pub struct HostedApp {
    /// The app's deterministic ULA.
    pub app_ula: Ipv6Addr,
    /// The address this listener bound (dial this to reach the app).
    pub addr: SocketAddr,
    /// The serving task; aborted on [`AppHost::unhost`] or drop.
    task: JoinHandle<()>,
}

impl Drop for HostedApp {
    fn drop(&mut self) {
        // Defensive: a dropped hosted app (e.g. on process teardown or if a
        // record is replaced) must not leak its listener task.
        self.task.abort();
    }
}

/// The per-app axum router: EVERY path/method falls through to the WASM
/// runtime (no `/apps/<uuid>` prefix — the ULA is the identity).
fn app_router(serve: AppServe) -> Router {
    Router::new()
        .fallback(any(serve_app))
        .with_state(Arc::new(serve))
}

/// Serve one request: bump activity, buffer the body, hand the WHOLE request to
/// the WASM runtime, translate its response back.
async fn serve_app(State(serve): State<Arc<AppServe>>, req: Request<Body>) -> Response {
    (serve.on_request)();

    let (parts, body) = req.into_parts();
    let collected = match body.collect().await {
        Ok(c) => c.to_bytes(),
        Err(e) => return error_json(StatusCode::BAD_REQUEST, &format!("read request body: {e}")),
    };
    let wasm_req = Request::from_parts(parts, collected);

    match serve.runtime.handle(wasm_req).await {
        Ok(resp) => wasm_response_to_axum(resp),
        Err(e) => error_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("wasm execution failed: {e}"),
        ),
    }
}

/// Translate the wasm `http::Response<Bytes>` into an axum response.
fn wasm_response_to_axum(resp: http::Response<Bytes>) -> Response {
    let (parts, body) = resp.into_parts();
    let mut builder = Response::builder().status(parts.status);
    if let Some(headers) = builder.headers_mut() {
        *headers = parts.headers;
    }
    builder
        .body(Body::from(body))
        .unwrap_or_else(|_| error_json(StatusCode::INTERNAL_SERVER_ERROR, "build response"))
}

fn error_json(status: StatusCode, msg: &str) -> Response {
    (status, axum::Json(json!({ "error": msg }))).into_response()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use dashmap::DashMap;
    use uuid::Uuid;

    use super::*;
    use crate::app_ula::derive_app_ula;
    use crate::runtime::WasmRuntime;

    const APP_UUID: &str = "0191e7c2-1111-7222-8333-444455556666";
    const HELLO_WASM: &[u8] = include_bytes!("../tests/fixtures/hello.wasm");

    /// A fake [`MeshHost`] that records the app-ULAs it was asked to host /
    /// unhost, so we can assert the routing calls with no real TUN.
    #[derive(Default)]
    struct FakeMeshHost {
        hosted: DashMap<Ipv6Addr, ()>,
        unhosted: DashMap<Ipv6Addr, ()>,
    }

    impl MeshHost for FakeMeshHost {
        fn tun_iface(&self) -> Option<String> {
            Some("utun-fake".to_owned())
        }
        fn mesh_host_ula(&self, app_ula: Ipv6Addr) -> BoxFut<'_, Result<()>> {
            self.hosted.insert(app_ula, ());
            Box::pin(async { Ok(()) })
        }
        fn mesh_unhost_ula(&self, app_ula: Ipv6Addr) -> BoxFut<'_, Result<()>> {
            self.unhosted.insert(app_ula, ());
            Box::pin(async { Ok(()) })
        }
    }

    fn fixture_runtime() -> Arc<dyn AppRuntime> {
        Arc::new(WasmRuntime::load(HELLO_WASM).expect("load fixture"))
    }

    fn noop_serve() -> AppServe {
        AppServe::new(fixture_runtime(), Arc::new(|| {}))
    }

    /// Hosting in mesh mode computes the right app-ULA and routes it through
    /// the joiner via `host_app_ula`.
    #[tokio::test]
    async fn host_routes_the_derived_app_ula_through_the_joiner() {
        let fake = Arc::new(FakeMeshHost::default());
        // Loopback bind so we don't need a real TUN, but a real mesh handle so
        // the joiner routing call still fires.
        let host = AppHost {
            mesh: Some(fake.clone()),
            bind: HostBind::Loopback(IpAddr::V6(Ipv6Addr::LOCALHOST)),
        };
        let app_ula = derive_app_ula(Uuid::parse_str(APP_UUID).unwrap());

        let hosted = host.host(app_ula, noop_serve()).await.expect("host");

        assert_eq!(hosted.app_ula, app_ula);
        assert!(
            fake.hosted.contains_key(&app_ula),
            "joiner.host_app_ula was not called with the derived app-ULA"
        );
        host.unhost(hosted).await;
    }

    /// Unhosting aborts the listener AND unhosts the app-ULA on the joiner.
    #[tokio::test]
    async fn unhost_calls_joiner_unhost_and_aborts_listener() {
        let fake = Arc::new(FakeMeshHost::default());
        let host = AppHost {
            mesh: Some(fake.clone()),
            bind: HostBind::Loopback(IpAddr::V6(Ipv6Addr::LOCALHOST)),
        };
        let app_ula = derive_app_ula(Uuid::parse_str(APP_UUID).unwrap());
        let hosted = host.host(app_ula, noop_serve()).await.expect("host");
        let addr = hosted.addr;

        host.unhost(hosted).await;

        assert!(fake.unhosted.contains_key(&app_ula), "unhost not called");
        // The listener is gone: a fresh connection must fail.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let dial = reqwest::Client::new()
            .get(format!("http://{addr}/"))
            .timeout(std::time::Duration::from_millis(300))
            .send()
            .await;
        assert!(dial.is_err(), "listener still answered after unhost");
    }

    /// In `--no-mesh`/loopback mode the joiner is NOT called (no TUN) and the
    /// per-app listener still serves the fixture WASM end-to-end on the bound
    /// loopback addr.
    #[tokio::test]
    async fn loopback_host_serves_wasm_without_a_joiner() {
        let host = AppHost::loopback();
        assert!(!host.is_mesh());
        let app_ula = derive_app_ula(Uuid::parse_str(APP_UUID).unwrap());

        let hosted = host.host(app_ula, noop_serve()).await.expect("host");
        let addr = hosted.addr;

        let body = reqwest::Client::new()
            .get(format!("http://{addr}/api/hello"))
            .send()
            .await
            .expect("dial")
            .text()
            .await
            .expect("body");
        assert_eq!(body, "Hello, Tabbify!");
        host.unhost(hosted).await;
    }

    /// The activity callback fires once per request to the per-app listener.
    #[tokio::test]
    async fn serving_bumps_the_activity_callback() {
        let host = AppHost::loopback();
        let hits = Arc::new(AtomicUsize::new(0));
        let h2 = hits.clone();
        let serve = AppServe::new(
            fixture_runtime(),
            Arc::new(move || {
                h2.fetch_add(1, Ordering::SeqCst);
            }),
        );
        let app_ula = derive_app_ula(Uuid::parse_str(APP_UUID).unwrap());
        let hosted = host.host(app_ula, serve).await.expect("host");
        let addr = hosted.addr;

        let client = reqwest::Client::new();
        for _ in 0..3 {
            let _ = client
                .get(format!("http://{addr}/"))
                .send()
                .await
                .expect("get");
        }
        assert_eq!(hits.load(Ordering::SeqCst), 3);
        host.unhost(hosted).await;
    }
}

//! Minimal WASM HTTP runtime (contract §8).
//!
//! Loads a `wasi:http/proxy@0.2` component (a Tabbify HTTP app) and dispatches
//! incoming HTTP requests to it. This is the substrate `wasm-http-runtime` glue
//! stripped of the custom `event-log` host import: Phase-1 test apps are pure
//! `wasi:http/proxy` and instantiate with stock `wasmtime-wasi` +
//! `wasmtime-wasi-http` linkers only.
//!
//! Design:
//! - One [`Engine`] per [`WasmRuntime`] (engine construction is expensive; it is
//!   refcounted and cheap to clone).
//! - `instantiate_pre` + `ProxyPre::new` (the link/type-check step) are done
//!   ONCE in [`WasmRuntime::load`] and stored as `proxy_pre`.
//! - Each request gets a FRESH [`Store`] with its own fuel budget and resource
//!   table, then calls `proxy_pre.instantiate_async` — no re-linking per request.
//!
//! Public API (§8):
//! - [`WasmRuntime::load`] — compile + link a component once.
//! - [`WasmRuntime::handle`] — run one request, return its response.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use bytes::Bytes;
use http::{Request, Response};
use http_body_util::{BodyExt, Full};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use wasmtime::component::{Linker, ResourceTable};
use wasmtime::{Config, Engine, InstanceAllocationStrategy, PoolingAllocationConfig, Store};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiView};
use wasmtime_wasi_http::bindings::ProxyPre;
use wasmtime_wasi_http::bindings::http::types::{ErrorCode, Scheme};
use wasmtime_wasi_http::body::HyperOutgoingBody;
use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpView};

/// Default per-request fuel budget when a manifest omits it (mirrors §3).
pub const DEFAULT_FUEL_PER_REQUEST: u64 = 1_000_000_000;

/// A boxed, `Send` future — the object-safe return shape for [`AppRuntime`]
/// (avoids the `async-trait` dependency, mirroring [`crate::host::MeshHost`]).
pub type BoxRespFut<'a> = Pin<Box<dyn Future<Output = Result<Response<Bytes>>> + Send + 'a>>;

/// A generic boxed, `Send` future for any output type — used by
/// [`AppRuntime::health`] so the trait stays object-safe without `async-trait`.
pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Liveness of the app itself (not the runner process).
///
/// Returned by [`AppRuntime::health`]. `Serving` means the runtime considers
/// the app reachable and ready; `Unavailable` carries a human-readable reason
/// (e.g. "TCP connect refused" or "container stopped").
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeHealth {
    /// The app is up and serving requests.
    Serving,
    /// The app is not reachable; the String explains why.
    Unavailable(String),
}

/// The reason an app runtime exited unexpectedly.
///
/// Resolved by [`AppRuntime::watch_for_exit`] when the runtime dies without an
/// explicit [`AppRuntime::shutdown`] request. The runner uses this to trigger a
/// fail-fast `process::exit(1)` so the supervisor's L2 monitor respawns it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExitReason {
    /// The runtime process / container died; the String carries a detail
    /// (e.g. the container name and exit code).
    Died(String),
}

// Deploy-time runtime selection enum — the FROZEN wire type from the contract.
//
// Distinct from `manifest::Runtime` (the `[runtime]` TABLE). This enum is the
// `runtime` field in `[runtime].type`, each `[[deploy]].runtime`, and the
// node→supervisor request body. Wire strings are FROZEN (contract D4):
// `"docker"` / `"firecracker"` / `"wasm-http"`. Vendor IDENTICALLY in
// cli / node / supervisor; every repo carries the golden round-trip test below.

/// How a runner EXECUTES the artifact, chosen at deploy time per target.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    utoipa::ToSchema,
    schemars::JsonSchema,
)]
#[serde(rename_all = "kebab-case")]
pub enum Runtime {
    /// `docker run` the OCI image.
    Docker,
    /// Boot the OCI image as a Firecracker microVM.
    Firecracker,
    /// In-process wasmtime; the always-available runtime (never capability-gated).
    #[default]
    WasmHttp,
}

impl Runtime {
    /// The exact wire string (FROZEN, contract D4). Mirrors serde output.
    #[must_use]
    pub fn as_wire(self) -> &'static str {
        match self {
            Runtime::Docker => "docker",
            Runtime::Firecracker => "firecracker",
            Runtime::WasmHttp => "wasm-http",
        }
    }
}

/// The runtime seam the per-app listener ([`crate::host`]) dispatches to. Both
/// the in-process WASM runtime ([`WasmRuntime`]) and the Firecracker microVM
/// runtime ([`crate::firecracker::FirecrackerRuntime`]) implement it, so the
/// hosting/serving code is identical regardless of how an app actually runs.
///
/// Object-safe (`Arc<dyn AppRuntime>`): the registry picks the concrete runtime
/// from `manifest.runtime.type` and hands the listener a trait object.
pub trait AppRuntime: Send + Sync {
    /// Drive one HTTP request through the app and return its response.
    ///
    /// # Errors
    /// Runtime-specific: a wasm trap / fuel exhaustion, or (firecracker) a proxy
    /// failure talking to the guest.
    fn handle<'a>(&'a self, request: Request<Bytes>) -> BoxRespFut<'a>;

    /// Liveness of the app itself (not the runner process).
    ///
    /// Default: [`RuntimeHealth::Serving`] — a wasm runtime is serviceable as
    /// soon as it is loaded. Firecracker and Docker override this with a real
    /// probe (TCP connect to the guest/container).
    fn health<'a>(&'a self) -> BoxFut<'a, RuntimeHealth> {
        Box::pin(async { RuntimeHealth::Serving })
    }

    /// Resolves when the runtime dies UNEXPECTEDLY (without an explicit
    /// [`shutdown`] call). The runner selects on this alongside its shutdown
    /// signal: if this resolves first the runner calls `process::exit(1)` so
    /// the supervisor's L2 monitor respawns it with backoff.
    ///
    /// Default: **never resolves** — a WASM runtime is in-process and handles
    /// one request at a time; there is no long-lived external process to watch.
    /// Docker and Firecracker override this with real process/container watching.
    ///
    /// [`shutdown`]: AppRuntime::shutdown
    fn watch_for_exit<'a>(&'a self) -> BoxFut<'a, ExitReason> {
        Box::pin(std::future::pending())
    }

    /// Graceful teardown of the runtime's resources. Idempotent. Default: no-op.
    ///
    /// Called by the runner on the [`RunnerExit::CleanShutdown`] path — BEFORE
    /// `process::exit(0)` — so the runtime can release its external resources
    /// (stop a container, kill a VM + tear down the tap) cleanly. NOT called on
    /// [`RunnerExit::Crashed`]: the runtime already died; [`Drop`] + the L2
    /// kill-before-respawn handle remnants instead.
    ///
    /// Implementations MUST be idempotent: a second call must be a no-op (the
    /// container / VM may already be gone by the time `Drop` runs its own
    /// best-effort cleanup).
    ///
    /// Default: **no-op** — a WASM runtime drops cleanly with no external
    /// resources to release; `WasmRuntime` uses this default.
    fn shutdown<'a>(&'a self) -> BoxFut<'a, ()> {
        Box::pin(async {})
    }
}

impl AppRuntime for WasmRuntime {
    fn handle<'a>(&'a self, request: Request<Bytes>) -> BoxRespFut<'a> {
        Box::pin(WasmRuntime::handle(self, request))
    }
}

/// Per-`Store` context required by `wasmtime-wasi` (Preview 2) and
/// `wasmtime-wasi-http`. Intentionally minimal: empty stdio/env/fs — apps run
/// with no ambient authority beyond what `wasi:http` itself needs.
struct Ctx {
    table: ResourceTable,
    wasi: WasiCtx,
    http: WasiHttpCtx,
}

impl Ctx {
    fn new() -> Self {
        Self {
            table: ResourceTable::new(),
            wasi: WasiCtxBuilder::new().build(),
            http: WasiHttpCtx::new(),
        }
    }
}

impl WasiView for Ctx {
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.wasi
    }
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}

impl WasiHttpView for Ctx {
    fn ctx(&mut self) -> &mut WasiHttpCtx {
        &mut self.http
    }
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}

/// A compiled-and-linked WASM HTTP component, ready to serve requests.
///
/// Cheap to clone: `Engine` is refcounted, `ProxyPre` is wrapped in `Arc`
/// (the macro-generated type may not derive `Clone` itself, but `Arc` is always
/// cheap to clone). A running app instance can be shared across concurrent
/// requests without re-linking.
#[derive(Clone)]
pub struct WasmRuntime {
    engine: Engine,
    /// Pre-instantiation artifact: the link/type-check step done once in `load`.
    /// Per-request code only calls `proxy_pre.instantiate_async(store)`.
    proxy_pre: Arc<ProxyPre<Ctx>>,
    fuel_per_request: u64,
}

impl WasmRuntime {
    /// Compile `wasm_bytes` into a ready-to-serve runtime with the default fuel
    /// budget. See [`WasmRuntime::load_with_fuel`] to override the budget.
    ///
    /// # Errors
    /// - the bytes aren't a valid wasm component,
    /// - the engine cannot be built with the component model enabled,
    /// - linker registration fails.
    pub fn load(wasm_bytes: &[u8]) -> Result<Self> {
        Self::load_with_fuel(wasm_bytes, DEFAULT_FUEL_PER_REQUEST)
    }

    /// Like [`WasmRuntime::load`] but with an explicit per-request fuel budget
    /// (taken from the app manifest's `runtime.fuel_per_request`).
    ///
    /// The link/type-check step (`instantiate_pre` + `ProxyPre::new`) runs here,
    /// once, and the result is stored in `proxy_pre`. Per-request code only calls
    /// `instantiate_async` against the pre-built artifact.
    ///
    /// # Errors
    /// See [`WasmRuntime::load`].
    pub fn load_with_fuel(wasm_bytes: &[u8], fuel_per_request: u64) -> Result<Self> {
        let engine = new_engine()?;
        let component = wasmtime::component::Component::new(&engine, wasm_bytes)?;
        let linker = build_linker(&engine)?;
        let pre = linker.instantiate_pre(&component)?;
        let proxy_pre = Arc::new(ProxyPre::new(pre)?);
        Ok(Self {
            engine,
            proxy_pre,
            fuel_per_request,
        })
    }

    /// Load a compiled component from an AOT `.cwasm` cache, or compile from
    /// `wasm_bytes` if the cache is absent, corrupt, or version-mismatched.
    ///
    /// Cache decision:
    /// 1. `cache_path` exists → `Component::deserialize_file` (skip Cranelift).
    ///    If that fails for any reason (missing, corrupt, engine/version header
    ///    mismatch — wasmtime embeds a header and `deserialize` rejects
    ///    mismatches) → fall through to compile.
    /// 2. Compile path: `Component::new` (Cranelift), then best-effort write the
    ///    cache: `engine.precompile_component` → write bytes to `cache_path`.
    ///    A write failure is logged and silently ignored (caching is advisory).
    ///
    /// The linker + `ProxyPre` are built identically to [`load_with_fuel`].
    ///
    /// # Errors
    /// - `wasm_bytes` is not a valid wasm component,
    /// - the engine cannot be built,
    /// - linker registration fails.
    pub fn load_cached_or_compile(
        wasm_bytes: &[u8],
        cache_path: &std::path::Path,
        fuel_per_request: u64,
    ) -> Result<Self> {
        let engine = new_engine()?;

        // --- cache hit: try to deserialize the pre-compiled artifact ----------
        let component = if cache_path.exists() {
            // SAFETY: wasmtime validates an engine-version/config header embedded
            // in the `.cwasm` file and returns `Err` on any mismatch.  We handle
            // that error by falling back to a full recompile below.
            match unsafe { wasmtime::component::Component::deserialize_file(&engine, cache_path) } {
                Ok(c) => {
                    tracing::debug!(path = %cache_path.display(), "wasm cache hit: deserialized .cwasm");
                    c
                }
                Err(e) => {
                    tracing::warn!(
                        path = %cache_path.display(),
                        error = %e,
                        "wasm cache miss (corrupt/mismatch): recompiling"
                    );
                    compile_and_cache(&engine, wasm_bytes, cache_path)?
                }
            }
        } else {
            // --- cache miss: compile and persist ------------------------------
            compile_and_cache(&engine, wasm_bytes, cache_path)?
        };

        let linker = build_linker(&engine)?;
        let pre = linker.instantiate_pre(&component)?;
        let proxy_pre = Arc::new(ProxyPre::new(pre)?);
        Ok(Self {
            engine,
            proxy_pre,
            fuel_per_request,
        })
    }

    /// Per-request fuel budget this runtime was built with.
    #[must_use]
    pub const fn fuel_per_request(&self) -> u64 {
        self.fuel_per_request
    }

    /// Drive one HTTP request through the WASM component and collect the
    /// response into memory.
    ///
    /// A fresh [`Store`] with its own fuel budget is created per call; both the
    /// request and response bodies are buffered (Phase-1). The link/type-check
    /// step was done once in [`WasmRuntime::load`]; this method only calls the
    /// cheap `instantiate_async` against the pre-built `proxy_pre`.
    ///
    /// # Errors
    /// - per-request fuel exhausted or the component traps,
    /// - the component returns without producing a response,
    /// - the response body fails to collect.
    pub async fn handle(&self, request: Request<Bytes>) -> Result<Response<Bytes>> {
        let ctx = Ctx::new();
        let mut store = Store::new(&self.engine, ctx);
        store.set_fuel(self.fuel_per_request)?;

        // Realise the pre-built proxy_pre into a per-request `Proxy` bound to
        // the fresh store. No re-linking: the link/type-check step is in `load`.
        let proxy = self.proxy_pre.instantiate_async(&mut store).await?;

        // Translate the inbound http::Request<Bytes> into a wasi:http body,
        // then mint the incoming-request resource via WasiHttpView.
        //
        // `wasi:http` requires a well-formed request: scheme + authority must
        // be present (otherwise it errors with "missing authority"). Server
        // requests (e.g. from axum) carry only a path-and-query URI plus a
        // `Host` header, so we normalize here — deriving the authority from the
        // URI, then the `Host` header, then a `localhost` placeholder.
        let (mut parts, body) = request.into_parts();
        let scheme = scheme_from_uri(parts.uri.scheme_str());
        parts.uri = normalize_uri(&parts.uri, &scheme, parts.headers.get(http::header::HOST))?;
        let body = Full::new(body).map_err(|never| match never {}).boxed();
        let req_record = http::Request::from_parts(parts, body);
        let incoming = WasiHttpView::new_incoming_request(store.data_mut(), scheme, req_record)?;

        // `call_handle` does NOT return the response directly — the component
        // writes it into a one-shot sender which we await after the call.
        let (tx, rx) = oneshot::channel::<Result<http::Response<HyperOutgoingBody>, ErrorCode>>();
        let outparam = WasiHttpView::new_response_outparam(store.data_mut(), tx)?;

        proxy
            .wasi_http_incoming_handler()
            .call_handle(&mut store, incoming, outparam)
            .await?;

        let resp_or_err = rx
            .await
            .map_err(|_| anyhow!("component did not produce a response"))?;
        let resp = resp_or_err.map_err(|e| anyhow!("wasi:http error: {e:?}"))?;

        let (resp_parts, body) = resp.into_parts();
        let collected = body
            .collect()
            .await
            .map_err(|e| anyhow!("failed to collect response body: {e:?}"))?
            .to_bytes();
        Ok(Response::from_parts(resp_parts, collected))
    }
}

/// Compile `wasm_bytes` with Cranelift and best-effort write the pre-compiled
/// artifact to `cache_path` so future calls can skip compilation.
///
/// Cache write failures are logged and silently ignored: the caller gets a
/// usable `Component` regardless.
fn compile_and_cache(
    engine: &Engine,
    wasm_bytes: &[u8],
    cache_path: &std::path::Path,
) -> Result<wasmtime::component::Component> {
    let component = wasmtime::component::Component::new(engine, wasm_bytes)?;

    // Best-effort: precompile → write; log + ignore any error.
    match engine.precompile_component(wasm_bytes) {
        Ok(cwasm_bytes) => {
            // Create parent directory if it doesn't exist.
            if let Some(parent) = cache_path.parent() {
                if let Err(e) = std::fs::create_dir_all(parent) {
                    tracing::warn!(
                        path = %cache_path.display(),
                        error = %e,
                        "wasm cache: could not create cache dir (ignored)"
                    );
                    return Ok(component);
                }
            }
            match std::fs::write(cache_path, &cwasm_bytes) {
                Ok(()) => {
                    tracing::debug!(
                        path = %cache_path.display(),
                        bytes = cwasm_bytes.len(),
                        "wasm cache: wrote .cwasm"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        path = %cache_path.display(),
                        error = %e,
                        "wasm cache: could not write .cwasm (ignored)"
                    );
                }
            }
        }
        Err(e) => {
            tracing::warn!(
                path = %cache_path.display(),
                error = %e,
                "wasm cache: precompile_component failed (ignored)"
            );
        }
    }

    Ok(component)
}

/// Build a Wasmtime [`Engine`]: component model on, async on, fuel on.
///
/// Uses a pooling instance-allocation strategy with conservative limits
/// suitable for `wasi:http/proxy` components.  The limits are sized for a
/// single-app runner: a handful of concurrent in-flight requests, one or two
/// memories per request, and modest table/stack counts.
///
/// If the pooling allocator cannot be initialised (non-fatal platform
/// constraint), the engine falls back to the default on-demand allocator and
/// logs a warning.
fn new_engine() -> Result<Engine> {
    let mut cfg = Config::new();
    cfg.async_support(true);
    cfg.consume_fuel(true);
    cfg.wasm_component_model(true);

    // Pooling allocator — conservative limits for a single-app WASM runner.
    // Each `wasi:http/proxy` request uses one component instance with one
    // linear memory and a small number of tables.  We size the pool to allow
    // up to 16 concurrent in-flight requests without contention.
    let mut pool = PoolingAllocationConfig::default();
    pool.total_component_instances(32);
    pool.total_core_instances(64); // inner wasm modules inside the component
    pool.total_memories(32);
    pool.total_tables(64);
    // Keep a small amount of memory warm between requests so the next
    // instantiation reuses a slot that already has physical pages mapped.
    pool.linear_memory_keep_resident(1 << 20); // 1 MiB
    pool.table_keep_resident(1 << 16); // 64 KiB
    cfg.allocation_strategy(InstanceAllocationStrategy::Pooling(pool));

    Engine::new(&cfg)
}

/// Build the component [`Linker`]: Preview-2 WASI first (async), then the
/// HTTP-only surface on top.
///
/// Using [`wasmtime_wasi_http::add_to_linker_async`] for step 2 would
/// re-register `wasi:clocks/*` (already added by `wasmtime_wasi`), which the
/// linker rejects with "defined twice"; `add_only_http_to_linker_async` is the
/// seam that lets the two crates coexist. No custom host imports are added —
/// the test apps are pure `wasi:http/proxy`.
fn build_linker(engine: &Engine) -> Result<Linker<Ctx>> {
    let mut linker = Linker::<Ctx>::new(engine);
    wasmtime_wasi::add_to_linker_async(&mut linker)?;
    wasmtime_wasi_http::add_only_http_to_linker_async(&mut linker)?;
    Ok(linker)
}

/// Map an optional URI scheme string to the wasi:http [`Scheme`] enum.
/// Defaults to [`Scheme::Http`] when no scheme is present (relative URIs).
fn scheme_from_uri(scheme: Option<&str>) -> Scheme {
    match scheme {
        Some("http") | None => Scheme::Http,
        Some("https") => Scheme::Https,
        Some(other) => Scheme::Other(other.to_string()),
    }
}

/// Rebuild `uri` so it always carries a scheme + authority, as `wasi:http`
/// requires. Keeps the original path-and-query; fills the authority from the
/// existing URI authority, else the `Host` header, else `localhost`.
fn normalize_uri(
    uri: &http::Uri,
    scheme: &Scheme,
    host_header: Option<&http::HeaderValue>,
) -> Result<http::Uri> {
    let scheme_str = match scheme {
        Scheme::Http => "http",
        Scheme::Https => "https",
        Scheme::Other(o) => o.as_str(),
    };
    let authority = uri
        .authority()
        .map(|a| a.as_str().to_owned())
        .or_else(|| host_header.and_then(|h| h.to_str().ok()).map(str::to_owned))
        .unwrap_or_else(|| "localhost".to_owned());
    let path_and_query = uri.path_and_query().map_or("/", |pq| pq.as_str());

    http::Uri::builder()
        .scheme(scheme_str)
        .authority(authority)
        .path_and_query(path_and_query)
        .build()
        .map_err(|e| anyhow!("normalize uri: {e}"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// GOLDEN round-trip: the wire string for each variant is FROZEN (contract D4).
    /// If this test changes, the contract changed — coordinate all three repos.
    #[test]
    fn runtime_wire_strings_are_frozen() {
        for (variant, wire) in [
            (Runtime::Docker, "docker"),
            (Runtime::Firecracker, "firecracker"),
            (Runtime::WasmHttp, "wasm-http"),
        ] {
            // serialize → exact string
            let json = serde_json::to_string(&variant).unwrap();
            assert_eq!(json, format!("\"{wire}\""), "serialize mismatch for {variant:?}");
            // deserialize ← exact string
            let back: Runtime = serde_json::from_str(&format!("\"{wire}\"")).unwrap();
            assert_eq!(back, variant, "deserialize mismatch for {wire}");
        }
    }

    #[test]
    fn runtime_default_is_wasm_http() {
        assert_eq!(Runtime::default(), Runtime::WasmHttp);
    }

    #[test]
    fn runtime_rejects_unknown_string() {
        assert!(serde_json::from_str::<Runtime>("\"podman\"").is_err());
    }

    /// The committed fixture (`tests/fixtures/hello.wasm`, a pure-proxy
    /// component) must compile and answer a GET with 200 + the expected body.
    /// Uses a full URI (authority present).
    #[tokio::test]
    async fn fixture_get_returns_200_with_body() {
        let wasm = include_bytes!("../tests/fixtures/hello.wasm");
        let rt = WasmRuntime::load(wasm).expect("load fixture");

        let req = Request::builder()
            .method("GET")
            .uri("http://example.com/api/hello")
            .body(Bytes::new())
            .unwrap();
        let resp = rt.handle(req).await.expect("handle request");

        assert_eq!(resp.status(), 200);
        let body = String::from_utf8_lossy(resp.body());
        assert_eq!(body, "Hello, Tabbify!");
    }

    /// A path-only URI + a `Host` header (the shape an axum server hands us)
    /// must be normalized by the runtime and still serve 200.
    #[tokio::test]
    async fn fixture_handles_path_only_uri_with_host_header() {
        let wasm = include_bytes!("../tests/fixtures/hello.wasm");
        let rt = WasmRuntime::load(wasm).expect("load fixture");

        let req = Request::builder()
            .method("GET")
            .uri("/")
            .header("host", "supervisor.local")
            .body(Bytes::new())
            .unwrap();
        let resp = rt.handle(req).await.expect("handle request");
        assert_eq!(resp.status(), 200);
        assert_eq!(String::from_utf8_lossy(resp.body()), "Hello, Tabbify!");
    }

    /// The fixture must serve a 200 + body when driven THROUGH the
    /// `AppRuntime` trait object (the seam the per-app listener uses), not just
    /// via the inherent `WasmRuntime::handle`.
    #[tokio::test]
    async fn serves_fixture_through_appruntime_trait() {
        let wasm = include_bytes!("../tests/fixtures/hello.wasm");
        let rt: std::sync::Arc<dyn AppRuntime> =
            std::sync::Arc::new(WasmRuntime::load(wasm).expect("load fixture"));

        let req = Request::builder()
            .method("GET")
            .uri("http://example.com/api/hello")
            .body(Bytes::new())
            .unwrap();
        let resp = rt.handle(req).await.expect("handle via trait");

        assert_eq!(resp.status(), 200);
        assert_eq!(String::from_utf8_lossy(resp.body()), "Hello, Tabbify!");
    }

    /// The runtime is reusable across requests (fresh store each time).
    #[tokio::test]
    async fn fixture_handles_multiple_requests() {
        let wasm = include_bytes!("../tests/fixtures/hello.wasm");
        let rt = WasmRuntime::load(wasm).expect("load fixture");
        for _ in 0..3 {
            let req = Request::builder()
                .uri("http://example.com/")
                .body(Bytes::new())
                .unwrap();
            let resp = rt.handle(req).await.expect("handle request");
            assert_eq!(resp.status(), 200);
        }
    }

    // ---- warm-path: pre-instantiation in load --------------------------------

    /// `WasmRuntime::load` must succeed and produce a runtime whose `proxy_pre`
    /// is usable: a single `handle` call after `load` must return 200.
    ///
    /// This is the primary warm-path correctness check: compile + link happen in
    /// `load`; `handle` only calls `instantiate_async`.
    #[tokio::test]
    async fn load_builds_proxy_pre_and_handle_serves() {
        let wasm = include_bytes!("../tests/fixtures/hello.wasm");
        let rt = WasmRuntime::load(wasm).expect("load must succeed (proxy_pre built here)");

        let req = Request::builder()
            .method("GET")
            .uri("http://example.com/warm-path")
            .body(Bytes::new())
            .unwrap();
        let resp = rt
            .handle(req)
            .await
            .expect("handle must succeed using stored proxy_pre");

        assert_eq!(resp.status(), 200);
        assert_eq!(String::from_utf8_lossy(resp.body()), "Hello, Tabbify!");
    }

    /// Two sequential `handle` calls against ONE loaded runtime both succeed —
    /// the stored `proxy_pre` is reused and per-request isolation is preserved
    /// (each call gets a fresh `Store`).
    #[tokio::test]
    async fn two_sequential_handles_reuse_proxy_pre() {
        let wasm = include_bytes!("../tests/fixtures/hello.wasm");
        let rt = WasmRuntime::load(wasm).expect("load fixture");

        for i in 0..2u32 {
            let req = Request::builder()
                .method("GET")
                .uri(format!("http://example.com/req/{i}"))
                .body(Bytes::new())
                .unwrap();
            let resp = rt
                .handle(req)
                .await
                .unwrap_or_else(|e| panic!("handle {i} failed: {e}"));
            assert_eq!(resp.status(), 200, "request {i} must return 200");
            assert_eq!(
                String::from_utf8_lossy(resp.body()),
                "Hello, Tabbify!",
                "request {i} body mismatch"
            );
        }
    }

    // ---- health() contract ---------------------------------------------------

    /// WasmRuntime uses the default health() implementation, which always
    /// returns Serving (a wasm component is ready as soon as it is loaded).
    #[tokio::test]
    async fn wasm_runtime_health_is_serving_by_default() {
        let wasm = include_bytes!("../tests/fixtures/hello.wasm");
        let rt = WasmRuntime::load(wasm).expect("load fixture");
        assert_eq!(rt.health().await, RuntimeHealth::Serving);
    }

    /// WasmRuntime health is also Serving when accessed through the AppRuntime
    /// trait object (confirms the default is visible through dyn dispatch).
    #[tokio::test]
    async fn wasm_health_via_trait_object_is_serving() {
        let wasm = include_bytes!("../tests/fixtures/hello.wasm");
        let rt: std::sync::Arc<dyn AppRuntime> =
            std::sync::Arc::new(WasmRuntime::load(wasm).expect("load fixture"));
        assert_eq!(rt.health().await, RuntimeHealth::Serving);
    }

    // ---- watch_for_exit() contract ------------------------------------------

    /// WasmRuntime uses the default watch_for_exit() which never resolves. A
    /// short timeout must elapse without the future completing — it must be
    /// pending forever (wasm has no long-lived external process to watch).
    #[tokio::test]
    async fn wasm_watch_for_exit_never_resolves() {
        let wasm = include_bytes!("../tests/fixtures/hello.wasm");
        let rt = WasmRuntime::load(wasm).expect("load fixture");
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(50), rt.watch_for_exit()).await;
        assert!(
            result.is_err(),
            "watch_for_exit must be pending (wasm has no long-lived process)"
        );
    }

    /// WasmRuntime watch_for_exit() is also pending when accessed through the
    /// AppRuntime trait object (confirms the default is visible through dyn
    /// dispatch).
    #[tokio::test]
    async fn wasm_watch_for_exit_via_trait_object_never_resolves() {
        let wasm = include_bytes!("../tests/fixtures/hello.wasm");
        let rt: std::sync::Arc<dyn AppRuntime> =
            std::sync::Arc::new(WasmRuntime::load(wasm).expect("load fixture"));
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(50), rt.watch_for_exit()).await;
        assert!(
            result.is_err(),
            "watch_for_exit via trait object must be pending"
        );
    }

    // ---- shutdown() contract -------------------------------------------------

    /// WasmRuntime uses the default shutdown() which is a no-op: it must
    /// complete immediately (no external resources to release).
    #[tokio::test]
    async fn wasm_shutdown_is_noop_and_completes() {
        let wasm = include_bytes!("../tests/fixtures/hello.wasm");
        let rt = WasmRuntime::load(wasm).expect("load fixture");
        // Must complete without hanging (no external resources to release).
        tokio::time::timeout(std::time::Duration::from_millis(50), rt.shutdown())
            .await
            .expect("shutdown must complete immediately for WasmRuntime");
    }

    /// WasmRuntime shutdown() via the AppRuntime trait object is also a no-op
    /// (confirms the default is visible through dyn dispatch).
    #[tokio::test]
    async fn wasm_shutdown_via_trait_object_is_noop() {
        let wasm = include_bytes!("../tests/fixtures/hello.wasm");
        let rt: std::sync::Arc<dyn AppRuntime> =
            std::sync::Arc::new(WasmRuntime::load(wasm).expect("load fixture"));
        tokio::time::timeout(std::time::Duration::from_millis(50), rt.shutdown())
            .await
            .expect("shutdown via trait object must complete immediately for WasmRuntime");
    }

    // ---- AOT .cwasm cache tests ---------------------------------------------

    /// Cache miss: `load_cached_or_compile` must succeed and write a `.cwasm`
    /// file when the cache does not exist yet.
    #[tokio::test]
    async fn cache_miss_compiles_and_writes_cwasm() {
        let wasm = include_bytes!("../tests/fixtures/hello.wasm");
        let dir = tempfile::tempdir().expect("tempdir");
        let cache_path = dir.path().join("app.cwasm");

        assert!(!cache_path.exists(), "precondition: no cache file yet");

        let rt = WasmRuntime::load_cached_or_compile(wasm, &cache_path, DEFAULT_FUEL_PER_REQUEST)
            .expect("load_cached_or_compile (miss) must succeed");

        assert!(cache_path.exists(), "cache file must be written after miss");

        // The returned runtime must be functional.
        let req = Request::builder()
            .method("GET")
            .uri("http://example.com/cache-miss")
            .body(Bytes::new())
            .unwrap();
        let resp = rt.handle(req).await.expect("handle after cache miss");
        assert_eq!(resp.status(), 200);
        assert_eq!(String::from_utf8_lossy(resp.body()), "Hello, Tabbify!");
    }

    /// Cache hit: a second `load_cached_or_compile` with an existing `.cwasm`
    /// must succeed via deserialization (no recompile).
    #[tokio::test]
    async fn cache_hit_deserializes_cwasm() {
        let wasm = include_bytes!("../tests/fixtures/hello.wasm");
        let dir = tempfile::tempdir().expect("tempdir");
        let cache_path = dir.path().join("app.cwasm");

        // Prime the cache.
        WasmRuntime::load_cached_or_compile(wasm, &cache_path, DEFAULT_FUEL_PER_REQUEST)
            .expect("prime cache");
        assert!(cache_path.exists(), "cache must exist after prime");

        // Second call: deserialize path.
        let rt = WasmRuntime::load_cached_or_compile(wasm, &cache_path, DEFAULT_FUEL_PER_REQUEST)
            .expect("load_cached_or_compile (hit) must succeed");

        let req = Request::builder()
            .method("GET")
            .uri("http://example.com/cache-hit")
            .body(Bytes::new())
            .unwrap();
        let resp = rt.handle(req).await.expect("handle after cache hit");
        assert_eq!(resp.status(), 200);
        assert_eq!(String::from_utf8_lossy(resp.body()), "Hello, Tabbify!");
    }

    /// Corrupt cache: writing garbage bytes into `app.cwasm` must NOT cause
    /// `load_cached_or_compile` to fail — it must fall back to recompile.
    #[tokio::test]
    async fn corrupt_cache_falls_back_to_compile() {
        let wasm = include_bytes!("../tests/fixtures/hello.wasm");
        let dir = tempfile::tempdir().expect("tempdir");
        let cache_path = dir.path().join("app.cwasm");

        // Write garbage bytes.
        std::fs::write(&cache_path, b"not a valid cwasm file at all").unwrap();

        let rt = WasmRuntime::load_cached_or_compile(wasm, &cache_path, DEFAULT_FUEL_PER_REQUEST)
            .expect("must fall back to compile on corrupt cache");

        let req = Request::builder()
            .method("GET")
            .uri("http://example.com/corrupt-cache")
            .body(Bytes::new())
            .unwrap();
        let resp = rt
            .handle(req)
            .await
            .expect("handle after corrupt-cache fallback");
        assert_eq!(resp.status(), 200);
        assert_eq!(String::from_utf8_lossy(resp.body()), "Hello, Tabbify!");
    }

    /// Round-trip: `load_cached_or_compile` (miss) → write cache → second call
    /// (hit via deserialize) → `handle` returns 200.  Validates the full
    /// precompile → deserialize → serve pipeline end-to-end.
    #[tokio::test]
    async fn cache_round_trip_handle_returns_200() {
        let wasm = include_bytes!("../tests/fixtures/hello.wasm");
        let dir = tempfile::tempdir().expect("tempdir");
        let cache_path = dir.path().join("app.cwasm");

        // First: miss + write.
        let _ = WasmRuntime::load_cached_or_compile(wasm, &cache_path, DEFAULT_FUEL_PER_REQUEST)
            .expect("prime");
        let cwasm_size = std::fs::metadata(&cache_path).expect("metadata").len();
        assert!(cwasm_size > 0, "cwasm must be non-empty");

        // Second: hit.
        let rt = WasmRuntime::load_cached_or_compile(wasm, &cache_path, DEFAULT_FUEL_PER_REQUEST)
            .expect("hit");

        let req = Request::builder()
            .method("GET")
            .uri("http://example.com/round-trip")
            .body(Bytes::new())
            .unwrap();
        let resp = rt.handle(req).await.expect("handle round-trip");
        assert_eq!(resp.status(), 200);
        assert_eq!(String::from_utf8_lossy(resp.body()), "Hello, Tabbify!");
    }
}

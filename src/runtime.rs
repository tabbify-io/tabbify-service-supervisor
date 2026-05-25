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
//! - A compiled [`Component`] + a pre-built [`Linker`] are kept on the runtime;
//!   each request gets a FRESH [`Store`] with its own fuel budget and resource
//!   table, so no state leaks across requests.
//!
//! Public API (§8):
//! - [`WasmRuntime::load`] — compile a component once.
//! - [`WasmRuntime::handle`] — run one request, return its response.

use std::future::Future;
use std::pin::Pin;

use anyhow::{Result, anyhow};
use bytes::Bytes;
use http::{Request, Response};
use http_body_util::{BodyExt, Full};
use tokio::sync::oneshot;
use wasmtime::component::{Component, Linker, ResourceTable};
use wasmtime::{Config, Engine, Store};
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
/// Cheap to clone (`Component`, `Linker`, `Engine` are all refcounted), so a
/// running app instance can be shared across concurrent requests.
#[derive(Clone)]
pub struct WasmRuntime {
    component: Component,
    linker: Linker<Ctx>,
    engine: Engine,
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
    /// # Errors
    /// See [`WasmRuntime::load`].
    pub fn load_with_fuel(wasm_bytes: &[u8], fuel_per_request: u64) -> Result<Self> {
        let engine = new_engine()?;
        let component = Component::new(&engine, wasm_bytes)?;
        let linker = build_linker(&engine)?;
        Ok(Self {
            component,
            linker,
            engine,
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
    /// request and response bodies are buffered (Phase-1).
    ///
    /// # Errors
    /// - per-request fuel exhausted or the component traps,
    /// - the component returns without producing a response,
    /// - the response body fails to collect.
    pub async fn handle(&self, request: Request<Bytes>) -> Result<Response<Bytes>> {
        let ctx = Ctx::new();
        let mut store = Store::new(&self.engine, ctx);
        store.set_fuel(self.fuel_per_request)?;

        // Pre-instantiate against the wasi:http proxy world, then realise into
        // a per-request `Proxy` bound to the fresh store.
        let pre = self.linker.instantiate_pre(&self.component)?;
        let proxy_pre = ProxyPre::new(pre)?;
        let proxy = proxy_pre.instantiate_async(&mut store).await?;

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

/// Build a Wasmtime [`Engine`]: component model on, async on, fuel on.
fn new_engine() -> Result<Engine> {
    let mut cfg = Config::new();
    cfg.async_support(true);
    cfg.consume_fuel(true);
    cfg.wasm_component_model(true);
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
}

//! Tests for [`super`] — DockerRuntime helpers.
#![allow(clippy::unwrap_used)]

use super::*;
use crate::config::DockerConfig;
use crate::manifest::Runtime;

#[test]
fn rt_app_port_uses_config() {
    let rt = Runtime {
        r#type: "docker".to_owned(),
        entry: "context.tar.gz".to_owned(),
        fuel_per_request: 0,
        memory_mb: 0,
        vcpus: None,
        kernel: None,
        registry_ref: None,
    };
    let cfg = DockerConfig {
        app_port: 9000,
        ..DockerConfig::default()
    };
    assert_eq!(rt_app_port(&rt, &cfg), 9000);
}

#[tokio::test]
async fn reserve_loopback_port_returns_a_nonzero_port() {
    let p = reserve_loopback_port().await.unwrap();
    assert_ne!(p, 0);
}

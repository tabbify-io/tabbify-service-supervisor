//! Runner-scoped OCI registry authentication.

#[cfg(test)]
use std::path::Path;

use anyhow::{Context, Result, bail};

use crate::runner::build::firecracker::FcBuildRunner;

/// Docker-format registry auth config owned for one runner process lifetime.
///
/// The token is retained so the config can be RE-SCOPED when the platform
/// registry moves to a new coordinator-allocated mesh address: a deploy ref on
/// a new in-mesh host extends the auth config instead of failing the deploy.
/// The token already lives in this process's env and on disk in `config.json`,
/// so retention adds no new exposure. Dropping the config removes the
/// directory and file.
pub(crate) struct RegistryConfig {
    dir: tempfile::TempDir,
    file: String,
    token: String,
    /// Hosts currently written into `config.json`. Seeded with the spawn-time
    /// host; grows when a deploy ref names a new mesh-ULA host.
    hosts: std::sync::Mutex<Vec<String>>,
}

impl RegistryConfig {
    /// Create an authenticated config for the registry host in `reff`.
    pub(crate) fn new(token: &str, reff: &str) -> Result<Self> {
        let normalized_ref = crate::oras::lowercase_oci_repo(reff);
        let host = registry_host_from_ref(&normalized_ref);
        if host.is_empty() {
            bail!("OCI ref has no registry host: {reff:?}");
        }
        let dir = tempfile::Builder::new()
            .prefix("tabbify-runner-registry-")
            .tempdir()
            .context("create runner registry auth directory")?;
        crate::skopeo::write_registry_config(token, host, dir.path())
            .context("write runner registry auth config")?;
        let file = dir
            .path()
            .join("config.json")
            .to_string_lossy()
            .into_owned();
        Ok(Self {
            dir,
            file,
            token: token.to_owned(),
            hosts: std::sync::Mutex::new(vec![host.to_owned()]),
        })
    }

    /// Return the config path for `reff`, re-scoping to its host when needed.
    ///
    /// The registry's mesh address is coordinator-allocated and can change
    /// during this runner's lifetime. Refs are built by the node from the
    /// CURRENT roster address and arrive over the authenticated supervisor
    /// control channel, so a new host is trusted — but only inside the mesh: a
    /// non-ULA host still fails closed, so the runner token can never be
    /// pointed at an arbitrary external registry.
    pub(crate) fn file_for_ref(&self, reff: &str) -> Result<&str> {
        let normalized_ref = crate::oras::lowercase_oci_repo(reff);
        let host = registry_host_from_ref(&normalized_ref);
        let mut hosts = self
            .hosts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !hosts.iter().any(|known| known == host) {
            if !is_mesh_ula_host(host) {
                bail!(
                    "runner registry auth is scoped to mesh registries {:?}; \
                     refusing non-mesh host {:?}",
                    *hosts,
                    host
                );
            }
            let mut next: Vec<&str> = hosts.iter().map(String::as_str).collect();
            next.push(host);
            crate::skopeo::write_registry_config_hosts(&self.token, &next, self.dir.path())
                .context("re-scope runner registry auth config")?;
            tracing::warn!(
                new_host = host,
                known_hosts = ?*hosts,
                "registry auth re-scoped: deploy ref names a new mesh registry host"
            );
            hosts.push(host.to_owned());
        }
        Ok(&self.file)
    }

    #[cfg(test)]
    fn path(&self) -> &Path {
        Path::new(&self.file)
    }
}

/// Resolve an OCI digest using the runner's registry authentication when set.
pub(crate) async fn resolve_oci_digest(
    reff: &str,
    runner: &FcBuildRunner,
    registry: Option<&RegistryConfig>,
) -> Result<String> {
    let normalized_ref = crate::oras::lowercase_oci_repo(reff);
    let config = registry
        .map(|config| config.file_for_ref(&normalized_ref))
        .transpose()?;
    crate::runner::build::firecracker::resolve_oci_digest(&normalized_ref, runner, config).await
}

fn registry_host_from_ref(reff: &str) -> &str {
    reff.split('/').next().unwrap_or(reff)
}

/// True when `host` is a bracketed mesh-ULA authority (`[fc00::/7]:port`).
/// Re-scoping is limited to these so a runner token cannot be redirected at an
/// external registry.
fn is_mesh_ula_host(host: &str) -> bool {
    let Some(rest) = host.strip_prefix('[') else {
        return false;
    };
    let Some((addr, _port)) = rest.split_once(']') else {
        return false;
    };
    addr.parse::<std::net::Ipv6Addr>()
        .is_ok_and(|ip| ip.segments()[0] & 0xfe00 == 0xfc00)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::runtime::BoxFut;

    #[tokio::test]
    async fn authenticated_resolve_uses_runner_scoped_config_without_token_in_argv() {
        const TOKEN: &str = "runner-secret-token";
        const REFF: &str = "registry.example:5000/acme/app:main";
        const DIGEST: &str =
            "sha256:1111111111111111111111111111111111111111111111111111111111111111";
        let config = RegistryConfig::new(TOKEN, REFF).unwrap();
        let config_path = config.path().to_string_lossy().into_owned();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let calls_for_runner = calls.clone();
        let runner: FcBuildRunner = Arc::new(move |argv| {
            calls_for_runner.lock().unwrap().push(argv);
            let future: BoxFut<'static, (bool, Vec<u8>)> =
                Box::pin(async { (true, format!("{DIGEST}\n").into_bytes()) });
            future
        });

        let resolved = resolve_oci_digest(REFF, &runner, Some(&config))
            .await
            .unwrap();

        assert_eq!(resolved, DIGEST);
        let argv = &calls.lock().unwrap()[0];
        assert!(
            argv.windows(2)
                .any(|args| args[0] == "--registry-config" && args[1] == config_path)
        );
        assert!(!argv.iter().any(|arg| arg.contains(TOKEN)));
    }

    #[test]
    fn config_is_removed_at_end_of_runner_scope_and_never_crosses_registry_hosts() {
        let path = {
            let config =
                RegistryConfig::new("runner-secret-token", "registry.example:5000/acme/app:main")
                    .unwrap();
            let path = config.path().to_owned();
            assert!(path.is_file());
            assert!(
                config
                    .file_for_ref("other.example:5000/acme/app:main")
                    .is_err()
            );
            path
        };
        assert!(!path.exists());
    }

    /// The mesh registry can be re-homed to a new coordinator-allocated ULA
    /// mid-runner-lifetime. A deploy ref on the new address must keep working:
    /// freezing the spawn-time host broke every in-place redeploy on 2026-07-21.
    #[test]
    fn re_scopes_to_a_new_mesh_registry_and_keeps_the_old_host_authenticated() {
        const TOKEN: &str = "runner-secret-token";
        let config = RegistryConfig::new(TOKEN, "[fd5a:1f00:0:3::1]:5000/acme/app:main").unwrap();

        config
            .file_for_ref("[fd5a:1f00:4ed8:16::1]:5000/acme/app:main")
            .expect("a new in-mesh registry host is accepted");

        let written = std::fs::read_to_string(config.path()).unwrap();
        assert!(written.contains("[fd5a:1f00:0:3::1]:5000"), "{written}");
        assert!(written.contains("[fd5a:1f00:4ed8:16::1]:5000"), "{written}");
        // Still one shared token, never leaked in cleartext.
        assert!(!written.contains(TOKEN), "{written}");
        // The original host keeps resolving after the re-scope.
        config
            .file_for_ref("[fd5a:1f00:0:3::1]:5000/acme/app:main")
            .unwrap();
    }

    /// Re-scoping is deliberately limited to the mesh: a runner token must
    /// never become usable against an arbitrary external registry.
    #[test]
    fn refuses_to_re_scope_to_a_non_mesh_host() {
        let config = RegistryConfig::new(
            "runner-secret-token",
            "[fd5a:1f00:0:3::1]:5000/acme/app:main",
        )
        .unwrap();

        for host in [
            "evil.example:5000/acme/app:main",
            "[2001:db8::1]:5000/acme/app:main",
            "[not-an-ip]:5000/acme/app:main",
        ] {
            assert!(config.file_for_ref(host).is_err(), "{host} must be refused");
        }
    }
}

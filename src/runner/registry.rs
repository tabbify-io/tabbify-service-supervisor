//! Runner-scoped OCI registry authentication.

#[cfg(test)]
use std::path::Path;

use anyhow::{Context, Result, bail};

use crate::runner::build::firecracker::FcBuildRunner;

/// Docker-format registry auth config owned for one runner process lifetime.
///
/// The token is written once into a private temporary directory and is never
/// retained in this value. Dropping the config removes the directory and file.
pub(crate) struct RegistryConfig {
    _dir: tempfile::TempDir,
    file: String,
    host: String,
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
            _dir: dir,
            file,
            host: host.to_owned(),
        })
    }

    /// Return the config path only for refs on this config's registry.
    pub(crate) fn file_for_ref(&self, reff: &str) -> Result<&str> {
        let normalized_ref = crate::oras::lowercase_oci_repo(reff);
        let host = registry_host_from_ref(&normalized_ref);
        if host != self.host {
            bail!(
                "runner registry auth is scoped to {:?}, not {:?}",
                self.host,
                host
            );
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::runtime::BoxFut;

    use super::*;

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
}

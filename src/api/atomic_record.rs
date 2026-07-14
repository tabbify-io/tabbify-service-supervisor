//! Crash-safe persistence for API sidecar records.

use std::fs::{self, File};
use std::io::{self, Write as _};
use std::path::Path;

use serde::Serialize;

pub(super) fn save_json<T: Serialize>(dir: &Path, path: &Path, value: &T) -> io::Result<()> {
    let json = serde_json::to_vec_pretty(value)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    fs::create_dir_all(dir)?;
    atomic_replace(path, &json)
}

fn atomic_replace(path: &Path, contents: &[u8]) -> io::Result<()> {
    atomic_replace_with(path, contents, |temp, target| {
        temp.persist(target)
            .map(|_| ())
            .map_err(|error| error.error)
    })?;
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "record path has no parent"))?;
    File::open(parent)?.sync_all()
}

fn atomic_replace_with(
    path: &Path,
    contents: &[u8],
    persist: impl FnOnce(tempfile::NamedTempFile, &Path) -> io::Result<()>,
) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "record path has no parent"))?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        temp.as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    temp.as_file_mut().write_all(contents)?;
    temp.as_file_mut().sync_all()?;
    persist(temp, path)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn atomic_record_is_mode_0600() {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("record.json");
        atomic_replace(&path, b"secret").unwrap();

        assert_eq!(
            fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn failed_persist_preserves_existing_record() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("record.json");
        fs::write(&path, b"old-generation").unwrap();

        let error = atomic_replace_with(&path, b"new-generation", |_temp, _target| {
            Err(io::Error::other("injected persist failure"))
        })
        .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(fs::read(path).unwrap(), b"old-generation");
    }
}

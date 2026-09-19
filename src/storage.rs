use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail};
use filetime::FileTime;
use sha2::{Digest, Sha256};
use tokio::{fs, io::AsyncWriteExt};

#[derive(Clone, Debug)]
pub struct Storage {
    root: PathBuf,
}

impl Storage {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        if !root.is_absolute() {
            bail!("storage_dir must be absolute");
        }
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn server_dir(&self, server: &str) -> Result<PathBuf> {
        validate_component(server)?;
        Ok(self.root.join(server))
    }

    pub fn artifact_path(
        &self,
        server: &str,
        namespace: &str,
        collection: &str,
        version: &str,
    ) -> Result<PathBuf> {
        for value in [server, namespace, collection, version] {
            validate_component(value)?;
        }
        Ok(self
            .root
            .join(server)
            .join(namespace)
            .join(collection)
            .join(format!("{namespace}-{collection}-{version}.tar.gz")))
    }

    pub async fn is_disabled(&self, server: &str) -> Result<Option<String>> {
        let path = self.server_dir(server)?.join(".disable");
        let metadata = match fs::symlink_metadata(&path).await {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error).with_context(|| format!("inspect {}", path.display())),
        };
        if !metadata.file_type().is_file() {
            bail!("{} must be a regular file", path.display());
        }
        if metadata.len() > 4096 {
            bail!("{} exceeds 4 KiB", path.display());
        }
        Ok(Some(fs::read_to_string(path).await?.trim().to_owned()))
    }
}

pub fn validate_component(value: &str) -> Result<()> {
    if value.is_empty() || value == "." || value == ".." || value.contains(['/', '\\', '\0']) {
        bail!("unsafe path component: {value:?}");
    }
    let mut components = Path::new(value).components();
    if !matches!(components.next(), Some(Component::Normal(_))) || components.next().is_some() {
        bail!("unsafe path component: {value:?}");
    }
    Ok(())
}

pub async fn sha256_file(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path).await?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0; 128 * 1024];
    loop {
        let read = tokio::io::AsyncReadExt::read(&mut file, &mut buffer).await?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(hex::encode(digest.finalize()))
}

pub async fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().context("destination has no parent")?;
    fs::create_dir_all(parent).await?;
    let tmp = temporary_path(path);
    let result = async {
        let mut file = fs::File::create(&tmp).await?;
        file.write_all(bytes).await?;
        file.flush().await?;
        file.sync_all().await?;
        drop(file);
        fs::rename(&tmp, path).await?;
        sync_parent_directory(parent).await
    }
    .await;

    if result.is_err() {
        let _ = remove_file_if_exists(&tmp).await;
    }
    result
}

pub async fn publish_download(tmp: &Path, final_path: &Path, created_unix: i64) -> Result<()> {
    let parent = final_path.parent().context("destination has no parent")?;
    let tmp_path = tmp.to_owned();
    tokio::task::spawn_blocking(move || {
        filetime::set_file_mtime(&tmp_path, FileTime::from_unix_time(created_unix, 0))?;
        std::fs::File::open(tmp_path)?.sync_all()
    })
    .await??;

    // Set and persist metadata before rename so that the destination is never
    // observable as a complete artifact with incomplete metadata.
    fs::rename(tmp, final_path).await?;
    sync_parent_directory(parent).await?;
    Ok(())
}

/// Return the temporary file name used while atomically creating `path`.
pub fn temporary_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(".tmp");
    PathBuf::from(value)
}

/// Remove a file, treating an already absent file as success.
pub async fn remove_file_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("remove {}", path.display())),
    }
}

/// Remove regular `*.tmp` files below `root` without following symlinks.
///
/// This is intended for startup recovery after an interrupted write. The
/// returned value is the number of files removed.
pub async fn cleanup_tmp_files(root: &Path) -> Result<u64> {
    let mut pending = vec![root.to_owned()];
    let mut removed = 0;

    while let Some(directory) = pending.pop() {
        let mut entries = match fs::read_dir(&directory).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(error).with_context(|| format!("read {}", directory.display()));
            }
        };
        while let Some(entry) = entries.next_entry().await? {
            let file_type = entry.file_type().await?;
            if file_type.is_dir() {
                pending.push(entry.path());
            } else if file_type.is_file() && entry.file_name().to_string_lossy().ends_with(".tmp") {
                remove_file_if_exists(&entry.path()).await?;
                removed += 1;
            }
        }
    }
    Ok(removed)
}

#[cfg(unix)]
async fn sync_parent_directory(path: &Path) -> Result<()> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        match std::fs::File::open(path).and_then(|file| file.sync_all()) {
            Ok(()) => Ok(()),
            // macOS and some filesystems do not permit fsync on directories.
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::PermissionDenied
                        | std::io::ErrorKind::InvalidInput
                        | std::io::ErrorKind::Unsupported
                ) =>
            {
                Ok(())
            }
            Err(error) => Err(error),
        }
    })
    .await??;
    Ok(())
}

#[cfg(not(unix))]
async fn sync_parent_directory(_path: &Path) -> Result<()> {
    // Some supported platforms cannot open directories as files. The file
    // itself is still synced before the atomic rename.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_traversal() {
        for value in ["", ".", "..", "a/b", "a\\b", "a\0b"] {
            assert!(validate_component(value).is_err());
        }
        assert!(validate_component("community").is_ok());
    }

    #[tokio::test]
    async fn atomic_write_and_hash() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x");
        atomic_write(&path, b"hello").await.unwrap();
        assert_eq!(
            sha256_file(&path).await.unwrap(),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[tokio::test]
    async fn publish_sets_mtime_before_exposing_file() {
        let dir = tempfile::tempdir().unwrap();
        let final_path = dir.path().join("artifact.tar.gz");
        let tmp = temporary_path(&final_path);
        fs::write(&tmp, b"artifact").await.unwrap();

        publish_download(&tmp, &final_path, 1_600_000_000)
            .await
            .unwrap();

        assert!(!tmp.exists());
        assert_eq!(fs::read(&final_path).await.unwrap(), b"artifact");
        let metadata = fs::metadata(&final_path).await.unwrap();
        assert_eq!(
            FileTime::from_last_modification_time(&metadata).unix_seconds(),
            1_600_000_000
        );
    }

    #[tokio::test]
    async fn cleanup_removes_only_regular_tmp_files() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("server/namespace");
        fs::create_dir_all(&nested).await.unwrap();
        fs::write(nested.join("artifact.tar.gz.tmp"), b"partial")
            .await
            .unwrap();
        fs::write(nested.join("artifact.tar.gz"), b"complete")
            .await
            .unwrap();
        fs::create_dir(nested.join("not-a-file.tmp")).await.unwrap();

        assert_eq!(cleanup_tmp_files(dir.path()).await.unwrap(), 1);
        assert!(!nested.join("artifact.tar.gz.tmp").exists());
        assert!(nested.join("artifact.tar.gz").exists());
        assert!(nested.join("not-a-file.tmp").exists());
    }

    #[tokio::test]
    async fn cleanup_of_missing_root_is_a_noop() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            cleanup_tmp_files(&dir.path().join("missing"))
                .await
                .unwrap(),
            0
        );
    }
}

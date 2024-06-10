// todo!(remove)
#![allow(unused)]

use crate::SshSession;
use anyhow::{anyhow, Result};
use async_tar::Archive;
use fs::{CopyOptions, CreateOptions, Fs, Metadata, RemoveOptions, RenameOptions, Watcher};
use futures::{stream, AsyncRead, Stream};
use git::repository::GitRepository;
use rpc::proto::{self, envelope::Payload};
use smol::stream::StreamExt;
use std::{
    io,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime},
};
use text::{LineEnding, Rope};

pub struct RemoteFs {
    session: SshSession,
}

struct RemoteWatcher {}

impl RemoteFs {
    pub fn new(session: SshSession) -> Self {
        Self { session }
    }
}

#[async_trait::async_trait]
impl Fs for RemoteFs {
    async fn create_dir(&self, path: &Path) -> Result<()> {
        Err(anyhow!("not implemented"))
    }

    async fn create_symlink(&self, path: &Path, target: PathBuf) -> Result<()> {
        Err(anyhow!("not implemented"))
    }

    async fn create_file(&self, path: &Path, options: CreateOptions) -> Result<()> {
        Err(anyhow!("not implemented"))
    }

    async fn create_file_with(
        &self,
        path: &Path,
        content: Pin<&mut (dyn AsyncRead + Send)>,
    ) -> Result<()> {
        Err(anyhow!("not implemented"))
    }

    async fn extract_tar_file(
        &self,
        path: &Path,
        content: Archive<Pin<&mut (dyn AsyncRead + Send)>>,
    ) -> Result<()> {
        Err(anyhow!("not implemented"))
    }

    async fn copy_file(&self, source: &Path, target: &Path, options: CopyOptions) -> Result<()> {
        Err(anyhow!("not implemented"))
    }

    async fn rename(&self, source: &Path, target: &Path, options: RenameOptions) -> Result<()> {
        Err(anyhow!("not implemented"))
    }

    async fn remove_dir(&self, path: &Path, options: RemoveOptions) -> Result<()> {
        Err(anyhow!("not implemented"))
    }

    async fn remove_file(&self, path: &Path, options: RemoveOptions) -> Result<()> {
        Err(anyhow!("not implemented"))
    }

    async fn trash_file(&self, path: &Path, _options: RemoveOptions) -> Result<()> {
        Err(anyhow!("not implemented"))
    }

    async fn trash_dir(&self, path: &Path, options: RemoveOptions) -> Result<()> {
        Err(anyhow!("not implemented"))
    }

    async fn open_sync(&self, path: &Path) -> Result<Box<dyn io::Read>> {
        Err(anyhow!("not implemented"))
    }

    async fn load(&self, path: &Path) -> Result<String> {
        Ok(self
            .session
            .request(proto::ReadFile {
                path: path.to_string_lossy().to_string(),
            })
            .await?
            .content)
    }

    async fn atomic_write(&self, path: PathBuf, data: String) -> Result<()> {
        Err(anyhow!("not implemented"))
    }

    async fn save(&self, path: &Path, text: &Rope, line_ending: LineEnding) -> Result<()> {
        let response = self
            .session
            .request(proto::WriteFile {
                path: path.to_string_lossy().to_string(),
                content: text.to_string(),
                line_ending: match line_ending {
                    LineEnding::Unix => proto::write_file::LineEnding::Unix as i32,
                    LineEnding::Windows => proto::write_file::LineEnding::Windows as i32,
                },
            })
            .await?;
        Ok(())
    }

    async fn canonicalize(&self, path: &Path) -> Result<PathBuf> {
        Ok(self
            .session
            .request(proto::Canonicalize {
                path: path.to_string_lossy().to_string(),
            })
            .await?
            .path
            .into())
    }

    async fn is_file(&self, path: &Path) -> bool {
        false
    }

    async fn is_dir(&self, path: &Path) -> bool {
        false
    }

    async fn metadata(&self, path: &Path) -> Result<Option<Metadata>> {
        let metadata = self
            .session
            .request(proto::Stat {
                path: path.to_string_lossy().to_string(),
            })
            .await?;
        Ok(Some(Metadata {
            inode: metadata.inode,
            mtime: SystemTime::UNIX_EPOCH + Duration::from_millis(metadata.mtime),
            is_symlink: metadata.is_symlink,
            is_dir: metadata.is_dir,
        }))
    }

    async fn read_link(&self, path: &Path) -> Result<PathBuf> {
        Ok(self
            .session
            .request(proto::ReadLink {
                path: path.to_string_lossy().to_string(),
            })
            .await?
            .path
            .into())
    }

    async fn read_dir(
        &self,
        path: &Path,
    ) -> Result<Pin<Box<dyn Send + Stream<Item = Result<PathBuf>>>>> {
        let response = self
            .session
            .request(proto::ReadDir {
                path: path.to_string_lossy().to_string(),
            })
            .await?;
        Ok(stream::iter(response.paths.into_iter().map(|path| Ok(path.into()))).boxed())
    }

    async fn watch(
        &self,
        path: &Path,
        latency: Duration,
    ) -> (
        Pin<Box<dyn Send + Stream<Item = Vec<PathBuf>>>>,
        Arc<(dyn Watcher + 'static)>,
    ) {
        (stream::pending().boxed(), Arc::new(RemoteWatcher {}))
    }

    fn open_repo(&self, dotgit_path: &Path) -> Option<Arc<dyn GitRepository>> {
        None
    }

    fn is_fake(&self) -> bool {
        false
    }

    async fn is_case_sensitive(&self) -> Result<bool> {
        Ok(false)
    }
}

impl Watcher for RemoteWatcher {
    fn add(&self, _path: &Path) -> Result<()> {
        Err(anyhow!("not implemented"))
    }

    fn remove(&self, _path: &Path) -> Result<()> {
        Err(anyhow!("not implemented"))
    }
}

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::bail;
use log::{error, info};
use tokio::fs;
use tokio::task::spawn_blocking;
use tokio::time::sleep;

use super::update_actor::UpdateActorHandle;
use super::uuid_resolver::UuidResolverHandle;
use crate::helpers::compression;

pub struct SnapshotService<U, R> {
    uuid_resolver_handle: R,
    update_handle: U,
    snapshot_period: Duration,
    snapshot_path: PathBuf,
    db_name: String,
}

impl<U, R> SnapshotService<U, R>
where
    U: UpdateActorHandle,
    R: UuidResolverHandle,
{
    pub fn new(
        uuid_resolver_handle: R,
        update_handle: U,
        snapshot_period: Duration,
        snapshot_path: PathBuf,
        db_name: String,
    ) -> Self {
        Self {
            uuid_resolver_handle,
            update_handle,
            snapshot_period,
            snapshot_path,
            db_name,
        }
    }

    pub async fn run(self) {
        info!(
            "Snapshot scheduled every {}s.",
            self.snapshot_period.as_secs()
        );
        loop {
            if let Err(e) = self.perform_snapshot().await {
                error!("{}", e);
            }
            sleep(self.snapshot_period).await;
        }
    }

    async fn perform_snapshot(&self) -> anyhow::Result<()> {
        info!("Performing snapshot.");

        let snapshot_dir = self.snapshot_path.clone();
        fs::create_dir_all(&snapshot_dir).await?;
        let temp_snapshot_dir =
            spawn_blocking(move || tempfile::tempdir_in(snapshot_dir)).await??;
        let temp_snapshot_path = temp_snapshot_dir.path().to_owned();

        let uuids = self
            .uuid_resolver_handle
            .snapshot(temp_snapshot_path.clone())
            .await?;

        if uuids.is_empty() {
            return Ok(());
        }

        let tasks = uuids
            .iter()
            .map(|&uuid| {
                self.update_handle
                    .snapshot(uuid, temp_snapshot_path.clone())
            })
            .collect::<Vec<_>>();

        futures::future::try_join_all(tasks).await?;

        let snapshot_dir = self.snapshot_path.clone();
        let snapshot_path = self
            .snapshot_path
            .join(format!("{}.snapshot", self.db_name));
        let snapshot_path = spawn_blocking(move || -> anyhow::Result<PathBuf> {
            let temp_snapshot_file = tempfile::NamedTempFile::new_in(snapshot_dir)?;
            let temp_snapshot_file_path = temp_snapshot_file.path().to_owned();
            compression::to_tar_gz(temp_snapshot_path, temp_snapshot_file_path)?;
            temp_snapshot_file.persist(&snapshot_path)?;
            Ok(snapshot_path)
        })
        .await??;

        info!("Created snapshot in {:?}.", snapshot_path);

        Ok(())
    }
}

pub fn load_snapshot(
    db_path: impl AsRef<Path>,
    snapshot_path: impl AsRef<Path>,
    ignore_snapshot_if_db_exists: bool,
    ignore_missing_snapshot: bool,
) -> anyhow::Result<()> {
    if !db_path.as_ref().exists() && snapshot_path.as_ref().exists() {
        match compression::from_tar_gz(snapshot_path, &db_path) {
            Ok(()) => Ok(()),
            Err(e) => {
                // clean created db folder
                std::fs::remove_dir_all(&db_path)?;
                Err(e)
            }
        }
    } else if db_path.as_ref().exists() && !ignore_snapshot_if_db_exists {
        bail!(
            "database already exists at {:?}, try to delete it or rename it",
            db_path
                .as_ref()
                .canonicalize()
                .unwrap_or_else(|_| db_path.as_ref().to_owned())
        )
    } else if !snapshot_path.as_ref().exists() && !ignore_missing_snapshot {
        bail!(
            "snapshot doesn't exist at {:?}",
            snapshot_path
                .as_ref()
                .canonicalize()
                .unwrap_or_else(|_| snapshot_path.as_ref().to_owned())
        )
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use futures::future::{err, ok};
    use rand::Rng;
    use tokio::time::timeout;
    use uuid::Uuid;

    use super::*;
    use crate::index_controller::update_actor::{MockUpdateActorHandle, UpdateError};
    use crate::index_controller::uuid_resolver::{MockUuidResolverHandle, UuidError};

    #[actix_rt::test]
    async fn test_normal() {
        let mut rng = rand::thread_rng();
        let uuids_num = rng.gen_range(5, 10);
        let uuids = (0..uuids_num).map(|_| Uuid::new_v4()).collect::<Vec<_>>();

        let mut uuid_resolver = MockUuidResolverHandle::new();
        let uuids_clone = uuids.clone();
        uuid_resolver
            .expect_snapshot()
            .times(1)
            .returning(move |_| Box::pin(ok(uuids_clone.clone())));

        let mut update_handle = MockUpdateActorHandle::new();
        let uuids_clone = uuids.clone();
        update_handle
            .expect_snapshot()
            .withf(move |uuid, _path| uuids_clone.contains(uuid))
            .times(uuids_num)
            .returning(move |_, _| Box::pin(ok(())));

        let snapshot_path = tempfile::tempdir_in(".").unwrap();
        let snapshot_service = SnapshotService::new(
            uuid_resolver,
            update_handle,
            Duration::from_millis(100),
            snapshot_path.path().to_owned(),
            "data.ms".to_string(),
        );

        snapshot_service.perform_snapshot().await.unwrap();
    }

    #[actix_rt::test]
    async fn error_performing_uuid_snapshot() {
        let mut uuid_resolver = MockUuidResolverHandle::new();
        uuid_resolver
            .expect_snapshot()
            .times(1)
            // abitrary error
            .returning(|_| Box::pin(err(UuidError::NameAlreadyExist)));

        let update_handle = MockUpdateActorHandle::new();

        let snapshot_path = tempfile::tempdir_in(".").unwrap();
        let snapshot_service = SnapshotService::new(
            uuid_resolver,
            update_handle,
            Duration::from_millis(100),
            snapshot_path.path().to_owned(),
            "data.ms".to_string(),
        );

        assert!(snapshot_service.perform_snapshot().await.is_err());
        // Nothing was written to the file
        assert!(!snapshot_path.path().join("data.ms.snapshot").exists());
    }

    #[actix_rt::test]
    async fn error_performing_index_snapshot() {
        let uuid = Uuid::new_v4();
        let mut uuid_resolver = MockUuidResolverHandle::new();
        uuid_resolver
            .expect_snapshot()
            .times(1)
            .returning(move |_| Box::pin(ok(vec![uuid])));

        let mut update_handle = MockUpdateActorHandle::new();
        update_handle
            .expect_snapshot()
            // abitrary error
            .returning(|_, _| Box::pin(err(UpdateError::UnexistingUpdate(0))));

        let snapshot_path = tempfile::tempdir_in(".").unwrap();
        let snapshot_service = SnapshotService::new(
            uuid_resolver,
            update_handle,
            Duration::from_millis(100),
            snapshot_path.path().to_owned(),
            "data.ms".to_string(),
        );

        assert!(snapshot_service.perform_snapshot().await.is_err());
        // Nothing was written to the file
        assert!(!snapshot_path.path().join("data.ms.snapshot").exists());
    }

    #[actix_rt::test]
    async fn test_loop() {
        let mut uuid_resolver = MockUuidResolverHandle::new();
        uuid_resolver
            .expect_snapshot()
            // we expect the funtion to be called between 2 and 3 time in the given interval.
            .times(2..4)
            // abitrary error, to short-circuit the function
            .returning(move |_| Box::pin(err(UuidError::NameAlreadyExist)));

        let update_handle = MockUpdateActorHandle::new();

        let snapshot_path = tempfile::tempdir_in(".").unwrap();
        let snapshot_service = SnapshotService::new(
            uuid_resolver,
            update_handle,
            Duration::from_millis(100),
            snapshot_path.path().to_owned(),
            "data.ms".to_string(),
        );

        let _ = timeout(Duration::from_millis(300), snapshot_service.run()).await;
    }
}

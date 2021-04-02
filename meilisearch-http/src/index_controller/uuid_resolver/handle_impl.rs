use std::path::{Path, PathBuf};

use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use super::{HeedUuidStore, Result, UuidResolveMsg, UuidResolverActor, UuidResolverHandle};

#[derive(Clone)]
pub struct UuidResolverHandleImpl {
    sender: mpsc::Sender<UuidResolveMsg>,
}

impl UuidResolverHandleImpl {
    pub fn new(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let (sender, reveiver) = mpsc::channel(100);
        let store = HeedUuidStore::new(path)?;
        let actor = UuidResolverActor::new(reveiver, store);
        tokio::spawn(actor.run());
        Ok(Self { sender })
    }
}

macro_rules! handler {
    ($({$fn_name:ident, $message:ident, [$($arg:ident: $arg_type:ty),*], $return:ty}),*) => {
        #[async_trait::async_trait]
        impl UuidResolverHandle for UuidResolverHandleImpl {
            $(
                async fn $fn_name(&self, $($arg: $arg_type, )*) -> $return {
                    let (ret, receiver) = oneshot::channel();
                    let msg = UuidResolveMsg::$message { $($arg,)* ret };
                    let _ = self.sender.send(msg).await;
                    Ok(receiver.await.expect("UuidResolverActor has been killed")?)
                }
            )*
        }
    };
}

handler!(
    {get, Get, [uid: String], Result<Uuid>},
    {create, Create, [uid: String], anyhow::Result<Uuid>},
    {delete, Delete, [uid: String], anyhow::Result<Uuid>},
    {list, List, [], anyhow::Result<Vec<(String, Uuid)>>},
    {insert, Insert, [name: String, uuid: Uuid], anyhow::Result<()>},
    {snapshot, SnapshotRequest, [path: PathBuf], Result<Vec<Uuid>>}
);

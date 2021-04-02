use std::path::{Path, PathBuf};

use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::index_controller::IndexActorHandle;

use super::{
    MapUpdateStoreStore, PayloadData, Result, UpdateActor, UpdateActorHandle, UpdateMeta,
    UpdateMsg, UpdateStatus,
};

#[derive(Clone)]
pub struct UpdateActorHandleImpl<D> {
    sender: mpsc::Sender<UpdateMsg<D>>,
}

impl<D> UpdateActorHandleImpl<D>
where
    D: AsRef<[u8]> + Sized + 'static + Sync + Send,
{
    pub fn new<I>(
        index_handle: I,
        path: impl AsRef<Path>,
        update_store_size: usize,
    ) -> anyhow::Result<Self>
    where
        I: IndexActorHandle + Clone + Send + Sync + 'static,
    {
        let path = path.as_ref().to_owned().join("updates");
        let (sender, receiver) = mpsc::channel(100);
        let store = MapUpdateStoreStore::new(index_handle.clone(), &path, update_store_size);
        let actor = UpdateActor::new(store, receiver, path, index_handle)?;

        tokio::task::spawn(actor.run());

        Ok(Self { sender })
    }
}

macro_rules! handler {
    ($({$fn_name:ident, $message:ident, [$($arg:ident: $arg_type:ty),*], $return:ty}),*) => {
        #[async_trait::async_trait]
        impl<D> UpdateActorHandle for UpdateActorHandleImpl<D>
        where
            D: AsRef<[u8]> + Sized + 'static + Sync + Send,
        {
            type Data = D;
            $(
                async fn $fn_name(&self, $($arg: $arg_type, )*) -> $return {
                    let (ret, receiver) = oneshot::channel();
                    let msg = UpdateMsg::$message { $($arg,)* ret };
                    let _ = self.sender.send(msg).await;
                    Ok(receiver.await.expect("UpdateActor has been killed")?)
                }
            )*
        }
    };
}

handler!(
    {update, Update, [meta: UpdateMeta, data: mpsc::Receiver<PayloadData<Self::Data>>, uuid: Uuid], Result<UpdateStatus>},
    {get_all_updates_status, ListUpdates, [uuid: Uuid], Result<Vec<UpdateStatus>>},
    {update_status, GetUpdate, [uuid: Uuid, id: u64], Result<UpdateStatus>},
    {delete, Delete, [uuid: Uuid], Result<()>},
    {create, Create, [uuid: Uuid], Result<()>},
    {snapshot, Snapshot, [uuid: Uuid, path: PathBuf], Result<()>}
);

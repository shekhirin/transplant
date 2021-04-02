use std::path::{Path, PathBuf};

use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::index::{Document, SearchQuery, SearchResult, Settings};
use crate::index_controller::IndexSettings;
use crate::index_controller::{updates::Processing, UpdateMeta};

use super::{
    IndexActor, IndexActorHandle, IndexMeta, IndexMsg, MapIndexStore, Result, UpdateResult,
};

#[derive(Clone)]
pub struct IndexActorHandleImpl {
    read_sender: mpsc::Sender<IndexMsg>,
    write_sender: mpsc::Sender<IndexMsg>,
}

impl IndexActorHandleImpl {
    pub fn new(path: impl AsRef<Path>, index_size: usize) -> anyhow::Result<Self> {
        let (read_sender, read_receiver) = mpsc::channel(100);
        let (write_sender, write_receiver) = mpsc::channel(100);

        let store = MapIndexStore::new(path, index_size);
        let actor = IndexActor::new(read_receiver, write_receiver, store)?;
        tokio::task::spawn(actor.run());
        Ok(Self {
            read_sender,
            write_sender,
        })
    }
}

macro_rules! handler {
    ($({$fn_name:ident, $message:ident, [$($arg:ident: $arg_type:ty),*], $return:ty}),*) => {
        #[async_trait::async_trait]
        impl IndexActorHandle for IndexActorHandleImpl {
            $(
                async fn $fn_name(&self, $($arg: $arg_type, )*) -> $return {
                    let (ret, receiver) = oneshot::channel();
                    let msg = IndexMsg::$message { $($arg,)* ret };
                    let _ = self.read_sender.send(msg).await;
                    Ok(receiver.await.expect("IndexActor has been killed")?)
                }
            )*
        }
    };
}

handler!(
    {update, Update, [meta: Processing<UpdateMeta>, data: std::fs::File], anyhow::Result<UpdateResult>},
    {create_index, CreateIndex, [uuid: Uuid, primary_key: Option<String>], Result<IndexMeta>},
    {search, Search, [uuid: Uuid, query: SearchQuery], Result<SearchResult>},
    {settings, Settings, [uuid: Uuid], Result<Settings>},
    {documents, Documents, [uuid: Uuid, offset: usize, limit: usize, attributes_to_retrieve: Option<Vec<String>>], Result<Vec<Document>>},
    {document, Document, [uuid: Uuid, doc_id: String, attributes_to_retrieve: Option<Vec<String>>], Result<Document>},
    {delete, Delete, [uuid: Uuid], Result<()>},
    {get_index_meta, GetMeta, [uuid: Uuid], Result<IndexMeta>},
    {update_index, UpdateIndex, [uuid: Uuid, index_settings: IndexSettings], Result<IndexMeta>},
    {snapshot, Snapshot, [uuid: Uuid, path: PathBuf], Result<()>}
);

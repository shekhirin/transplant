use actix_web::web::Payload;
use milli::update::{IndexDocumentsMethod, UpdateFormat};

use super::Data;
use crate::index::Settings;
use crate::index_controller::{IndexMetadata, IndexSettings, UpdateStatus};

impl Data {
    pub async fn add_documents(
        &self,
        index: String,
        method: IndexDocumentsMethod,
        format: UpdateFormat,
        stream: Payload,
        primary_key: Option<String>,
    ) -> anyhow::Result<UpdateStatus> {
        let update_status = self
            .index_controller
            .add_documents(index, method, format, stream, primary_key)
            .await?;
        Ok(update_status)
    }

    pub async fn update_settings(
        &self,
        index: String,
        settings: Settings,
        create: bool,
    ) -> anyhow::Result<UpdateStatus> {
        let update = self
            .index_controller
            .update_settings(index, settings, create)
            .await?;
        Ok(update)
    }

    pub async fn clear_documents(&self, index: String) -> anyhow::Result<UpdateStatus> {
        let update = self.index_controller.clear_documents(index).await?;
        Ok(update)
    }

    pub async fn delete_documents(
        &self,
        index: String,
        document_ids: Vec<String>,
    ) -> anyhow::Result<UpdateStatus> {
        let update = self
            .index_controller
            .delete_documents(index, document_ids)
            .await?;
        Ok(update)
    }

    pub async fn delete_index(&self, index: String) -> anyhow::Result<()> {
        self.index_controller.delete_index(index).await?;
        Ok(())
    }

    pub async fn get_update_status(&self, index: String, uid: u64) -> anyhow::Result<UpdateStatus> {
        self.index_controller.update_status(index, uid).await
    }

    pub async fn get_updates_status(&self, index: String) -> anyhow::Result<Vec<UpdateStatus>> {
        self.index_controller.all_update_status(index).await
    }

    pub async fn update_index(
        &self,
        uid: String,
        primary_key: Option<String>,
        new_uid: Option<String>,
    ) -> anyhow::Result<IndexMetadata> {
        let settings = IndexSettings {
            uid: new_uid,
            primary_key,
        };

        self.index_controller.update_index(uid, settings).await
    }
}

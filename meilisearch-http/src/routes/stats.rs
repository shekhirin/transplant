use std::collections::{BTreeMap, HashMap};

use actix_web::get;
use actix_web::web;
use actix_web::HttpResponse;
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::error::ResponseError;
use crate::helpers::Authentication;
use crate::routes::IndexParam;
use crate::Data;

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(index_stats)
        .service(get_stats)
        .service(get_version);
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct IndexStatsResponse {
    number_of_documents: u64,
    is_indexing: bool,
    fields_distribution: BTreeMap<String, usize>,
}

#[get("/indexes/{index_uid}/stats", wrap = "Authentication::Private")]
async fn index_stats(
    _data: web::Data<Data>,
    _path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StatsResult {
    database_size: u64,
    last_update: Option<DateTime<Utc>>,
    indexes: HashMap<String, IndexStatsResponse>,
}

#[get("/stats", wrap = "Authentication::Private")]
async fn get_stats(_data: web::Data<Data>) -> Result<HttpResponse, ResponseError> {
    todo!()
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct VersionResponse {
    commit_sha: String,
    commit_date: String,
    pkg_version: String,
}

#[get("/version", wrap = "Authentication::Private")]
async fn get_version() -> HttpResponse {
    let commit_sha = match option_env!("COMMIT_SHA") {
        Some("") | None => env!("VERGEN_SHA"),
        Some(commit_sha) => commit_sha
    };
    let commit_date = match option_env!("COMMIT_DATE") {
        Some("") | None => env!("VERGEN_COMMIT_DATE"),
        Some(commit_date) => commit_date
    };

    HttpResponse::Ok().json(VersionResponse {
        commit_sha: commit_sha.to_string(),
        commit_date: commit_date.to_string(),
        pkg_version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

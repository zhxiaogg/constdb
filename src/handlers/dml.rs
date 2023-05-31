use std::collections::HashMap;
use std::sync::Arc;

use crate::constdb::Engine;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::routing::{delete, get, put};
use axum::Router;
use tokio::sync::RwLock;

pub fn dml_routes() -> Router<Arc<RwLock<Engine>>> {
    Router::new()
        .route("/", post(table_insert))
        .route("/", get(table_get_by_key))
        .route("/", delete(table_delete))
        .route("/", put(table_update))
}

pub async fn table_insert(
    State(const_db): State<Arc<RwLock<Engine>>>,
    Path((db_name, table_name)): Path<(String, String)>,
    bytes: Bytes,
) -> impl IntoResponse {
    let const_db = Arc::clone(&const_db);
    let cdb = const_db.read().await;
    let result = cdb.insert(db_name.as_str(), table_name.as_str(), bytes);
    match result {
        Ok(()) => (StatusCode::OK, ()).into_response(),
        Err(e) => (e.http_status_code(), e.to_string()).into_response(),
    }
}

pub async fn table_get_by_key(
    State(const_db): State<Arc<RwLock<Engine>>>,
    Path((db_name, table_name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let const_db = Arc::clone(&const_db);
    let cdb = const_db.read().await;
    let result = cdb.query_by_key(db_name.as_str(), table_name.as_str(), params);
    match result {
        Ok(v) => (StatusCode::OK, [("content-type", "aplication/json")], v).into_response(),
        Err(e) => (e.http_status_code(), e.to_string()).into_response(),
    }
}

pub async fn table_delete(
    State(const_db): State<Arc<RwLock<Engine>>>,
    Path((db_name, table_name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let const_db = Arc::clone(&const_db);
    let cdb = const_db.read().await;
    let result = cdb.delete(db_name.as_str(), table_name.as_str(), params);
    match result {
        Ok(()) => (StatusCode::OK, ()).into_response(),
        Err(e) => (e.http_status_code(), e.to_string()).into_response(),
    }
}

pub async fn table_update(
    State(const_db): State<Arc<RwLock<Engine>>>,
    Path((db_name, table_name)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    bytes: Bytes,
) -> impl IntoResponse {
    let const_db = Arc::clone(&const_db);
    let cdb = const_db.read().await;
    let result = cdb.update(db_name.as_str(), table_name.as_str(), bytes, params);
    match result {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => (e.http_status_code(), e.to_string()).into_response(),
    }
}

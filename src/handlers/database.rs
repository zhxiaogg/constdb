use std::sync::Arc;

use crate::constdb::Engine;
use crate::handlers::models::*;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::routing::{delete, post};
use axum::{Json, Router};
use tokio::sync::RwLock;

pub fn db_routes() -> Router<Arc<RwLock<Engine>>> {
    Router::new()
        .route("/", get(list_db_route))
        .route("/", post(create_db_route))
        .route("/:db_name", delete(drop_db_route))
}

pub async fn list_db_route(State(const_db): State<Arc<RwLock<Engine>>>) -> impl IntoResponse {
    let cdb = const_db.read().await;
    let result = cdb.list_db();
    match result {
        Ok(dbs) => (StatusCode::OK, Json(dbs)).into_response(),
        Err(e) => (e.http_status_code(), e.to_string()).into_response(),
    }
}

pub async fn create_db_route(
    State(const_db): State<Arc<RwLock<Engine>>>,
    Json(create_db_input): Json<CreateDBInput>,
) -> impl IntoResponse {
    println!("creating db [{}]...", create_db_input.name);
    let mut cdb = const_db.write().await;
    let result = cdb.create_db(create_db_input.name.as_str());
    match result {
        Ok(db) => (StatusCode::CREATED, Json(db)).into_response(),
        Err(e) => (e.http_status_code(), e.to_string()).into_response(),
    }
}

pub async fn drop_db_route(
    State(const_db): State<Arc<RwLock<Engine>>>,
    Path(db_name): Path<String>,
) -> impl IntoResponse {
    let mut cdb = const_db.write().await;
    let result = cdb.drop_db(db_name.as_str());
    match result {
        Ok(_) => (StatusCode::OK, ()).into_response(),
        Err(e) => (e.http_status_code(), e.to_string()).into_response(),
    }
}

use std::sync::Arc;

use crate::constdb::Engine;
use crate::handlers::models::*;
use crate::protos::constdb_model::TableSettings;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use tokio::sync::RwLock;

pub fn table_routes() -> Router<Arc<RwLock<Engine>>> {
    Router::new()
        .route("/", get(list_table_route))
        .route("/", post(create_table_route))
        .route("/:table_name", delete(drop_table_route))
}

pub async fn list_table_route(
    State(const_db): State<Arc<RwLock<Engine>>>,
    Path(db_name): Path<String>,
) -> impl IntoResponse {
    let cdb = const_db.read().await;
    let result = cdb.list_table(db_name.as_str());
    match result {
        Ok(tables) => (StatusCode::OK, Json(tables)).into_response(),
        Err(e) => (e.http_status_code(), e.to_string()).into_response(),
    }
}

pub async fn create_table_route(
    State(const_db): State<Arc<RwLock<Engine>>>,
    Path(db_name): Path<String>,
    Json(new_table_input): Json<TableSettings>,
) -> impl IntoResponse {
    println!(
        "create table [{}] under db [{}]",
        new_table_input.name, db_name
    );
    let mut db = const_db.write().await;
    let result = db.create_table(db_name.as_str(), &new_table_input);
    match result {
        Ok(()) => {
            let output = CreateTableOutput {
                name: new_table_input.name.to_string(),
            };
            (StatusCode::CREATED, Json(output)).into_response()
        }
        Err(e) => (e.http_status_code(), e.to_string()).into_response(),
    }
}

pub async fn drop_table_route(
    State(const_db): State<Arc<RwLock<Engine>>>,
    Path(db_name): Path<String>,
    Path(table_name): Path<String>,
) -> impl IntoResponse {
    let mut cdb = const_db.write().await;
    let result = cdb.delete_table(db_name.as_str(), table_name.as_str());
    match result {
        Ok(_) => (StatusCode::OK, ()).into_response(),
        Err(e) => (e.http_status_code(), e.to_string()).into_response(),
    }
}

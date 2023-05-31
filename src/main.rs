pub mod constdb;
use std::net::SocketAddr;
use std::sync::Arc;

mod handlers;
mod protos;
mod utils;
use axum::ServiceExt;

use axum::routing::get;
use axum::Router;
use constdb::{Engine, Settings};
use handlers::database::db_routes;
use handlers::dml::dml_routes;
use handlers::table::table_routes;

use tokio::sync::RwLock;

use clap::Parser;
use tower::layer::Layer;
use tower_http::normalize_path::NormalizePathLayer;

/// The constdb app
#[derive(Debug, Parser)]
#[clap(author, version, about, long_about=None)]
struct ConstDBArgs {
    /// Path to the root folder of constdb
    #[clap(short, long, action)]
    root: String,
}

#[tokio::main]
async fn main() {
    let args = ConstDBArgs::parse();
    let settings = Settings {
        root: args.root.to_string(),
    };
    let const_db = Arc::new(RwLock::new(Engine::new(settings).unwrap()));

    let router = Router::new()
        .route("/", get(root))
        .nest("/api/v1/dbs", db_routes())
        .nest("/api/v1/dbs/:db_name/tables", table_routes())
        .nest("/api/v1/dbs/:db_name/tables/:table_name/data", dml_routes())
        .with_state(const_db);

    let app = NormalizePathLayer::trim_trailing_slash().layer(router);

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn root() -> &'static str {
    "Hello, ConstDB!"
}

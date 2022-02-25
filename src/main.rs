pub mod constdb;
use std::sync::Arc;

mod handlers;
mod protos;
mod utils;
use constdb::{ConstDB, Settings};
use handlers::database::{create_db_route, list_db_route};
use handlers::dml::{table_delete, table_get_by_key, table_insert, table_update};
use handlers::table::{create_table_route, list_table_route};
use tokio::sync::RwLock;

use warp;
use warp::Filter;

use clap::Parser;

/// The constdb app
#[derive(Debug, Parser)]
#[clap(author, version, about, long_about=None)]
struct ConstDBArgs {
    /// Path to the root folder of constdb
    #[clap(short, long)]
    root: String,
}

#[tokio::main]
async fn main() {
    let args = ConstDBArgs::parse();
    let settings = Settings {
        root: args.root.to_string(),
    };
    let const_db = Arc::new(RwLock::new(ConstDB::new(settings).unwrap()));

    let index_route = warp::path::end().map(|| "Hello, ConstDB!");

    let table_insert = table_insert(&const_db);
    let table_query_by_key = table_get_by_key(&const_db);
    let table_delete = table_delete(&const_db);
    let table_update = table_update(&const_db);
    let list_table = list_table_route(&const_db);
    let create_table = create_table_route(&const_db);
    let list_db = list_db_route(&const_db);
    let create_db = create_db_route(&const_db);
    let ddl_dml_routes = warp::path!("dbs" / ..).and(
        create_db
            .or(list_db)
            .or(create_table)
            .or(list_table)
            .or(table_insert)
            .or(table_query_by_key)
            .or(table_delete)
            .or(table_update),
    );
    let api_routes = warp::path!("api" / "v1" / ..).and(ddl_dml_routes);
    warp::serve(index_route.or(api_routes))
        .run(([0, 0, 0, 0], 8000))
        .await
}

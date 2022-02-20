#![feature(path_try_exists)]
pub mod constdb;
use std::sync::Arc;

use crate::constdb::api::*;

use constdb::{ConstDB, Settings};
use tokio::sync::RwLock;
use warp::hyper::StatusCode;
use warp::reply::{json, with_status};
use warp::Filter;
use warp::{self, Reply};

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
    let const_db = Arc::new(RwLock::new(ConstDB::create(settings).unwrap()));

    let index_route = warp::path::end().map(|| "Hello, ConstDB!");
    let create_table = create_table_route(&const_db);
    let list_db = list_db_route(&const_db);
    let create_db = create_db_route(&const_db);
    let ddl_routes = warp::path!("dbs" / ..).and(create_db.or(list_db).or(create_table));
    let api_routes = warp::path!("api" / "v1" / ..).and(ddl_routes);
    warp::serve(index_route.or(api_routes))
        .run(([0, 0, 0, 0], 8000))
        .await
}
fn list_db_route(
    db: &Arc<RwLock<ConstDB>>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let const_db = Arc::clone(db);
    warp::path::end().and(warp::get()).then(move || {
        let const_db = Arc::clone(&const_db);
        async move {
            let cdb = const_db.read().await;
            let result = cdb.list_db();
            match result {
                Ok(dbs) => with_status(json(&dbs), StatusCode::OK).into_response(),

                Err(e) => with_status(e.to_string(), e.http_status_code()).into_response(),
            }
        }
    })
}
fn create_db_route(
    db: &Arc<RwLock<ConstDB>>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let const_db = Arc::clone(db);
    warp::path::end()
        .and(warp::post())
        .and(warp::body::json())
        .then(move |create_db_input: CreateDBInput| {
            println!("creating db [{}]...", create_db_input.name);
            let const_db = Arc::clone(&const_db);
            async move {
                let mut cdb = const_db.write().await;
                let result = cdb.create_db(create_db_input.name.as_str());
                match result {
                    Ok(_) => {
                        let output = CreateDBOutput {
                            name: create_db_input.name.to_string(),
                        };
                        with_status(json(&output), StatusCode::CREATED).into_response()
                    }
                    Err(e) => with_status(e.to_string(), e.http_status_code()).into_response(),
                }
            }
        })
}

fn create_table_route(
    db: &Arc<RwLock<ConstDB>>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let const_db = Arc::clone(db);
    warp::path!(String / "tables")
        .and(warp::path::end())
        .and(warp::post())
        .and(warp::body::json())
        .then(move |db_name: String, new_table_input: CreateTableInput| {
            println!(
                "create table [{}] under db [{}]",
                new_table_input.name, db_name
            );
            let const_db = Arc::clone(&const_db);
            async move {
                let mut db = const_db.write().await;
                let result = db.create_table(db_name.as_str(), &new_table_input);
                match result {
                    Ok(()) => {
                        let output = CreateTableOutput {
                            name: new_table_input.name.to_string(),
                        };
                        with_status(warp::reply::json(&output), StatusCode::CREATED).into_response()
                    }
                    Err(e) => with_status(e.to_string(), e.http_status_code()).into_response(),
                }
            }
        })
}

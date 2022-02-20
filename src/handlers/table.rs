use std::sync::Arc;

use crate::constdb::{api::*, ConstDB};

use tokio::sync::RwLock;
use warp::hyper::StatusCode;
use warp::reply::{json, with_status};
use warp::Filter;
use warp::{self, Reply};

pub fn list_table_route(
    db: &Arc<RwLock<ConstDB>>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let const_db = Arc::clone(db);
    warp::path!(String / "tables")
        .and(warp::path::end())
        .and(warp::get())
        .then(move |db_name: String| {
            let const_db = Arc::clone(&const_db);
            async move {
                let cdb = const_db.read().await;
                let result = cdb.list_table(db_name.as_str());
                match result {
                    Ok(tables) => with_status(json(&tables), StatusCode::OK).into_response(),
                    Err(e) => with_status(e.to_string(), e.http_status_code()).into_response(),
                }
            }
        })
}

pub fn create_table_route(
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

use std::collections::HashMap;
use std::sync::Arc;

use crate::constdb::Engine;

use tokio::sync::RwLock;
use warp::hyper::body::Bytes;
use warp::hyper::StatusCode;
use warp::reply::{with_header, with_status};
use warp::Filter;
use warp::{self, Reply};

pub fn table_insert(
    db: &Arc<RwLock<Engine>>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    let const_db = Arc::clone(db);
    warp::path!(String / "tables" / String)
        .and(warp::path::end())
        .and(warp::post())
        .and(warp::body::bytes())
        .then(move |db_name: String, table_name: String, bytes: Bytes| {
            let const_db = Arc::clone(&const_db);
            async move {
                let cdb = const_db.read().await;
                let result = cdb.insert(db_name.as_str(), table_name.as_str(), bytes);
                match result {
                    Ok(()) => StatusCode::OK.into_response(),
                    Err(e) => with_status(e.to_string(), e.http_status_code()).into_response(),
                }
            }
        })
}

pub fn table_get_by_key(
    db: &Arc<RwLock<Engine>>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    let const_db = Arc::clone(db);
    warp::path!(String / "tables" / String)
        .and(warp::path::end())
        .and(warp::get())
        .and(warp::query())
        .then(
            move |db_name: String, table_name: String, params: HashMap<String, String>| {
                let const_db = Arc::clone(&const_db);
                async move {
                    let cdb = const_db.read().await;
                    let result = cdb.query_by_key(db_name.as_str(), table_name.as_str(), params);
                    match result {
                        Ok(v) => with_header(
                            with_status(v, StatusCode::OK),
                            "content-type",
                            "aplication/json",
                        )
                        .into_response(),
                        Err(e) => with_status(e.to_string(), e.http_status_code()).into_response(),
                    }
                }
            },
        )
}

pub fn table_delete(
    db: &Arc<RwLock<Engine>>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    let const_db = Arc::clone(db);
    warp::path!(String / "tables" / String)
        .and(warp::path::end())
        .and(warp::delete())
        .and(warp::query())
        .then(
            move |db_name: String, table_name: String, params: HashMap<String, String>| {
                let const_db = Arc::clone(&const_db);
                async move {
                    let cdb = const_db.read().await;
                    let result = cdb.delete(db_name.as_str(), table_name.as_str(), params);
                    match result {
                        Ok(()) => StatusCode::OK.into_response(),
                        Err(e) => with_status(e.to_string(), e.http_status_code()).into_response(),
                    }
                }
            },
        )
}

pub fn table_update(
    db: &Arc<RwLock<Engine>>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    let const_db = Arc::clone(db);
    warp::path!(String / "tables" / String)
        .and(warp::path::end())
        .and(warp::put())
        .and(warp::body::bytes())
        .and(warp::query())
        .then(
            move |db_name: String,
                  table_name: String,
                  bytes: Bytes,
                  params: HashMap<String, String>| {
                let const_db = Arc::clone(&const_db);
                async move {
                    let cdb = const_db.read().await;
                    let result = cdb.update(db_name.as_str(), table_name.as_str(), bytes, params);
                    match result {
                        Ok(()) => StatusCode::OK.into_response(),
                        Err(e) => with_status(e.to_string(), e.http_status_code()).into_response(),
                    }
                }
            },
        )
}

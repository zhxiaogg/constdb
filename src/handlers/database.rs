use std::sync::Arc;

use crate::constdb::{api::*, Engine};

use tokio::sync::RwLock;
use warp::hyper::StatusCode;
use warp::reply::{json, with_status};
use warp::Filter;
use warp::{self, Reply};

pub fn list_db_route(
    db: &Arc<RwLock<Engine>>,
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

pub fn create_db_route(
    db: &Arc<RwLock<Engine>>,
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

pub fn drop_db_route(
    db: &Arc<RwLock<Engine>>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let const_db = Arc::clone(db);
    warp::path!(String)
        .and(warp::path::end())
        .and(warp::delete())
        .then(move |db_name: String| {
            let const_db = Arc::clone(&const_db);
            async move {
                let mut cdb = const_db.write().await;
                let result = cdb.drop_db(db_name.as_str());
                match result {
                    Ok(_) => StatusCode::OK.into_response(),
                    Err(e) => with_status(e.to_string(), e.http_status_code()).into_response(),
                }
            }
        })
}

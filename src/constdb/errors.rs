use axum::http::StatusCode;

use super::Id;

#[derive(Debug)]
pub enum ConstDBError {
    AlreadyExists(Id),
    NotFound(Id),
    InvalidStates(String),
    InvalidArguments(String),
}

impl ToString for ConstDBError {
    fn to_string(&self) -> String {
        match self {
            ConstDBError::AlreadyExists(id) => format!("{} already exists!", id.to_string()),
            ConstDBError::NotFound(id) => format!("{} not found!", id.to_string()),
            ConstDBError::InvalidStates(msg) => msg.to_owned(),
            ConstDBError::InvalidArguments(msg) => msg.to_owned(),
        }
    }
}

impl ConstDBError {
    pub fn http_status_code(&self) -> StatusCode {
        match self {
            ConstDBError::AlreadyExists(_msg) => StatusCode::BAD_REQUEST,
            ConstDBError::NotFound(_) => StatusCode::NOT_FOUND,
            ConstDBError::InvalidStates(_msg) => StatusCode::INTERNAL_SERVER_ERROR,
            ConstDBError::InvalidArguments(_) => StatusCode::BAD_REQUEST,
        }
    }
}

impl From<std::io::Error> for ConstDBError {
    fn from(e: std::io::Error) -> Self {
        ConstDBError::InvalidStates(format!("io error: {}", e))
    }
}

impl From<rocksdb::Error> for ConstDBError {
    fn from(e: rocksdb::Error) -> Self {
        ConstDBError::InvalidStates(format!("rocksdb failed: {}", e))
    }
}

impl From<serde_json::Error> for ConstDBError {
    fn from(e: serde_json::Error) -> Self {
        ConstDBError::InvalidStates(format!("serialization failed: {}", e))
    }
}

impl From<protobuf::Error> for ConstDBError {
    fn from(e: protobuf::Error) -> Self {
        ConstDBError::InvalidStates(format!("protobuf serialization failed: {}", e))
    }
}

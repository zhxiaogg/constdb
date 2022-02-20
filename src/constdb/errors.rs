use warp::hyper::StatusCode;

#[derive(Debug)]
pub enum ConstDBError {
    AlreadyExists(String),
    NotFound(String),
    InvalidStates(String),
}

impl ToString for ConstDBError {
    fn to_string(&self) -> String {
        match self {
            ConstDBError::AlreadyExists(msg) => msg.to_owned(),
            ConstDBError::NotFound(msg) => msg.to_owned(),
            ConstDBError::InvalidStates(msg) => msg.to_owned(),
        }
    }
}

impl ConstDBError {
    pub fn http_status_code(&self) -> StatusCode {
        match self {
            ConstDBError::AlreadyExists(_msg) => StatusCode::BAD_REQUEST,
            ConstDBError::NotFound(_msg) => StatusCode::NOT_FOUND,
            ConstDBError::InvalidStates(_msg) => StatusCode::INTERNAL_SERVER_ERROR,
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

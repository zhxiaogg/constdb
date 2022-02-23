use super::errors::ConstDBError;

#[derive(Debug)]
pub enum PrimaryKey {
    Complete(Vec<u8>),
    Prefix(Vec<u8>),
}

impl PrimaryKey {
    pub fn bytes(&self) -> &[u8] {
        match self {
            PrimaryKey::Complete(bytes) => bytes.as_slice(),
            PrimaryKey::Prefix(bytes) => bytes.as_slice(),
        }
    }

    pub fn complete(&self) -> Result<&[u8], ConstDBError> {
        match self {
            PrimaryKey::Complete(bytes) => Ok(bytes.as_slice()),
            PrimaryKey::Prefix(_) => Err(ConstDBError::InvalidArguments(
                "primary key not complete".to_owned(),
            )),
        }
    }
}

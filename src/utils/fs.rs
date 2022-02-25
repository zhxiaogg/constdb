use std::{
    io::{Error, ErrorKind},
    path::Path,
};

pub fn exists<P: AsRef<Path>>(p: P) -> Result<bool, Error> {
    match std::fs::metadata(p) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

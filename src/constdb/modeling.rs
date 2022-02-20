use super::errors::ConstDBError;

pub enum SystemKeys {
    TableMetaKey { db: String, table: String },
    DBMetaKey { db: String },
    DBMetaPrefix,
}

impl SystemKeys {
    pub fn table_meta_key(db: &str, table: &str) -> Self {
        SystemKeys::TableMetaKey {
            db: db.to_owned(),
            table: table.to_owned(),
        }
    }

    pub fn db_meta_key(db: &str) -> Self {
        SystemKeys::DBMetaKey { db: db.to_owned() }
    }

    pub fn as_db_meta_key(bytes: &[u8]) -> Result<String, ConstDBError> {
        match String::from_utf8(bytes.to_vec()) {
            Ok(s) if s.starts_with("d") => Ok(s.trim_start_matches("d").to_owned()),
            Ok(s) => Err(ConstDBError::InvalidStates(format!(
                "invalid table meta key: {}",
                s
            ))),
            Err(e) => Err(ConstDBError::InvalidStates(format!(
                "invalid table meta key, error: {}",
                e
            ))),
        }
    }

    pub fn as_key(&self) -> String {
        match self {
            SystemKeys::TableMetaKey { db, table } => format!("t.{}.{}", db, table),
            SystemKeys::DBMetaKey { db } => format!("d{}", db),
            SystemKeys::DBMetaPrefix => "d".to_owned(),
        }
    }
}

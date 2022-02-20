use super::errors::ConstDBError;

pub enum SystemKeys {
    TableMetaKey { db: String, table: String },
    TableMetaPrefix { db: String },
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

    pub fn table_meta_prefix(db: &str) -> Self {
        SystemKeys::TableMetaPrefix { db: db.to_owned() }
    }

    pub fn db_meta_key(db: &str) -> Self {
        SystemKeys::DBMetaKey { db: db.to_owned() }
    }

    pub fn parse_table_meta_key(bytes: &[u8]) -> Result<(String, String), ConstDBError> {
        match String::from_utf8(bytes.to_vec()) {
            Ok(s) if s.starts_with("t") && s.contains(".") => {
                let parts: Vec<&str> = s.trim_start_matches("t").splitn(2, ".").collect();
                Ok((parts[0].to_owned(), parts[1].to_owned()))
            }
            Ok(s) => Err(ConstDBError::InvalidStates(format!(
                "invalid table meta key: {}",
                s
            ))),
            Err(e) => Err(ConstDBError::InvalidStates(format!(
                "invalid table meta key: {}",
                e
            ))),
        }
    }

    pub fn parse_db_meta_key(bytes: &[u8]) -> Result<String, ConstDBError> {
        match String::from_utf8(bytes.to_vec()) {
            Ok(s) if s.starts_with("d") => Ok(s.trim_start_matches("d").to_owned()),
            Ok(s) => Err(ConstDBError::InvalidStates(format!(
                "invalid db meta key: {}",
                s
            ))),
            Err(e) => Err(ConstDBError::InvalidStates(format!(
                "invalid db meta key, error: {}",
                e
            ))),
        }
    }

    pub fn as_key(&self) -> String {
        match self {
            SystemKeys::TableMetaKey { db, table } => format!("t{}.{}", db, table),
            SystemKeys::TableMetaPrefix { db } => format!("t{}.", db),
            SystemKeys::DBMetaKey { db } => format!("d{}", db),
            SystemKeys::DBMetaPrefix => "d".to_owned(),
        }
    }
}

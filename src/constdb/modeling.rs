pub enum SystemKeys {
    TableMetaKey { db: String, table: String },
}

impl SystemKeys {
    pub fn table_meta_key(db: &str, table: &str) -> Self {
        SystemKeys::TableMetaKey {
            db: db.to_owned(),
            table: table.to_owned(),
        }
    }

    pub fn as_key(&self) -> String {
        match self {
            SystemKeys::TableMetaKey { db, table } => format!("{}#{}", db, table),
        }
    }
}

#[derive(Debug)]
pub enum Id {
    Database(String),
    Table { db: String, name: String },
    Data,
}

impl Id {
    pub fn table(db: &str, name: &str) -> Id {
        Id::Table {
            db: db.to_owned(),
            name: name.to_owned(),
        }
    }
}

impl ToString for Id {
    fn to_string(&self) -> String {
        match self {
            Id::Database(name) => format!("database[{}]", name),
            Id::Table { db, name } => format!("table[{}.{}]", db, name),
            _ => "data".to_owned(),
        }
    }
}

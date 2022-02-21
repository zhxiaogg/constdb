use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTableOutput {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateDBInput {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateDBOutput {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DBItem {
    pub name: String,
}

impl DBItem {
    pub fn new(name: &str) -> DBItem {
        DBItem {
            name: name.to_owned(),
        }
    }
}

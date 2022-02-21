use std::{fs, path::Path};

use rocksdb::{ColumnFamilyDescriptor, Options, DB};

use crate::protos::constdb_model::TableSettings;

use super::errors::ConstDBError;

pub struct DBInstance {
    pub name: String,
    pub root: String,
    pub rocks_db: Option<DB>,
}

impl DBInstance {
    pub fn new(name: &str, root: &str) -> Self {
        Self {
            name: name.to_owned(),
            root: root.to_owned(),
            rocks_db: None,
        }
    }

    pub fn rocks_db(&self) -> Result<&DB, ConstDBError> {
        self.rocks_db.as_ref().ok_or(ConstDBError::InvalidStates(
            "rocks db not initialized!".to_owned(),
        ))
    }

    pub fn create_table(&mut self, input: &TableSettings) -> Result<(), ConstDBError> {
        if self.rocks_db.is_none() {
            self.open_rocks_db()?;
        }
        let rocks_db = self.rocks_db.as_mut().unwrap();
        let opts = Options::default();
        // TODO: check if cf already exists
        rocks_db.create_cf(input.name.as_str(), &opts)?;
        Ok(())
    }

    pub fn open_rocks_db(&mut self) -> Result<(), ConstDBError> {
        let rocks_db_path = Path::new(self.root.as_str()).join("bin.db");
        let opts = Options::default();
        match fs::try_exists(rocks_db_path.clone()) {
            Ok(true) => {
                let cfs = DB::list_cf(&opts, rocks_db_path.clone())?
                    .into_iter()
                    .map(|cf_name| ColumnFamilyDescriptor::new(cf_name, Options::default()));
                self.rocks_db = Some(DB::open_cf_descriptors(&opts, rocks_db_path, cfs)?);
            }
            _ => {
                self.rocks_db = Some(DB::open_default(rocks_db_path)?);
            }
        }
        Ok(())
    }

    pub fn try_open_rocks_db(&mut self) -> Result<(), ConstDBError> {
        let rocks_db_path = Path::new(self.root.as_str()).join("bin.db");
        if fs::try_exists(rocks_db_path)? {
            self.open_rocks_db()?;
        }
        Ok(())
    }
}

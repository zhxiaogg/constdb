use std::{collections::HashMap, fs, path::Path};

use rocksdb::{ColumnFamilyDescriptor, Direction, Options, ReadOptions, DB};

use self::{
    api::{CreateTableInput, DBItem},
    errors::ConstDBError,
};

mod modeling;
use modeling::*;

pub mod api;
pub mod errors;

/// ConstDB settings
pub struct Settings {
    pub root: String,
}

pub struct ConstDB {
    dbs: HashMap<String, DBInfo>,
    settings: Settings,
}

pub struct DBInfo {
    name: String,
    root: String,
    rocks_db: Option<DB>,
}

impl DBInfo {
    pub fn rocks_db(&self) -> Result<&DB, ConstDBError> {
        self.rocks_db.as_ref().ok_or(ConstDBError::InvalidStates(
            "rocks db not initialized!".to_owned(),
        ))
    }

    pub fn create_table(&mut self, input: &CreateTableInput) -> Result<(), ConstDBError> {
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
        let cfs = DB::list_cf(&opts, rocks_db_path.clone())?
            .into_iter()
            .map(|cf_name| ColumnFamilyDescriptor::new(cf_name, Options::default()));
        self.rocks_db = Some(DB::open_cf_descriptors(&opts, rocks_db_path, cfs)?);
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

impl ConstDB {
    pub fn create(settings: Settings) -> Result<Self, ConstDBError> {
        let mut db = ConstDB {
            dbs: HashMap::new(),
            settings,
        };
        let system_db = db.open("system")?;

        let read_opts = ReadOptions::default();
        let prefix = SystemKeys::DBMetaPrefix.as_key();
        let tables = system_db.rocks_db()?.iterator_opt(
            rocksdb::IteratorMode::From(prefix.as_ref(), Direction::Forward),
            read_opts,
        );
        for try_db_name in tables
            .into_iter()
            .map(|(k, _v)| SystemKeys::as_db_meta_key(k.as_ref()))
        {
            if !try_db_name.is_ok() {
                break;
            }
            let db_name = try_db_name?;
            println!("found db [{}]...", db_name);
            let d = db.open(db_name.as_ref())?;
            db.dbs.insert(db_name, d);
        }

        db.dbs.insert("system".to_owned(), system_db);
        Ok(db)
    }

    fn system_db(&self) -> Result<&DBInfo, ConstDBError> {
        self.dbs.get("system").ok_or(ConstDBError::InvalidStates(
            "cannot find [system] db".to_owned(),
        ))
    }

    pub fn db_exists(&self, name: &str) -> bool {
        self.dbs.contains_key(name)
    }

    pub fn table_exists(&self, db: &str, table: &str) -> Result<bool, ConstDBError> {
        let system_db = self.system_db()?;
        let rocks_db = system_db.rocks_db()?;
        Ok(rocks_db
            .get_pinned(SystemKeys::table_meta_key(db, table).as_key())
            .map(|r| r.is_some())?)
    }

    pub fn create_db(&mut self, name: &str) -> Result<(), ConstDBError> {
        if self.db_exists(name) {
            return Err(ConstDBError::AlreadyExists(format!(
                "db [{}] already exists!",
                name
            )));
        }
        let db = self.open(name)?;
        self.dbs.insert(name.to_owned(), db);
        self.system_db()?
            .rocks_db()?
            .put(SystemKeys::db_meta_key(name).as_key(), "")?;
        Ok(())
    }

    pub fn list_db(&self) -> Result<Vec<DBItem>, ConstDBError> {
        Ok(self
            .dbs
            .iter()
            .map(|(k, _)| DBItem::new(k.as_str()))
            .collect())
    }

    pub fn create_table(
        &mut self,
        db_name: &str,
        input: &CreateTableInput,
    ) -> Result<(), ConstDBError> {
        if !self.db_exists(db_name) {
            return Err(ConstDBError::NotFound(format!(
                "db [{}] not found!",
                db_name
            )));
        }
        if self.table_exists(db_name, input.name.as_str())? {
            return Err(ConstDBError::AlreadyExists(format!(
                "table [{}.{}] already exists.",
                db_name, input.name
            )));
        }
        let db = self.dbs.get_mut(db_name).unwrap();
        db.create_table(input)?;
        let system_db = self.system_db()?;
        // TODO: set table info as value
        system_db.rocks_db()?.put(
            SystemKeys::table_meta_key(db_name, input.name.as_str()).as_key(),
            "",
        )?;
        Ok(())
    }

    fn open(&self, name: &str) -> Result<DBInfo, ConstDBError> {
        let path = Path::new(self.settings.root.as_str()).join(name);
        std::fs::create_dir_all(&path)?;
        let mut db = DBInfo {
            name: name.to_owned(),
            root: path.to_str().unwrap().to_owned(),
            rocks_db: None,
        };

        if "system".eq_ignore_ascii_case(name) {
            db.open_rocks_db()?;
        } else {
            db.try_open_rocks_db()?;
        };
        Ok(db)
    }
}

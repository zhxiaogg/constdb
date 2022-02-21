use core::panic;
use std::{collections::HashMap, fs, path::Path};

use protobuf::Message;
use rocksdb::{ColumnFamilyDescriptor, Direction, Options, ReadOptions, DB};
use serde_json::Value;
use warp::hyper::body::Bytes;

use crate::protos::constdb_model::TableSettings;

use self::{api::DBItem, db::DBInstance, errors::ConstDBError};

mod db;
mod modeling;
use modeling::*;

pub mod api;
pub mod errors;

/// ConstDB settings
pub struct Settings {
    pub root: String,
}

pub struct ConstDB {
    dbs: HashMap<String, DBInstance>,
    settings: Settings,
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
            .map(|(k, _v)| SystemKeys::parse_db_meta_key(k.as_ref()))
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

    fn system_db(&self) -> Result<&DBInstance, ConstDBError> {
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

    pub fn get_table(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> Result<TableSettings, ConstDBError> {
        if !self.db_exists(db_name) {
            return Err(ConstDBError::NotFound(format!(
                "db [{}] not found!",
                db_name
            )));
        }
        let system_db = self.system_db()?;
        let table_meta_key = SystemKeys::table_meta_key(db_name, table_name);
        let result = system_db.rocks_db()?.get(table_meta_key.as_key())?;
        match result {
            None => Err(ConstDBError::NotFound(format!(
                "table not exists [{}.{}]!",
                db_name, table_name
            ))),
            Some(value) => Ok(TableSettings::parse_from_bytes(&value)?),
        }
    }

    pub fn list_table(&self, db_name: &str) -> Result<Vec<TableSettings>, ConstDBError> {
        if !self.db_exists(db_name) {
            return Err(ConstDBError::NotFound(format!(
                "db [{}] not found!",
                db_name
            )));
        }

        let system_db = self.system_db()?;
        let prefix_key = SystemKeys::table_meta_prefix(db_name);
        let prefix = prefix_key.as_key();
        let table_meta_iter = system_db.rocks_db()?.iterator(rocksdb::IteratorMode::From(
            prefix.as_ref(),
            Direction::Forward,
        ));
        let mut table_items = Vec::new();
        for (k, v) in table_meta_iter {
            let try_table_meta_key = SystemKeys::parse_table_meta_key(k.as_ref());
            if !try_table_meta_key.is_ok() {
                break;
            }
            let settings = TableSettings::parse_from_bytes(v.as_ref())?;
            table_items.push(settings);
        }
        Ok(table_items)
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
        input: &TableSettings,
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
        system_db.rocks_db()?.put(
            SystemKeys::table_meta_key(db_name, input.name.as_str()).as_key(),
            input.write_to_bytes()?,
        )?;
        Ok(())
    }

    pub fn get_by_key(
        &self,
        db_name: &str,
        table_name: &str,
        params: HashMap<String, String>,
    ) -> Result<String, ConstDBError> {
        let table = self.get_table(db_name, table_name)?;
        let pkey = table
            .partition_keys
            .iter()
            .map(|k| params.get(k))
            .map(|v| match v {
                Some(v) => v.to_owned(),
                None => "".to_owned(),
            })
            .fold(String::new(), |mut s, v| {
                s.push_str(v.as_ref());
                s
            });

        let db = self.dbs.get(db_name).ok_or(ConstDBError::NotFound(format!(
            "database [{}] not found.",
            db_name
        )))?;

        let opt_value = db.rocks_db()?.get(pkey)?;
        match opt_value {
            Some(v) => Ok(String::from_utf8(v).unwrap()),
            None => Err(ConstDBError::NotFound("key not found!".to_owned())),
        }
    }
    pub fn insert(&self, db_name: &str, table_name: &str, data: Bytes) -> Result<(), ConstDBError> {
        let v: Value = serde_json::from_slice(&data).map_err(|e| {
            ConstDBError::InvalidArguments(format!("cannot deserialize request body: {}", e))
        })?;
        let try_primary_key = match v {
            Value::Object(object) => {
                let table = self.get_table(db_name, table_name)?;
                let primary_key = table
                    .partition_keys
                    .iter()
                    .map(|k| object.get(k))
                    .map(|v| match v {
                        None => "".to_owned(),
                        Some(Value::String(v)) => v.to_owned(),
                        Some(_) => panic!("TODO: not supported value type"),
                    })
                    .fold(String::new(), |mut s, v| {
                        s.push_str(v.as_ref());
                        s
                    });
                Ok(primary_key)
            }
            _ => Err(ConstDBError::InvalidArguments(
                "only json object are supported by now.".to_owned(),
            )),
        };

        let primary_key = try_primary_key?;
        let db = self.dbs.get(db_name).ok_or(ConstDBError::NotFound(format!(
            "database [{}] not found.",
            db_name
        )))?;

        let _table = db.rocks_db()?.put(primary_key, &data)?;
        Ok(())
    }

    fn open(&self, name: &str) -> Result<DBInstance, ConstDBError> {
        let path = Path::new(self.settings.root.as_str()).join(name);
        std::fs::create_dir_all(&path)?;
        let mut db = DBInstance::new(name, path.to_str().unwrap());
        if "system".eq_ignore_ascii_case(name) {
            db.open_rocks_db()?;
        } else {
            db.try_open_rocks_db()?;
        };
        Ok(db)
    }
}

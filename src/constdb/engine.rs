use crate::constdb::system_db::*;
use std::{collections::HashMap, path::Path};

use axum::body::Bytes;
use protobuf::Message;
use rocksdb::{Direction, Options, ReadOptions, DB};

use crate::protos::constdb_model::{DBSettings, TableSettings};

use crate::constdb::{db::DBInstance, errors::ConstDBError, schema::SchemaHelper};

use super::{Id, PrimaryKey};

/// ConstDB settings
pub struct Settings {
    pub root: String,
}

pub struct Engine {
    dbs: HashMap<String, DBInstance>,
    settings: Settings,
}

impl Engine {
    pub fn new(settings: Settings) -> Result<Self, ConstDBError> {
        let mut db = Engine {
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
        for try_db_name in tables.into_iter().map(|result_kv| match result_kv {
            Ok((k, _v)) => SystemKeys::parse_db_meta_key(k.as_ref()),
            Err(_) => Err(ConstDBError::InvalidStates(
                "error when scan db names.".to_string(),
            )),
        }) {
            if try_db_name.is_err() {
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

    /// get the system db
    fn system_db(&self) -> Result<&DBInstance, ConstDBError> {
        self.dbs
            .get("system")
            .ok_or_else(|| ConstDBError::InvalidStates("cannot find [system] db".to_owned()))
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

    pub fn create_db(&mut self, name: &str) -> Result<DBSettings, ConstDBError> {
        if self.db_exists(name) {
            return Err(ConstDBError::AlreadyExists(Id::Database(name.to_owned())));
        }
        let db = self.open(name)?;
        self.dbs.insert(name.to_owned(), db);
        let mut db_settings = DBSettings::new();
        db_settings.name = name.to_owned();
        self.system_db()?.rocks_db()?.put(
            SystemKeys::db_meta_key(name).as_key(),
            db_settings.write_to_bytes()?,
        )?;
        let mut db = DBSettings::new();
        db.name = name.to_owned();
        Ok(db)
    }

    pub fn drop_db(&mut self, name: &str) -> Result<(), ConstDBError> {
        match self.dbs.remove(name) {
            Some(db) => {
                DB::destroy(&Options::default(), db.root.as_str())?;
                Ok(())
            }
            None => Err(ConstDBError::NotFound(Id::Database(name.to_owned()))),
        }
    }

    pub fn get_table(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> Result<TableSettings, ConstDBError> {
        if !self.db_exists(db_name) {
            return Err(ConstDBError::NotFound(Id::Database(db_name.to_owned())));
        }
        let system_db = self.system_db()?;
        let table_meta_key = SystemKeys::table_meta_key(db_name, table_name);
        let result = system_db.rocks_db()?.get(table_meta_key.as_key())?;
        match result {
            None => Err(ConstDBError::NotFound(Id::table(db_name, table_name))),
            Some(value) => Ok(TableSettings::parse_from_bytes(&value)?),
        }
    }

    pub fn list_table(&self, db_name: &str) -> Result<Vec<TableSettings>, ConstDBError> {
        if !self.db_exists(db_name) {
            return Err(ConstDBError::NotFound(Id::Database(db_name.to_owned())));
        }

        // FIXME: scan should have start&end keys
        let system_db = self.system_db()?;
        let prefix_key = SystemKeys::table_meta_prefix(db_name);
        let prefix = prefix_key.as_key();
        let table_meta_iter = system_db.rocks_db()?.iterator(rocksdb::IteratorMode::From(
            prefix.as_ref(),
            Direction::Forward,
        ));
        let mut table_items = Vec::new();
        for result_kv in table_meta_iter {
            let (k, v) = result_kv?;
            SystemKeys::parse_table_meta_key(k.as_ref())?;
            let settings = TableSettings::parse_from_bytes(v.as_ref())?;
            table_items.push(settings);
        }
        Ok(table_items)
    }

    pub fn list_db(&self) -> Result<Vec<DBSettings>, ConstDBError> {
        Ok(self
            .dbs
            .keys()
            .map(|k| {
                let mut db = DBSettings::new();
                db.name = k.to_string();
                db
            })
            .collect())
    }

    pub fn create_table(
        &mut self,
        db_name: &str,
        input: &TableSettings,
    ) -> Result<(), ConstDBError> {
        if !self.db_exists(db_name) {
            return Err(ConstDBError::NotFound(Id::Database(db_name.to_owned())));
        }
        if self.table_exists(db_name, input.name.as_str())? {
            return Err(ConstDBError::AlreadyExists(Id::table(
                db_name,
                input.name.as_str(),
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

    pub fn delete_table(&mut self, db_name: &str, table_name: &str) -> Result<(), ConstDBError> {
        if !self.db_exists(db_name) {
            return Err(ConstDBError::NotFound(Id::Database(db_name.to_owned())));
        }

        if !self.table_exists(db_name, table_name)? {
            return Err(ConstDBError::NotFound(Id::table(db_name, table_name)));
        }

        let db = self.dbs.get_mut(db_name).unwrap();
        db.delete_table(table_name)?;
        let system_db = self.system_db()?;
        system_db
            .rocks_db()?
            .delete(SystemKeys::table_meta_key(db_name, table_name).as_key())?;
        Ok(())
    }

    pub fn query_by_key(
        &self,
        db_name: &str,
        table_name: &str,
        params: HashMap<String, String>,
    ) -> Result<String, ConstDBError> {
        let table = self.get_table(db_name, table_name)?;
        let schema = SchemaHelper::new(table);
        let pk = schema.build_pk_from_params(&params)?;
        let db = self
            .dbs
            .get(db_name)
            .ok_or_else(|| ConstDBError::NotFound(Id::Database(db_name.to_owned())))?;

        match pk {
            PrimaryKey::Prefix(prefix) => {
                let iter_mode = rocksdb::IteratorMode::From(prefix.as_slice(), Direction::Forward);
                let mut read_opts = ReadOptions::default();
                Self::build_upper_bound(&prefix)
                    .into_iter()
                    .for_each(|upper_key| read_opts.set_iterate_upper_bound(upper_key));
                let table = db.rocks_db_for_table(table_name)?;
                let rows_iter = db.rocks_db()?.iterator_cf_opt(table, read_opts, iter_mode);
                let mut rows = Vec::new();
                for result_kv in rows_iter {
                    let (_k, v) = result_kv?;
                    rows.push(String::from_utf8(v.into()).unwrap());
                }
                Ok(format!("[{}]", rows.join(",")))
            }
            PrimaryKey::Complete(key) => {
                let table = db.rocks_db_for_table(table_name)?;
                let opt_value = db.rocks_db()?.get_cf(table, key)?;
                match opt_value {
                    Some(v) => Ok(String::from_utf8(v).unwrap()),
                    None => Err(ConstDBError::NotFound(Id::Data)),
                }
            }
        }
    }

    fn build_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
        for pos in (0..prefix.len()).rev() {
            let v = &prefix[pos];
            if *v != 0xFF {
                let mut stop_key = prefix.to_owned();
                stop_key[pos] = v + 1;
                return Some(stop_key);
            }
        }
        None
    }

    pub fn insert(&self, db_name: &str, table_name: &str, data: Bytes) -> Result<(), ConstDBError> {
        let table = self.get_table(db_name, table_name)?;
        let schema = SchemaHelper::new(table);
        let primary_key = schema.build_pk_from_json(&data)?;
        let db = self
            .dbs
            .get(db_name)
            .ok_or_else(|| ConstDBError::NotFound(Id::Database(db_name.to_owned())))?;

        let table = db.rocks_db_for_table(table_name)?;
        db.rocks_db()?
            .put_cf(table, primary_key.complete()?, &data)?;
        Ok(())
    }

    pub fn update(
        &self,
        db_name: &str,
        table_name: &str,
        data: Bytes,
        params: HashMap<String, String>,
    ) -> Result<(), ConstDBError> {
        let table = self.get_table(db_name, table_name)?;
        let schema = SchemaHelper::new(table);
        let primary_key = schema.build_pk_from_params(&params)?;
        let db = self
            .dbs
            .get(db_name)
            .ok_or_else(|| ConstDBError::NotFound(Id::Database(db_name.to_owned())))?;

        let pk = primary_key.complete()?;
        let table = db.rocks_db_for_table(table_name)?;
        let rocks_db = db.rocks_db()?;
        let existing = rocks_db
            .get_cf(table, pk)?
            .ok_or(ConstDBError::NotFound(Id::Data))?;
        let updated = schema.update(&existing, &data)?;
        rocks_db.put_cf(table, pk, updated)?;
        Ok(())
    }

    pub fn delete(
        &self,
        db_name: &str,
        table_name: &str,
        params: HashMap<String, String>,
    ) -> Result<(), ConstDBError> {
        let table = self.get_table(db_name, table_name)?;
        let schema = SchemaHelper::new(table);
        let primary_key = schema.build_pk_from_params(&params)?;
        let db = self
            .dbs
            .get(db_name)
            .ok_or_else(|| ConstDBError::NotFound(Id::Database(db_name.to_owned())))?;

        let table = db.rocks_db_for_table(table_name)?;
        db.rocks_db()?.delete_cf(table, primary_key.complete()?)?;
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

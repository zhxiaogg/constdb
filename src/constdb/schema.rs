use std::collections::HashMap;

use crate::constdb::errors::ConstDBError;
use crate::protos::constdb_model::TableSettings;
use serde_json::{Map, Value};
use warp::hyper::body::Bytes;

use super::PrimaryKey;

pub struct SchemaHelper {
    table_settings: TableSettings,
}

impl SchemaHelper {
    pub fn new(table_settings: TableSettings) -> Self {
        SchemaHelper { table_settings }
    }

    /// extract&build primary key from input data
    pub fn build_pk_from_json(&self, data: &Bytes) -> Result<PrimaryKey, ConstDBError> {
        let json_object = serde_json::from_slice(data)
            .map_err(|e| ConstDBError::from(e))
            .and_then(|json| match json {
                Value::Object(object) => Ok(object),
                _ => Err(ConstDBError::InvalidArguments(
                    "only json object are supported!".to_owned(),
                )),
            })?;
        let mut pk = Vec::new();
        for k in &self.table_settings.partition_keys {
            let bytes = SchemaHelper::read_pk_field_from_json(&json_object, k.as_str())?;
            pk.push(bytes);
        }

        for k in &self.table_settings.sort_keys {
            let bytes = SchemaHelper::read_pk_field_from_json(&json_object, k.name.as_str())?;
            pk.push(bytes);
        }

        let bytes = pk.into_iter().fold(Vec::new(), |mut r, bytes| {
            r.extend(bytes);
            r
        });
        Ok(PrimaryKey::Complete(bytes))
    }

    pub fn build_pk_from_params(
        &self,
        params: &HashMap<String, String>,
    ) -> Result<PrimaryKey, ConstDBError> {
        let mut pk = Vec::new();
        for k in &self.table_settings.partition_keys {
            let bytes = SchemaHelper::read_pk_field_from_params(&params, k.as_str())?;
            pk.push(bytes);
        }

        for k in &self.table_settings.sort_keys {
            let bytes = SchemaHelper::read_pk_field_from_params(&params, k.name.as_str()).ok();
            if bytes.is_none() {
                break;
            }
            pk.push(bytes.unwrap());
        }

        let bytes = pk.iter().fold(Vec::new(), |mut r, bytes| {
            r.extend(*bytes);
            r
        });
        match pk.len()
            < self.table_settings.partition_keys.len() + self.table_settings.sort_keys.len()
        {
            true => Ok(PrimaryKey::Prefix(bytes)),
            false => Ok(PrimaryKey::Complete(bytes)),
        }
    }

    fn read_pk_field_from_params<'a>(
        params: &'a HashMap<String, String>,
        k: &str,
    ) -> Result<&'a [u8], ConstDBError> {
        match params.get(k) {
            Some(s) => Ok(s.as_bytes()),
            _ => {
                return Err(ConstDBError::InvalidArguments(format!(
                    "cannot find partition key: {}",
                    k
                )));
            }
        }
    }

    fn read_pk_field_from_json<'a>(
        json_object: &'a Map<String, Value>,
        k: &str,
    ) -> Result<&'a [u8], ConstDBError> {
        match json_object.get(k) {
            Some(Value::String(s)) => Ok(s.as_bytes()),
            None => Err(ConstDBError::InvalidArguments(format!(
                "cannot find primary key: {}",
                k
            ))),
            _ => Err(ConstDBError::InvalidArguments(format!(
                "unsupported type for partition key {}",
                k
            ))),
        }
    }
}

// #[cfg(test)]
// mod test {
//     use avro_rs::{types::Record, AvroResult, Reader, Schema, Writer};
//
//     use std::io::Read;
//     #[test]
//     pub fn test_json_interop() -> AvroResult<()> {
//         let schema_v1_str = r#"
//             {
//                 "type": "record",
//                 "name": "test",
//                 "fields": [
//                     {"name": "a", "type": "long", "default": 42},
//                     {"name": "b", "type": "string"}
//                 ]
//             }
//         "#;
//         let schema_v1 = Schema::parse_str(schema_v1_str).expect("schema parse error.");
//         let mut writer = Writer::new(&schema_v1, Vec::new());
//         let mut record = Record::new(writer.schema()).unwrap();
//         record.put("a", 12_i64);
//         record.put("b", "b");
//         let written = writer.append(record).expect("append record");
//         println!("{} bytes written.", written);
//         let bytes = writer.into_inner().expect("get written bytes");
//         let schema_v2_str = r#"
//             {
//                 "type": "record",
//                 "name": "test",
//                 "fields": [
//                     {"name": "a", "type": "long", "default": 42},
//                     {"name": "b", "type": "string"},
//                     {"name": "c", "type": ["long","null"], "default": null}
//                 ]
//             }
//         "#;
//         let schema_v2 = Schema::parse_str(schema_v2_str).expect("schema parse error.");
//         // let reader = Reader::with_schema(&schema_v2, bytes.as_slice())?;
//         let reader = Reader::new(bytes.as_slice())?;
//         for value in reader {
//             println!("read value: {:?}", value.unwrap().resolve(&schema_v2));
//         }
//         Ok(())
//     }
// }

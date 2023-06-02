use std::collections::HashMap;

use crate::protos::constdb_model::{DataType, TableSettings};
use crate::{constdb::errors::ConstDBError, protos::constdb_model::Field};
use axum::body::Bytes;
use serde_json::{Map, Value};

use super::PrimaryKey;

pub struct SchemaHelper {
    table_settings: TableSettings,
}

impl SchemaHelper {
    pub fn new(table_settings: TableSettings) -> Self {
        SchemaHelper { table_settings }
    }

    pub fn update(&self, old: &[u8], patch: &[u8]) -> Result<Bytes, ConstDBError> {
        let mut old_object = Self::get_json_object(old)?;
        let patch_object = Self::get_json_object(patch)?;
        // updating
        for (k, v) in patch_object {
            old_object.insert(k, v);
        }

        let bytes = serde_json::to_string(&old_object)?.into_bytes();
        Ok(Bytes::from(bytes))
    }

    fn get_json_object(data: &[u8]) -> Result<Map<String, Value>, ConstDBError> {
        serde_json::from_slice(data)
            .map_err(ConstDBError::from)
            .and_then(|json| match json {
                Value::Object(object) => Ok(object),
                _ => Err(ConstDBError::InvalidArguments(
                    "only json object are supported!".to_owned(),
                )),
            })
    }
    /// extract&build primary key from input data
    pub fn build_pk_from_json(&self, data: &Bytes) -> Result<PrimaryKey, ConstDBError> {
        let json_object = Self::get_json_object(data)?;
        let mut pk = Vec::new();
        for k in &self.table_settings.primary_keys {
            let bytes = SchemaHelper::read_pk_field_from_json(&json_object, k)?;
            if bytes.is_none() {
                break;
            }
            pk.push(bytes.unwrap());
        }

        let bytes = pk.iter().fold(Vec::new(), |mut r, bytes| {
            r.extend(bytes);
            r.push(0);
            r
        });
        match pk.len() < self.table_settings.primary_keys.len() {
            true => Ok(PrimaryKey::Prefix(bytes)),
            false => Ok(PrimaryKey::Complete(bytes)),
        }
    }

    pub fn build_pk_from_params(
        &self,
        params: &HashMap<String, String>,
    ) -> Result<PrimaryKey, ConstDBError> {
        let mut pk = Vec::new();
        for k in &self.table_settings.primary_keys {
            let bytes = SchemaHelper::read_pk_field_from_params(params, k)?;
            if bytes.is_none() {
                break;
            }
            pk.push(bytes.unwrap());
        }

        let bytes = pk.iter().fold(Vec::new(), |mut r, bytes| {
            r.extend(bytes);
            r.push(0);
            r
        });
        match pk.len() < self.table_settings.primary_keys.len() {
            true => Ok(PrimaryKey::Prefix(bytes)),
            false => Ok(PrimaryKey::Complete(bytes)),
        }
    }

    fn read_pk_field_from_params(
        params: &HashMap<String, String>,
        k: &Field,
    ) -> Result<Option<Vec<u8>>, ConstDBError> {
        match params.get(k.name.as_str()) {
            Some(s) => Self::cast_field_data_type(&Value::String(s.to_string()), k),
            None => Ok(None),
        }
    }

    fn cast_field_data_type(value: &Value, k: &Field) -> Result<Option<Vec<u8>>, ConstDBError> {
        match (value, k.data_type.enum_value_or(DataType::Unknown)) {
            (Value::String(v), DataType::String) => Ok(Some(v.as_bytes().to_vec())),
            (Value::String(v), DataType::Boolean) => {
                if v.eq_ignore_ascii_case("true") {
                    Ok(Some(vec![0x01]))
                } else if v.eq_ignore_ascii_case("false") {
                    Ok(Some(vec![0x00]))
                } else {
                    Err(ConstDBError::InvalidArguments(format!(
                        "Invalid value for primary key: {}",
                        k
                    )))
                }
            }
            (Value::String(v), DataType::DateTime) => Ok(Some(v.as_bytes().to_vec())),
            (Value::String(v), DataType::Int32) => {
                let i = v.parse::<i32>().map_err(|_| {
                    ConstDBError::InvalidArguments(format!(
                        "Primary key {} cannot be cast to Int32.",
                        k
                    ))
                })?;
                Ok(Some(i.to_be_bytes().to_vec()))
            }
            (Value::String(v), DataType::Int64) => {
                let i = v.parse::<i64>().map_err(|_| {
                    ConstDBError::InvalidArguments(format!(
                        "Primary key {} cannot be cast to Int64.",
                        k
                    ))
                })?;
                Ok(Some(i.to_be_bytes().to_vec()))
            }
            (Value::String(v), DataType::Float32) => {
                let f = v.parse::<f32>().map_err(|_| {
                    ConstDBError::InvalidArguments(format!(
                        "Primary key {} cannot be cast to Float32.",
                        k
                    ))
                })?;
                Ok(Some(f.to_be_bytes().to_vec()))
            }
            (Value::String(v), DataType::Float64) => {
                let f = v.parse::<f64>().map_err(|_| {
                    ConstDBError::InvalidArguments(format!(
                        "Primary key {} cannot be cast to Float64.",
                        k
                    ))
                })?;
                Ok(Some(f.to_be_bytes().to_vec()))
            }
            (Value::Number(v), DataType::Int32) => {
                let num_i64 = v.as_i64().ok_or(ConstDBError::InvalidArguments(format!(
                    "Primary key {} cannot be cast to Int32.",
                    k
                )))?;
                if num_i64 >= i32::MIN as i64 && num_i64 <= i32::MAX as i64 {
                    let num_i32 = num_i64 as i32;
                    Ok(Some(num_i32.to_be_bytes().to_vec()))
                } else {
                    Err(ConstDBError::InvalidArguments(format!(
                        "Invalid value for primary key: {}",
                        k
                    )))
                }
            }
            (Value::Number(v), DataType::Int64) => {
                let i = v.as_i64().ok_or(ConstDBError::InvalidArguments(format!(
                    "Primary key {} cannot be cast to Int64.",
                    k
                )))?;
                Ok(Some(i.to_be_bytes().to_vec()))
            }
            (Value::Number(v), DataType::Float32) => {
                let num_f64 = v.as_f64().ok_or(ConstDBError::InvalidArguments(format!(
                    "Primary key {} cannot be cast to Float32.",
                    k
                )))?;
                if num_f64 >= f32::MIN as f64 && num_f64 <= f32::MAX as f64 {
                    let num_f32 = num_f64 as f32;
                    Ok(Some(num_f32.to_be_bytes().to_vec()))
                } else {
                    Err(ConstDBError::InvalidArguments(format!(
                        "Invalid value for primary key: {}",
                        k
                    )))
                }
            }
            (Value::Number(v), DataType::Float64) => {
                let f = v.as_f64().ok_or(ConstDBError::InvalidArguments(format!(
                    "Primary key {} cannot be cast to Float64.",
                    k
                )))?;
                Ok(Some(f.to_be_bytes().to_vec()))
            }
            (Value::Bool(b), DataType::Boolean) => Ok(Some(vec![*b as u8])),
            _ => Err(ConstDBError::InvalidArguments(format!(
                "unsupported type for primary key {}",
                k
            ))),
        }
    }

    fn read_pk_field_from_json(
        json_object: &Map<String, Value>,
        k: &Field,
    ) -> Result<Option<Vec<u8>>, ConstDBError> {
        match json_object.get(k.name.as_str()) {
            Some(v) => Self::cast_field_data_type(v, k),
            None => Ok(None),
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
//                     {"name": "a", "type": "Int64", "default": 42},
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
//                     {"name": "a", "type": "Int64", "default": 42},
//                     {"name": "b", "type": "string"},
//                     {"name": "c", "type": ["Int64","null"], "default": null}
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

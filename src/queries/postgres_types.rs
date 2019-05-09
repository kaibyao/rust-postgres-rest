use crate::errors::ApiError;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use eui48::MacAddress as Eui48MacAddress;
use postgres::{
    accepts,
    rows::Row,
    types::{FromSql, IsNull, ToSql, Type, MACADDR},
};
use postgres_protocol::types::{macaddr_from_sql, macaddr_to_sql};
use rust_decimal::Decimal;
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::str::FromStr;
use uuid::Uuid;

/// we have to define our own MacAddress type in order for Serde to serialize it properly.
#[derive(Debug, Serialize)]
pub struct MacAddress(Eui48MacAddress);

// mostly copied from the postgres-protocol and postgres-shared libraries
impl FromSql for MacAddress {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<MacAddress, Box<StdError + Sync + Send>> {
        let bytes = macaddr_from_sql(raw)?;
        Ok(MacAddress(Eui48MacAddress::new(bytes)))
    }

    accepts!(MACADDR);
}

impl ToSql for MacAddress {
    fn to_sql(&self, _: &Type, w: &mut Vec<u8>) -> Result<IsNull, Box<dyn StdError + Sync + Send>> {
        let mut bytes = [0; 6];
        bytes.copy_from_slice(self.0.as_bytes());
        macaddr_to_sql(bytes, w);
        Ok(IsNull::No)
    }

    accepts!(MACADDR);
    to_sql_checked!();
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
/// Represents a single column value for a returned row. We have to have an Enum describing column data that is non-nullable vs nullable
pub enum ColumnValue<T: FromSql + ToSql> {
    Nullable(Option<T>),
    NotNullable(T),
}

impl<T: FromSql + ToSql> FromSql for ColumnValue<T> {
    fn accepts(ty: &Type) -> bool {
        <T as FromSql>::accepts(ty)
    }

    fn from_sql(ty: &Type, raw: &[u8]) -> Result<Self, Box<StdError + 'static + Send + Sync>> {
        <T as FromSql>::from_sql(ty, raw).map(ColumnValue::NotNullable)
    }

    fn from_sql_null(_: &Type) -> Result<Self, Box<StdError + Sync + Send>> {
        Ok(ColumnValue::Nullable(None))
    }

    fn from_sql_nullable(
        ty: &Type,
        raw: Option<&[u8]>,
    ) -> Result<Self, Box<StdError + 'static + Send + Sync>> {
        match raw {
            Some(raw_inner) => Self::from_sql(ty, raw_inner),
            None => Self::from_sql_null(ty),
        }
    }
}

impl<T: FromSql + ToSql> ToSql for ColumnValue<T> {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut Vec<u8>,
    ) -> Result<IsNull, Box<dyn StdError + 'static + Send + Sync>> {
        match self {
            ColumnValue::Nullable(val_opt) => val_opt.to_sql(ty, out),
            ColumnValue::NotNullable(val) => val.to_sql(ty, out),
        }
    }

    fn accepts(ty: &Type) -> bool {
        <T as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

impl<T: FromSql + ToSql> ColumnValue<T> {
    pub fn as_inner(&self) -> Result<&T, ()> {
        match self {
            ColumnValue::Nullable(val_opt) => match val_opt {
                Some(val) => Ok(val),
                None => Err(()),
            },
            ColumnValue::NotNullable(val) => Ok(val),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
/// Represents a postgres column's type
pub enum ColumnTypeValue {
    BigInt(ColumnValue<i64>),
    // Bit(bit_vec::BitVec),
    Bool(ColumnValue<bool>),
    ByteA(ColumnValue<Vec<u8>>),
    Char(ColumnValue<String>), // apparently it's a bad practice to use char(n)
    Citext(ColumnValue<String>),
    Date(ColumnValue<NaiveDate>),
    Decimal(ColumnValue<Decimal>),
    Float8(ColumnValue<f64>),
    HStore(ColumnValue<HashMap<String, Option<String>>>),
    Int(ColumnValue<i32>),
    Json(ColumnValue<serde_json::Value>),
    JsonB(ColumnValue<serde_json::Value>),
    MacAddr(ColumnValue<MacAddress>),
    Name(ColumnValue<String>),
    Oid(ColumnValue<u32>),
    Real(ColumnValue<f32>),
    SmallInt(ColumnValue<i16>),
    Text(ColumnValue<String>),
    Time(ColumnValue<NaiveTime>),
    Timestamp(ColumnValue<NaiveDateTime>),
    TimestampTz(ColumnValue<DateTime<Utc>>),
    // Unknown(ColumnValue<String>),
    Uuid(ColumnValue<Uuid>),
    // VarBit(ColumnValue<bit_vec::BitVec>),
    VarChar(ColumnValue<String>),
}

// impl <T: FromSql + ToSql> FromSql for ColumnTypeValue {
//     fn from_sql(ty: &Type, raw: &[u8]) -> Result<Self, Box<StdError + 'static + Send + Sync>> {
//         let cv = ColumnValue::<T>::from_sql(ty, raw);
//     }
// }

/// The field names and their values for a single table row.
pub type RowFields = HashMap<String, ColumnTypeValue>;

/// Analyzes a table postgres row and returns the Rust-equivalent value.
pub fn convert_row_fields(row: &Row) -> Result<RowFields, ApiError> {
    let mut row_fields = HashMap::new();
    for (i, column) in row.columns().iter().enumerate() {
        let column_type_name = column.type_().name();

        row_fields.insert(
            column.name().to_string(),
            match column_type_name {
                "int8" => ColumnTypeValue::BigInt(row.get(i)),
                "bool" => ColumnTypeValue::Bool(row.get(i)),
                "bytea" => {
                    // byte array (binary)
                    ColumnTypeValue::ByteA(row.get(i))
                }
                "bpchar" => ColumnTypeValue::Char(row.get(i)), // char
                "citext" => ColumnTypeValue::Citext(row.get(i)),
                "date" => ColumnTypeValue::Date(row.get(i)),
                "float4" => ColumnTypeValue::Real(row.get(i)),
                "float8" => ColumnTypeValue::Float8(row.get(i)),
                "hstore" => ColumnTypeValue::HStore(row.get(i)),
                "int2" => ColumnTypeValue::SmallInt(row.get(i)),
                "int4" => ColumnTypeValue::Int(row.get(i)), // int
                "json" => ColumnTypeValue::Json(row.get(i)),
                "jsonb" => ColumnTypeValue::JsonB(row.get(i)),
                "macaddr" => ColumnTypeValue::MacAddr(row.get(i)),
                "name" => ColumnTypeValue::Name(row.get(i)),
                // using rust-decimal per discussion at https://www.reddit.com/r/rust/comments/a7frqj/have_anyone_reviewed_any_of_the_decimal_crates/.
                // keep in mind that at the time of this writing, diesel uses bigdecimal
                "numeric" => ColumnTypeValue::Decimal(row.get(i)),
                "oid" => ColumnTypeValue::Oid(row.get(i)),
                "text" => ColumnTypeValue::Text(row.get(i)),
                "time" => ColumnTypeValue::Time(row.get(i)),
                "timestamp" => ColumnTypeValue::Timestamp(row.get(i)),
                "timestamptz" => ColumnTypeValue::TimestampTz(row.get(i)),
                "uuid" => ColumnTypeValue::Uuid(row.get(i)),
                // "varbit" => {
                //     ColumnTypeValue::VarBit(row.get(i))
                // }
                "varchar" => ColumnTypeValue::VarChar(row.get(i)),
                _ => {
                    return Err(ApiError::generate_error(
                        "UNSUPPORTED_DATA_TYPE",
                        format!(
                            "Column {} has unsupported type: {}",
                            column.name(),
                            column_type_name
                        ),
                    ))
                }
            },
        );
    }

    Ok(row_fields)
}

pub fn convert_json_value_to_rust(
    column_type: &str,
    value: &Value,
) -> Result<ColumnTypeValue, ApiError> {
    match column_type {
        "int8" => match value.as_i64() {
            Some(val) => Ok(ColumnTypeValue::BigInt(ColumnValue::NotNullable(val))),
            None => Err(ApiError::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        },
        "bool" => match value.as_bool() {
            Some(val) => Ok(ColumnTypeValue::Bool(ColumnValue::NotNullable(val))),
            None => Err(ApiError::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        },
        "bytea" => match value.as_array() {
            Some(raw_bytea_json_vec) => {
                let bytea_conversion: Result<Vec<u8>, ApiError> = raw_bytea_json_vec
                    .iter()
                    .map(|json_val| match json_val.as_u64() {
                        Some(bytea_val) => Ok(bytea_val as u8),
                        None => Err(ApiError::generate_error(
                            "INVALID_JSON_TYPE_CONVERSION",
                            format!("Value: `{}`. Column type: `{}`.", value, column_type),
                        )),
                    })
                    .collect();

                match bytea_conversion {
                    Ok(bytea_vec) => {
                        Ok(ColumnTypeValue::ByteA(ColumnValue::NotNullable(bytea_vec)))
                    }
                    Err(e) => Err(e),
                }
            }
            None => Err(ApiError::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        },
        "bpchar" => match value.as_str() {
            Some(val) => Ok(ColumnTypeValue::Char(ColumnValue::NotNullable(
                val.to_string(),
            ))),
            None => Err(ApiError::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        },
        "citext" => match value.as_str() {
            Some(val) => Ok(ColumnTypeValue::Citext(ColumnValue::NotNullable(
                val.to_string(),
            ))),
            None => Err(ApiError::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        },
        "date" => match value.as_str() {
            Some(val) => match NaiveDate::from_str(val) {
                Ok(date) => Ok(ColumnTypeValue::Date(ColumnValue::NotNullable(date))),
                Err(e) => Err(ApiError::generate_error(
                    "INVALID_JSON_TYPE_CONVERSION",
                    format!(
                        "Value: `{}`. Column type: `{}`. Message: `{}`.",
                        value, column_type, e
                    ),
                )),
            },
            None => Err(ApiError::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        },
        "float4" => match value.as_f64() {
            Some(n) => Ok(ColumnTypeValue::Real(ColumnValue::NotNullable(n as f32))),
            None => Err(ApiError::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        },
        "float8" => match value.as_f64() {
            Some(n) => Ok(ColumnTypeValue::Float8(ColumnValue::NotNullable(n))),
            None => Err(ApiError::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        },
        // "hstore" => ColumnTypeValue::HStore(),
        "int2" => match value.as_i64() {
            Some(n) => Ok(ColumnTypeValue::SmallInt(ColumnValue::NotNullable(
                n as i16,
            ))),
            None => Err(ApiError::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        },
        "int4" => match value.as_i64() {
            Some(n) => Ok(ColumnTypeValue::Int(ColumnValue::NotNullable(n as i32))),
            None => Err(ApiError::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        },
        "json" => Ok(ColumnTypeValue::Json(ColumnValue::NotNullable(
            value.clone(),
        ))),
        "jsonb" => Ok(ColumnTypeValue::JsonB(ColumnValue::NotNullable(
            value.clone(),
        ))),
        // "macaddr" => match value.as_str() { // temporarily commenting this out to avoid clippy errors
        //     Some(val) => match Eui48MacAddress::from_str(val) {
        //         Ok(mac) => Ok(ColumnTypeValue::MacAddr(ColumnValue::NotNullable(
        //             MacAddress(mac),
        //         ))),
        //         Err(e) => Err(ApiError::generate_error(
        //             "INVALID_JSON_TYPE_CONVERSION",
        //             format!(
        //                 "Value: `{}`. Column type: `{}`. Message: `{}`.",
        //                 value, column_type, e
        //             ),
        //         )),
        //     },
        //     None => Err(ApiError::generate_error(
        //         "INVALID_JSON_TYPE_CONVERSION",
        //         format!("Value: `{}`. Column type: `{}`.", value, column_type),
        //     )),
        // },
        "name" => match value.as_str() {
            Some(val) => Ok(ColumnTypeValue::Name(ColumnValue::NotNullable(
                val.to_string(),
            ))),
            None => Err(ApiError::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        },
        // // using rust-decimal per discussion at https://www.reddit.com/r/rust/comments/a7frqj/have_anyone_reviewed_any_of_the_decimal_crates/.
        // // keep in mind that at the time of this writing, diesel uses bigdecimal
        // "numeric" => ColumnTypeValue::Decimal(row.get(i)),
        // "oid" => ColumnTypeValue::Oid(row.get(i)),
        "text" => match value.as_str() {
            Some(val) => Ok(ColumnTypeValue::Text(ColumnValue::NotNullable(
                val.to_string(),
            ))),
            None => Err(ApiError::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        },
        "time" => match value.as_str() {
            Some(val) => match NaiveTime::from_str(val) {
                Ok(time) => Ok(ColumnTypeValue::Time(ColumnValue::NotNullable(time))),
                Err(e) => Err(ApiError::generate_error(
                    "INVALID_JSON_TYPE_CONVERSION",
                    format!(
                        "Value: `{}`. Column type: `{}`. Message: `{}`.",
                        value, column_type, e
                    ),
                )),
            },
            None => Err(ApiError::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        },
        "timestamp" => match value.as_str() {
            Some(val) => match NaiveDateTime::from_str(val) {
                Ok(timestamp) => Ok(ColumnTypeValue::Timestamp(ColumnValue::NotNullable(
                    timestamp,
                ))),
                Err(e) => Err(ApiError::generate_error(
                    "INVALID_JSON_TYPE_CONVERSION",
                    format!(
                        "Value: `{}`. Column type: `{}`. Message: `{}`.",
                        value, column_type, e
                    ),
                )),
            },
            None => Err(ApiError::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        },
        "timestamptz" => match value.as_str() {
            Some(val) => match DateTime::from_str(val) {
                Ok(timestamptz) => Ok(ColumnTypeValue::TimestampTz(ColumnValue::NotNullable(
                    timestamptz,
                ))),
                Err(e) => Err(ApiError::generate_error(
                    "INVALID_JSON_TYPE_CONVERSION",
                    format!(
                        "Value: `{}`. Column type: `{}`. Message: `{}`.",
                        value, column_type, e
                    ),
                )),
            },
            None => Err(ApiError::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        },
        "uuid" => match value.as_str() {
            Some(val) => match Uuid::parse_str(val) {
                Ok(uuid_val) => Ok(ColumnTypeValue::Uuid(ColumnValue::NotNullable(uuid_val))),
                Err(e) => Err(ApiError::generate_error(
                    "INVALID_JSON_TYPE_CONVERSION",
                    format!(
                        "Value: `{}`. Column type: `{}`. Message: `{}`.",
                        value, column_type, e
                    ),
                )),
            },
            None => Err(ApiError::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        },
        "varchar" => match value.as_str() {
            Some(val) => Ok(ColumnTypeValue::VarChar(ColumnValue::NotNullable(
                val.to_string(),
            ))),
            None => Err(ApiError::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        },
        _ => Err(ApiError::generate_error(
            "UNSUPPORTED_DATA_TYPE",
            format!("Value {} has unsupported type: {}", value, column_type),
        )),
    }
}

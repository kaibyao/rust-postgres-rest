use crate::Error;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use eui48::MacAddress as Eui48MacAddress;
use failure::Fail;
use postgres_protocol::types::macaddr_to_sql;
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::Value as JsonValue;
use sqlparser::ast::{Expr, Function, UnaryOperator, Value as SqlValue};
use std::{
    borrow::{Borrow, BorrowMut},
    collections::HashMap,
    error::Error as StdError,
    fmt, mem,
    str::FromStr,
};
use tokio_postgres::{
    accepts,
    row::Row,
    to_sql_checked,
    types::{FromSql, IsNull, ToSql, Type},
};
use uuid::Uuid;

/// we have to define our own MacAddress type in order for Serde to serialize it properly.
#[derive(Debug, PartialEq, Serialize)]
pub struct MacAddress(Eui48MacAddress);

// mostly copied from the postgres-protocol and postgres-shared libraries
impl<'a> FromSql<'a> for MacAddress {
    fn from_sql(typ: &Type, raw: &[u8]) -> Result<MacAddress, Box<dyn StdError + Sync + Send>> {
        let mac = <Eui48MacAddress as FromSql>::from_sql(typ, raw)?;
        Ok(MacAddress(mac))
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

#[derive(Debug, PartialEq, Serialize)]
#[serde(untagged)]
/// Represents a single column value for a returned row. We have to have an Enum describing column
/// data that is non-nullable vs nullable
pub enum IsNullColumnValue<T> {
    Nullable(Option<T>),
    NotNullable(T),
}

impl<'a, T> FromSql<'a> for IsNullColumnValue<T>
where
    T: FromSql<'a>,
{
    fn accepts(ty: &Type) -> bool {
        <T as FromSql>::accepts(ty)
    }

    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn StdError + 'static + Send + Sync>> {
        <T as FromSql>::from_sql(ty, raw).map(IsNullColumnValue::NotNullable)
    }

    fn from_sql_null(_: &Type) -> Result<Self, Box<dyn StdError + Sync + Send>> {
        Ok(IsNullColumnValue::Nullable(None))
    }

    fn from_sql_nullable(
        ty: &Type,
        raw: Option<&'a [u8]>,
    ) -> Result<Self, Box<dyn StdError + 'static + Send + Sync>> {
        match raw {
            Some(raw_inner) => Self::from_sql(ty, raw_inner),
            None => Self::from_sql_null(ty),
        }
    }
}

impl<T> ToSql for IsNullColumnValue<T>
where
    T: ToSql,
{
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut Vec<u8>,
    ) -> Result<IsNull, Box<dyn StdError + 'static + Send + Sync>> {
        match self {
            IsNullColumnValue::Nullable(val_opt) => val_opt.to_sql(ty, out),
            IsNullColumnValue::NotNullable(val) => val.to_sql(ty, out),
        }
    }

    fn accepts(ty: &Type) -> bool {
        <T as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

#[derive(Debug, PartialEq, Serialize)]
#[serde(untagged)]
/// Represents a postgres column's type
pub enum TypedColumnValue {
    BigInt(IsNullColumnValue<i64>),
    Bool(IsNullColumnValue<bool>),
    ByteA(IsNullColumnValue<Vec<u8>>),
    Char(IsNullColumnValue<String>), // apparently it's a bad practice to use char(n)
    Citext(IsNullColumnValue<String>),
    Date(IsNullColumnValue<NaiveDate>),
    Decimal(IsNullColumnValue<Decimal>),
    Float8(IsNullColumnValue<f64>),
    Int(IsNullColumnValue<i32>),
    Json(IsNullColumnValue<JsonValue>),
    JsonB(IsNullColumnValue<JsonValue>),
    MacAddr(IsNullColumnValue<MacAddress>),
    Name(IsNullColumnValue<String>),
    Oid(IsNullColumnValue<u32>),
    Real(IsNullColumnValue<f32>),
    SmallInt(IsNullColumnValue<i16>),
    Text(IsNullColumnValue<String>),
    Time(IsNullColumnValue<NaiveTime>),
    Timestamp(IsNullColumnValue<NaiveDateTime>),
    TimestampTz(IsNullColumnValue<DateTime<Utc>>),
    // Unknown(IsNullColumnValue<String>),
    Uuid(IsNullColumnValue<Uuid>),
    VarChar(IsNullColumnValue<String>),
}

impl<'a> FromSql<'a> for TypedColumnValue {
    fn accepts(ty: &Type) -> bool {
        match ty.name() {
            "int8" => <IsNullColumnValue<i64> as FromSql>::accepts(ty),
            "bool" => <IsNullColumnValue<bool> as FromSql>::accepts(ty),
            "bytea" => <IsNullColumnValue<Vec<u8>> as FromSql>::accepts(ty),
            "bpchar" => <IsNullColumnValue<String> as FromSql>::accepts(ty),
            "citext" => <IsNullColumnValue<String> as FromSql>::accepts(ty),
            "date" => <IsNullColumnValue<NaiveDate> as FromSql>::accepts(ty),
            "float4" => <IsNullColumnValue<f32> as FromSql>::accepts(ty),
            "float8" => <IsNullColumnValue<f64> as FromSql>::accepts(ty),
            "int2" => <IsNullColumnValue<i16> as FromSql>::accepts(ty),
            "int4" => <IsNullColumnValue<i32> as FromSql>::accepts(ty),
            "json" => <IsNullColumnValue<JsonValue> as FromSql>::accepts(ty),
            "jsonb" => <IsNullColumnValue<JsonValue> as FromSql>::accepts(ty),
            "macaddr" => <IsNullColumnValue<MacAddress> as FromSql>::accepts(ty),
            "name" => <IsNullColumnValue<String> as FromSql>::accepts(ty),
            "numeric" => <IsNullColumnValue<Decimal> as FromSql>::accepts(ty),
            "oid" => <IsNullColumnValue<u32> as FromSql>::accepts(ty),
            "text" => <IsNullColumnValue<String> as FromSql>::accepts(ty),
            "time" => <IsNullColumnValue<NaiveTime> as FromSql>::accepts(ty),
            "timestamp" => <IsNullColumnValue<NaiveDateTime> as FromSql>::accepts(ty),
            "timestamptz" => <IsNullColumnValue<DateTime<Utc>> as FromSql>::accepts(ty),
            "uuid" => <IsNullColumnValue<Uuid> as FromSql>::accepts(ty),
            "varchar" => <IsNullColumnValue<String> as FromSql>::accepts(ty),
            &_ => false,
        }
    }

    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn StdError + 'static + Send + Sync>> {
        match ty.name() {
            "int8" => Ok(Self::BigInt(<IsNullColumnValue<i64> as FromSql>::from_sql(
                ty, raw,
            )?)),
            "bool" => Ok(Self::Bool(<IsNullColumnValue<bool> as FromSql>::from_sql(
                ty, raw,
            )?)),
            "bytea" => Ok(Self::ByteA(
                <IsNullColumnValue<Vec<u8>> as FromSql>::from_sql(ty, raw)?,
            )),
            "bpchar" => Ok(Self::Char(
                <IsNullColumnValue<String> as FromSql>::from_sql(ty, raw)?,
            )),
            "citext" => Ok(Self::Citext(
                <IsNullColumnValue<String> as FromSql>::from_sql(ty, raw)?,
            )),
            "date" => Ok(Self::Date(
                <IsNullColumnValue<NaiveDate> as FromSql>::from_sql(ty, raw)?,
            )),
            "float4" => Ok(Self::Real(<IsNullColumnValue<f32> as FromSql>::from_sql(
                ty, raw,
            )?)),
            "float8" => Ok(Self::Float8(<IsNullColumnValue<f64> as FromSql>::from_sql(
                ty, raw,
            )?)),
            "int2" => Ok(Self::SmallInt(
                <IsNullColumnValue<i16> as FromSql>::from_sql(ty, raw)?,
            )),
            "int4" => Ok(Self::Int(<IsNullColumnValue<i32> as FromSql>::from_sql(
                ty, raw,
            )?)),
            "json" => Ok(Self::Json(
                <IsNullColumnValue<JsonValue> as FromSql>::from_sql(ty, raw)?,
            )),
            "jsonb" => Ok(Self::JsonB(
                <IsNullColumnValue<JsonValue> as FromSql>::from_sql(ty, raw)?,
            )),
            "macaddr" => Ok(Self::MacAddr(
                <IsNullColumnValue<MacAddress> as FromSql>::from_sql(ty, raw)?,
            )),
            "name" => Ok(Self::Name(
                <IsNullColumnValue<String> as FromSql>::from_sql(ty, raw)?,
            )),
            "numeric" => Ok(Self::Decimal(
                <IsNullColumnValue<Decimal> as FromSql>::from_sql(ty, raw)?,
            )),
            "oid" => Ok(Self::Oid(<IsNullColumnValue<u32> as FromSql>::from_sql(
                ty, raw,
            )?)),
            "text" => Ok(Self::Text(
                <IsNullColumnValue<String> as FromSql>::from_sql(ty, raw)?,
            )),
            "time" => Ok(Self::Time(
                <IsNullColumnValue<NaiveTime> as FromSql>::from_sql(ty, raw)?,
            )),
            "timestamp" => Ok(Self::Timestamp(
                <IsNullColumnValue<NaiveDateTime> as FromSql>::from_sql(ty, raw)?,
            )),
            "timestamptz" => Ok(Self::TimestampTz(
                <IsNullColumnValue<DateTime<Utc>> as FromSql>::from_sql(ty, raw)?,
            )),
            "uuid" => Ok(Self::Uuid(<IsNullColumnValue<Uuid> as FromSql>::from_sql(
                ty, raw,
            )?)),
            "varchar" => Ok(Self::VarChar(
                <IsNullColumnValue<String> as FromSql>::from_sql(ty, raw)?,
            )),
            &_ => Err(Box::new(
                Error::generate_error("TABLE_COLUMN_TYPE_NOT_FOUND", ty.name().to_string())
                    .compat(),
            )),
        }
    }

    fn from_sql_null(_: &Type) -> Result<Self, Box<dyn StdError + Sync + Send>> {
        Ok(Self::BigInt(IsNullColumnValue::Nullable(None)))
    }

    fn from_sql_nullable(
        ty: &Type,
        raw: Option<&'a [u8]>,
    ) -> Result<Self, Box<dyn StdError + 'static + Send + Sync>> {
        match raw {
            Some(raw_inner) => Self::from_sql(ty, raw_inner),
            None => Self::from_sql_null(ty),
        }
    }
}

impl ToSql for TypedColumnValue {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut Vec<u8>,
    ) -> Result<IsNull, Box<dyn StdError + 'static + Send + Sync>> {
        match self {
            Self::BigInt(col_val) => col_val.to_sql(ty, out),
            Self::Bool(col_val) => col_val.to_sql(ty, out),
            Self::ByteA(col_val) => col_val.to_sql(ty, out),
            Self::Char(col_val) => col_val.to_sql(ty, out),
            Self::Citext(col_val) => col_val.to_sql(ty, out),
            Self::Date(col_val) => col_val.to_sql(ty, out),
            Self::Decimal(col_val) => col_val.to_sql(ty, out),
            Self::Float8(col_val) => col_val.to_sql(ty, out),
            Self::Int(col_val) => col_val.to_sql(ty, out),
            Self::Json(col_val) => col_val.to_sql(ty, out),
            Self::JsonB(col_val) => col_val.to_sql(ty, out),
            Self::MacAddr(col_val) => col_val.to_sql(ty, out),
            Self::Name(col_val) => col_val.to_sql(ty, out),
            Self::Oid(col_val) => col_val.to_sql(ty, out),
            Self::Real(col_val) => col_val.to_sql(ty, out),
            Self::SmallInt(col_val) => col_val.to_sql(ty, out),
            Self::Text(col_val) => col_val.to_sql(ty, out),
            Self::Time(col_val) => col_val.to_sql(ty, out),
            Self::Timestamp(col_val) => col_val.to_sql(ty, out),
            Self::TimestampTz(col_val) => col_val.to_sql(ty, out),
            Self::Uuid(col_val) => col_val.to_sql(ty, out),
            Self::VarChar(col_val) => col_val.to_sql(ty, out),
        }
    }

    fn accepts(ty: &Type) -> bool {
        match ty.name() {
            "int8" => <IsNullColumnValue<i64> as ToSql>::accepts(ty),
            "bool" => <IsNullColumnValue<bool> as ToSql>::accepts(ty),
            "bytea" => <IsNullColumnValue<Vec<u8>> as ToSql>::accepts(ty),
            "bpchar" => <IsNullColumnValue<String> as ToSql>::accepts(ty),
            "citext" => <IsNullColumnValue<String> as ToSql>::accepts(ty),
            "date" => <IsNullColumnValue<NaiveDate> as ToSql>::accepts(ty),
            "float4" => <IsNullColumnValue<f32> as ToSql>::accepts(ty),
            "float8" => <IsNullColumnValue<f64> as ToSql>::accepts(ty),
            "int2" => <IsNullColumnValue<i16> as ToSql>::accepts(ty),
            "int4" => <IsNullColumnValue<i32> as ToSql>::accepts(ty),
            "json" => <IsNullColumnValue<JsonValue> as ToSql>::accepts(ty),
            "jsonb" => <IsNullColumnValue<JsonValue> as ToSql>::accepts(ty),
            "macaddr" => <IsNullColumnValue<MacAddress> as ToSql>::accepts(ty),
            "name" => <IsNullColumnValue<String> as ToSql>::accepts(ty),
            "numeric" => <IsNullColumnValue<Decimal> as ToSql>::accepts(ty),
            "oid" => <IsNullColumnValue<u32> as ToSql>::accepts(ty),
            "text" => <IsNullColumnValue<String> as ToSql>::accepts(ty),
            "time" => <IsNullColumnValue<NaiveTime> as ToSql>::accepts(ty),
            "timestamp" => <IsNullColumnValue<NaiveDateTime> as ToSql>::accepts(ty),
            "timestamptz" => <IsNullColumnValue<DateTime<Utc>> as ToSql>::accepts(ty),
            "uuid" => <IsNullColumnValue<Uuid> as ToSql>::accepts(ty),
            "varchar" => <IsNullColumnValue<String> as ToSql>::accepts(ty),
            &_ => false,
        }
    }

    to_sql_checked!();
}

impl TypedColumnValue {
    /// Parses a Value and returns the Rust-Typed version.
    pub fn from_json(column_type: &str, value: &JsonValue) -> Result<Self, Error> {
        match column_type {
            "int8" => Self::convert_json_value_to_bigint(value),
            "bool" => Self::convert_json_value_to_bool(value),
            "bytea" => Self::convert_json_value_to_bytea(value),
            "bpchar" => Self::convert_json_value_to_char(value),
            "citext" => Self::convert_json_value_to_citext(value),
            "date" => Self::convert_json_value_to_date(value),
            "float4" => Self::convert_json_value_to_real(value),
            "float8" => Self::convert_json_value_to_float8(value),
            "int2" => Self::convert_json_value_to_smallint(value),
            "int4" => Self::convert_json_value_to_int(value),
            "json" => Self::convert_json_value_to_json(value),
            "jsonb" => Self::convert_json_value_to_jsonb(value),
            "macaddr" => Self::convert_json_value_to_macaddr(value),
            "name" => Self::convert_json_value_to_name(value),
            "numeric" => Self::convert_json_value_to_decimal(value),
            "oid" => Self::convert_json_value_to_oid(value),
            "text" => Self::convert_json_value_to_text(value),
            "time" => Self::convert_json_value_to_time(value),
            "timestamp" => Self::convert_json_value_to_timestamp(value),
            "timestamptz" => Self::convert_json_value_to_timestamptz(value),
            "uuid" => Self::convert_json_value_to_uuid(value),
            "varchar" => Self::convert_json_value_to_varchar(value),
            _ => Err(Error::generate_error(
                "UNSUPPORTED_DATA_TYPE",
                format!("Value {} has unsupported type: {}", value, column_type),
            )),
        }
    }

    pub fn from_parsed_sql_value(column_type: &str, value: ParsedSQLValue) -> Result<Self, Error> {
        match column_type {
            "int8" => Ok(TypedColumnValue::BigInt(match value {
                ParsedSQLValue::Int8(val) => IsNullColumnValue::NotNullable(val),
                ParsedSQLValue::Null => IsNullColumnValue::Nullable(None),
                _ => unimplemented!("Cannot convert from ParsedSQLValue: `{}` to int8.", value),
            })),
            "bool" => Ok(TypedColumnValue::Bool(match value {
                ParsedSQLValue::Boolean(val) => IsNullColumnValue::NotNullable(val),
                ParsedSQLValue::Null => IsNullColumnValue::Nullable(None),
                _ => unimplemented!("Cannot convert from ParsedSQLValue: `{}` to bool.", value),
            })),
            "bytea" => Ok(TypedColumnValue::ByteA(match value {
                ParsedSQLValue::Null => IsNullColumnValue::Nullable(None),
                ParsedSQLValue::String(val) => IsNullColumnValue::NotNullable(val.into_bytes()),
                _ => unimplemented!("Cannot convert from ParsedSQLValue: `{}` to bytea.", value),
            })),
            "bpchar" => Ok(TypedColumnValue::Char(match value {
                ParsedSQLValue::Null => IsNullColumnValue::Nullable(None),
                ParsedSQLValue::String(val) => IsNullColumnValue::NotNullable(val),
                _ => unimplemented!("Cannot convert from ParsedSQLValue: `{}` to bpchar.", value),
            })),
            "citext" => Ok(TypedColumnValue::Citext(match value {
                ParsedSQLValue::Null => IsNullColumnValue::Nullable(None),
                ParsedSQLValue::String(val) => IsNullColumnValue::NotNullable(val),
                _ => unimplemented!("Cannot convert from ParsedSQLValue: `{}` to citext.", value),
            })),
            "date" => Ok(TypedColumnValue::Date(match value {
                ParsedSQLValue::Null => IsNullColumnValue::Nullable(None),
                ParsedSQLValue::String(val) => {
                    IsNullColumnValue::NotNullable(NaiveDate::from_str(&val)?)
                }
                _ => unimplemented!("Cannot convert from ParsedSQLValue: `{}` to date.", value),
            })),
            "float4" => Ok(TypedColumnValue::Real(match value {
                ParsedSQLValue::Float(val) => IsNullColumnValue::NotNullable(val as f32),
                ParsedSQLValue::Null => IsNullColumnValue::Nullable(None),
                _ => unimplemented!("Cannot convert from ParsedSQLValue: `{}` to float4.", value),
            })),
            "float8" => Ok(TypedColumnValue::Float8(match value {
                ParsedSQLValue::Float(val) => IsNullColumnValue::NotNullable(val),
                ParsedSQLValue::Null => IsNullColumnValue::Nullable(None),
                _ => unimplemented!("Cannot convert from ParsedSQLValue: `{}` to float8.", value),
            })),
            "int2" => Ok(TypedColumnValue::SmallInt(match value {
                ParsedSQLValue::Int8(val) => IsNullColumnValue::NotNullable(val as i16),
                ParsedSQLValue::Null => IsNullColumnValue::Nullable(None),
                _ => unimplemented!("Cannot convert from ParsedSQLValue: `{}` to int2.", value),
            })),
            "int4" => Ok(TypedColumnValue::Int(match value {
                ParsedSQLValue::Int8(val) => IsNullColumnValue::NotNullable(val as i32),
                ParsedSQLValue::Null => IsNullColumnValue::Nullable(None),
                _ => unimplemented!("Cannot convert from ParsedSQLValue: `{}` to int4.", value),
            })),
            "json" => Ok(TypedColumnValue::Json(match value {
                ParsedSQLValue::Null => IsNullColumnValue::Nullable(None),
                ParsedSQLValue::String(val) => {
                    IsNullColumnValue::NotNullable(serde_json::from_str(&val)?)
                }
                _ => unimplemented!("Cannot convert from ParsedSQLValue: `{}` to json.", value),
            })),
            "jsonb" => Ok(TypedColumnValue::JsonB(match value {
                ParsedSQLValue::Null => IsNullColumnValue::Nullable(None),
                ParsedSQLValue::String(val) => {
                    IsNullColumnValue::NotNullable(serde_json::from_str(&val)?)
                }
                _ => unimplemented!("Cannot convert from ParsedSQLValue: `{}` to json.", value),
            })),
            "macaddr" => Ok(TypedColumnValue::MacAddr(match value {
                ParsedSQLValue::Null => IsNullColumnValue::Nullable(None),
                ParsedSQLValue::String(val) => {
                    IsNullColumnValue::NotNullable(MacAddress(Eui48MacAddress::from_str(&val)?))
                }
                _ => unimplemented!(
                    "Cannot convert from ParsedSQLValue: `{}` to macaddr.",
                    value
                ),
            })),
            "name" => Ok(TypedColumnValue::Name(match value {
                ParsedSQLValue::Null => IsNullColumnValue::Nullable(None),
                ParsedSQLValue::String(val) => IsNullColumnValue::NotNullable(val),
                _ => unimplemented!("Cannot convert from ParsedSQLValue: `{}` to name.", value),
            })),
            "numeric" => Ok(TypedColumnValue::Decimal(match value {
                ParsedSQLValue::Null => IsNullColumnValue::Nullable(None),
                ParsedSQLValue::String(val) => {
                    IsNullColumnValue::NotNullable(Decimal::from_str(&val)?)
                }
                _ => unimplemented!(
                    "Cannot convert from ParsedSQLValue: `{}` to numeric.",
                    value
                ),
            })),
            "oid" => Ok(TypedColumnValue::Oid(match value {
                ParsedSQLValue::Int8(val) => IsNullColumnValue::NotNullable(val as u32),
                ParsedSQLValue::Null => IsNullColumnValue::Nullable(None),
                _ => unimplemented!("Cannot convert from ParsedSQLValue: `{}` to oid.", value),
            })),
            "text" => Ok(TypedColumnValue::Text(match value {
                ParsedSQLValue::Null => IsNullColumnValue::Nullable(None),
                ParsedSQLValue::String(val) => IsNullColumnValue::NotNullable(val),
                _ => unimplemented!("Cannot convert from ParsedSQLValue: `{}` to text.", value),
            })),
            "time" => Ok(TypedColumnValue::Time(match value {
                ParsedSQLValue::Null => IsNullColumnValue::Nullable(None),
                ParsedSQLValue::String(val) => {
                    IsNullColumnValue::NotNullable(NaiveTime::from_str(&val)?)
                }
                _ => unimplemented!("Cannot convert from ParsedSQLValue: `{}` to time.", value),
            })),
            "timestamp" => Ok(TypedColumnValue::Timestamp(match value {
                ParsedSQLValue::Null => IsNullColumnValue::Nullable(None),
                ParsedSQLValue::String(val) => {
                    IsNullColumnValue::NotNullable(NaiveDateTime::from_str(&val)?)
                }
                _ => unimplemented!(
                    "Cannot convert from ParsedSQLValue: `{}` to timestamp.",
                    value
                ),
            })),
            "timestamptz" => Ok(TypedColumnValue::TimestampTz(match value {
                ParsedSQLValue::Null => IsNullColumnValue::Nullable(None),
                ParsedSQLValue::String(val) => {
                    let timestamp = DateTime::from_str(&val)?;
                    IsNullColumnValue::NotNullable(timestamp)
                }
                _ => unimplemented!(
                    "Cannot convert from ParsedSQLValue: `{}` to timestamptz.",
                    value
                ),
            })),
            "uuid" => Ok(TypedColumnValue::Uuid(match value {
                ParsedSQLValue::Null => IsNullColumnValue::Nullable(None),
                ParsedSQLValue::String(val) => {
                    IsNullColumnValue::NotNullable(Uuid::from_str(&val)?)
                }
                _ => unimplemented!("Cannot convert from ParsedSQLValue: `{}` to uuid.", value),
            })),
            "varchar" => Ok(TypedColumnValue::VarChar(match value {
                ParsedSQLValue::Null => IsNullColumnValue::Nullable(None),
                ParsedSQLValue::String(val) => IsNullColumnValue::NotNullable(val),
                _ => unimplemented!(
                    "Cannot convert from ParsedSQLValue: `{}` to varchar.",
                    value
                ),
            })),
            _ => Err(Error::generate_error(
                "UNSUPPORTED_DATA_TYPE",
                format!("Value {} has unsupported type: {}", value, column_type),
            )),
        }
    }

    /// Parses a given AST and returns a tuple: (String [the converted expression that uses PREPARE
    /// parameters], Vec<TypedColumnValue>).
    pub fn generate_prepared_statement_from_ast_expr(
        ast: &Expr,
        table: &str,
        column_types: &HashMap<String, &'static str>,
        starting_pos: Option<&mut usize>,
    ) -> Result<(String, Vec<TypedColumnValue>), Error> {
        let mut ast = ast.clone();
        // mutates `ast`
        let prepared_values =
            Self::generate_prepared_values(&mut ast, table, column_types, starting_pos)?;

        Ok((ast.to_string(), prepared_values))
    }

    /// Extracts the values being assigned and replaces them with prepared statement position
    /// parameters (like `$1`, `$2`, etc.). Returns a Vec of prepared values.
    fn generate_prepared_values(
        ast: &mut Expr,
        table: &str,
        column_types: &HashMap<String, &'static str>,
        prepared_param_pos_opt: Option<&mut usize>,
    ) -> Result<Vec<TypedColumnValue>, Error> {
        let mut prepared_statement_values = vec![];
        let mut default_pos = 1;
        let prepared_param_pos = if let Some(pos) = prepared_param_pos_opt {
            pos
        } else {
            &mut default_pos
        };

        // Attempts to get the column name from an Expr
        let mut get_column_name =
            |possible_column_name_expr: &mut Expr| -> Result<Option<String>, Error> {
                match possible_column_name_expr {
                    Expr::Identifier(non_nested_column_name) => {
                        let column_name = non_nested_column_name.clone();
                        // prepend column with table prefix
                        *non_nested_column_name = [table, non_nested_column_name].join(".");
                        Ok(Some(column_name))
                    }
                    Expr::CompoundIdentifier(nested_fk_column_vec) => {
                        Ok(Some(nested_fk_column_vec.join(".")))
                    }
                    _ => {
                        prepared_statement_values.extend(Self::generate_prepared_values(
                            possible_column_name_expr,
                            table,
                            column_types,
                            Some(prepared_param_pos),
                        )?);
                        Ok(None)
                    }
                }
            };

        // every time there's a BinaryOp, InList, or UnaryOp extract the value
        let mut ast_temp_replace = mem::replace(ast, Expr::Wildcard);
        match &mut ast_temp_replace {
            Expr::BinaryOp {
                left: bin_left_ast_box,
                right: bin_right_ast_box,
                ..
            } => {
                let column_name_opt = get_column_name(bin_left_ast_box.borrow_mut())?;
                let expr = bin_right_ast_box.borrow_mut();
                if let Some(ast_replacement) = Self::attempt_prepared_value_extraction(
                    table,
                    column_types,
                    prepared_param_pos,
                    &column_name_opt,
                    expr,
                    &mut prepared_statement_values,
                )? {
                    *expr = ast_replacement;
                };
            }
            Expr::InList {
                expr: list_expr_ast_box,
                list: list_ast_vec,
                ..
            } => {
                let column_name_opt = get_column_name(list_expr_ast_box.borrow_mut())?;

                for expr in list_ast_vec {
                    if let Some(ast_replacement) = Self::attempt_prepared_value_extraction(
                        table,
                        column_types,
                        prepared_param_pos,
                        &column_name_opt,
                        expr,
                        &mut prepared_statement_values,
                    )? {
                        *expr = ast_replacement;
                    };
                }
            }

            Expr::Between {
                expr: between_expr_ast_box,
                low: between_low_ast_box,
                high: between_high_ast_box,
                ..
            } => {
                let column_name_opt = get_column_name(between_expr_ast_box.borrow_mut())?;

                let between_low_ast = between_low_ast_box.borrow_mut();
                if let Some(ast_replacement) = Self::attempt_prepared_value_extraction(
                    table,
                    column_types,
                    prepared_param_pos,
                    &column_name_opt,
                    between_low_ast,
                    &mut prepared_statement_values,
                )? {
                    *between_low_ast = ast_replacement;
                }

                let between_high_ast = between_high_ast_box.borrow_mut();
                if let Some(ast_replacement) = Self::attempt_prepared_value_extraction(
                    table,
                    column_types,
                    prepared_param_pos,
                    &column_name_opt,
                    between_high_ast,
                    &mut prepared_statement_values,
                )? {
                    *between_high_ast = ast_replacement;
                }
            }
            Expr::Case {
                conditions: case_conditions_ast_vec,
                results: case_results_ast_vec,
                else_result: case_else_results_ast_box_opt,
                ..
            } => {
                for case_condition_ast in case_conditions_ast_vec {
                    prepared_statement_values.extend(Self::generate_prepared_values(
                        case_condition_ast,
                        table,
                        column_types,
                        Some(prepared_param_pos),
                    )?);
                }

                for case_results_ast_vec in case_results_ast_vec {
                    prepared_statement_values.extend(Self::generate_prepared_values(
                        case_results_ast_vec,
                        table,
                        column_types,
                        Some(prepared_param_pos),
                    )?);
                }

                if let Some(case_else_results_ast_box) = case_else_results_ast_box_opt {
                    prepared_statement_values.extend(Self::generate_prepared_values(
                        case_else_results_ast_box.borrow_mut(),
                        table,
                        column_types,
                        Some(prepared_param_pos),
                    )?);
                }
            }
            Expr::Cast {
                expr: cast_expr_box,
                ..
            } => {
                prepared_statement_values.extend(Self::generate_prepared_values(
                    cast_expr_box,
                    table,
                    column_types,
                    Some(prepared_param_pos),
                )?);
            }
            Expr::Collate { expr, .. } => {
                prepared_statement_values.extend(Self::generate_prepared_values(
                    expr,
                    table,
                    column_types,
                    Some(prepared_param_pos),
                )?);
            }
            Expr::Extract { expr, .. } => {
                prepared_statement_values.extend(Self::generate_prepared_values(
                    expr,
                    table,
                    column_types,
                    Some(prepared_param_pos),
                )?);
            }
            Expr::Function(Function {
                args: args_ast_vec, ..
            }) => {
                for expr in args_ast_vec {
                    prepared_statement_values.extend(Self::generate_prepared_values(
                        expr,
                        table,
                        column_types,
                        Some(prepared_param_pos),
                    )?);
                }
            }
            Expr::InSubquery { expr: expr_box, .. } => {
                prepared_statement_values.extend(Self::generate_prepared_values(
                    expr_box.borrow_mut(),
                    table,
                    column_types,
                    Some(prepared_param_pos),
                )?);
            }
            Expr::IsNotNull(null_ast_box) => {
                prepared_statement_values.extend(Self::generate_prepared_values(
                    null_ast_box.borrow_mut(),
                    table,
                    column_types,
                    Some(prepared_param_pos),
                )?);
            }
            Expr::IsNull(null_ast_box) => {
                prepared_statement_values.extend(Self::generate_prepared_values(
                    null_ast_box.borrow_mut(),
                    table,
                    column_types,
                    Some(prepared_param_pos),
                )?);
            }
            Expr::Nested(nested_ast_box) => {
                prepared_statement_values.extend(Self::generate_prepared_values(
                    nested_ast_box.borrow_mut(),
                    table,
                    column_types,
                    Some(prepared_param_pos),
                )?);
            }

            // Not supported
            Expr::Identifier(_non_nested_column_name) => (),
            Expr::CompoundIdentifier(_nested_fk_column_vec) => (),
            Expr::Exists(_query_box) => (),
            Expr::QualifiedWildcard(_wildcard_vec) => (),
            Expr::Subquery(_query_box) => (),
            Expr::UnaryOp {
                expr: _unary_expr_box,
                ..
            } => (),
            Expr::Value(_val) => (),
            Expr::Wildcard => (),
        };

        // move the mutated AST back into the main AST tree
        *ast = ast_temp_replace;

        Ok(prepared_statement_values)
    }

    /// Attempts to swap a Value with a prepared parameter string ($1, $2, etc.) and extract that
    /// value as a TypedColumnValue.
    fn attempt_prepared_value_extraction(
        table: &str,
        column_types: &HashMap<String, &'static str>,
        prepared_param_pos: &mut usize,
        column_name_opt: &Option<String>,
        expr: &mut Expr,
        prepared_statement_values: &mut Vec<TypedColumnValue>,
    ) -> Result<Option<Expr>, Error> {
        let val_opt = ParsedSQLValue::attempt_extract_prepared_value_from_expr(expr);

        if let (Some(column_name), true) = (column_name_opt, val_opt.is_some()) {
            if let Some(column_type) = column_types.get(column_name) {
                prepared_statement_values
                    .push(Self::from_parsed_sql_value(column_type, val_opt.unwrap())?);
                let new_node = Expr::Identifier(format!("${}", prepared_param_pos));
                *prepared_param_pos += 1;

                return Ok(Some(new_node));
            }
        }

        prepared_statement_values.extend(Self::generate_prepared_values(
            expr,
            table,
            column_types,
            Some(prepared_param_pos),
        )?);

        Ok(None)
    }

    fn convert_json_value_to_bigint(value: &JsonValue) -> Result<Self, Error> {
        match value.as_i64() {
            Some(val) => Ok(TypedColumnValue::BigInt(IsNullColumnValue::NotNullable(
                val,
            ))),
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value must be an integer: `{}`.", value),
            )),
        }
    }

    fn convert_json_value_to_bool(value: &JsonValue) -> Result<Self, Error> {
        match value.as_bool() {
            Some(val) => Ok(TypedColumnValue::Bool(IsNullColumnValue::NotNullable(val))),
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value must be boolean: `{}`.", value),
            )),
        }
    }

    fn convert_json_value_to_bytea(value: &JsonValue) -> Result<Self, Error> {
        match value.as_array() {
            Some(raw_bytea_json_vec) => {
                let bytea_conversion: Result<Vec<u8>, Error> = raw_bytea_json_vec
                    .iter()
                    .map(|json_val| match json_val.as_u64() {
                        Some(bytea_val) => Ok(bytea_val as u8),
                        None => Err(Error::generate_error(
                            "INVALID_JSON_TYPE_CONVERSION",
                            format!("Value must be an array of unsigned integers: `{}`.", value),
                        )),
                    })
                    .collect();

                match bytea_conversion {
                    Ok(bytea_vec) => Ok(TypedColumnValue::ByteA(IsNullColumnValue::NotNullable(
                        bytea_vec,
                    ))),
                    Err(e) => Err(e),
                }
            }
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value must be an array of unsigned integers: `{}`.", value),
            )),
        }
    }

    fn convert_json_value_to_char(value: &JsonValue) -> Result<Self, Error> {
        match value.as_str() {
            Some(val) => Ok(TypedColumnValue::Char(IsNullColumnValue::NotNullable(
                val.to_string(),
            ))),
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value must be a string: `{}`.", value),
            )),
        }
    }

    fn convert_json_value_to_citext(value: &JsonValue) -> Result<Self, Error> {
        match value.as_str() {
            Some(val) => Ok(TypedColumnValue::Citext(IsNullColumnValue::NotNullable(
                val.to_string(),
            ))),
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value must be a string: `{}`.", value),
            )),
        }
    }

    fn convert_json_value_to_date(value: &JsonValue) -> Result<Self, Error> {
        match value.as_str() {
            Some(val) => match NaiveDate::from_str(val) {
                Ok(date) => Ok(TypedColumnValue::Date(IsNullColumnValue::NotNullable(date))),
                Err(e) => Err(Error::generate_error(
                    "INVALID_JSON_TYPE_CONVERSION",
                    format!("Value must be a valid date: `{}`. Message: `{}`.", value, e),
                )),
            },
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value must be a string: `{}`.", value),
            )),
        }
    }

    fn convert_json_value_to_decimal(value: &JsonValue) -> Result<Self, Error> {
        match value.as_str() {
            Some(val) => match Decimal::from_str(val) {
                Ok(decimal) => Ok(TypedColumnValue::Decimal(IsNullColumnValue::NotNullable(
                    decimal,
                ))),
                Err(e) => Err(Error::generate_error(
                    "INVALID_JSON_TYPE_CONVERSION",
                    format!(
                        "Value must be a valid decimal: `{}`. Message: `{}`.",
                        value, e
                    ),
                )),
            },
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value must be a string: `{}`.", value),
            )),
        }
    }

    fn convert_json_value_to_float8(value: &JsonValue) -> Result<Self, Error> {
        match value.as_f64() {
            Some(n) => Ok(TypedColumnValue::Float8(IsNullColumnValue::NotNullable(n))),
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value must be a floating number: `{}`.", value),
            )),
        }
    }

    fn convert_json_value_to_int(value: &JsonValue) -> Result<Self, Error> {
        match value.as_i64() {
            Some(n) => Ok(TypedColumnValue::Int(IsNullColumnValue::NotNullable(
                n as i32,
            ))),
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value must be an integer: `{}`.", value),
            )),
        }
    }

    fn convert_json_value_to_json(value: &JsonValue) -> Result<Self, Error> {
        Ok(TypedColumnValue::Json(IsNullColumnValue::NotNullable(
            value.clone(),
        )))
    }

    fn convert_json_value_to_jsonb(value: &JsonValue) -> Result<Self, Error> {
        Ok(TypedColumnValue::JsonB(IsNullColumnValue::NotNullable(
            value.clone(),
        )))
    }

    fn convert_json_value_to_macaddr(value: &JsonValue) -> Result<Self, Error> {
        match value.as_str() {
            Some(val) => match Eui48MacAddress::from_str(val) {
                Ok(mac) => Ok(TypedColumnValue::MacAddr(IsNullColumnValue::NotNullable(
                    MacAddress(mac),
                ))),
                Err(e) => Err(Error::generate_error(
                    "INVALID_JSON_TYPE_CONVERSION",
                    format!(
                        "Value must be a valid mac address: `{}`. Message: `{}`.",
                        value, e
                    ),
                )),
            },
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value must be a string: `{}`.", value),
            )),
        }
    }

    fn convert_json_value_to_name(value: &JsonValue) -> Result<Self, Error> {
        match value.as_str() {
            Some(val) => Ok(TypedColumnValue::Name(IsNullColumnValue::NotNullable(
                val.to_string(),
            ))),
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value must be a string: `{}`.", value),
            )),
        }
    }

    fn convert_json_value_to_oid(value: &JsonValue) -> Result<Self, Error> {
        match value.as_u64() {
            Some(val) => Ok(TypedColumnValue::Oid(IsNullColumnValue::NotNullable(
                val as u32,
            ))),
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value must be an unsigned integer: `{}`.", value),
            )),
        }
    }

    fn convert_json_value_to_real(value: &JsonValue) -> Result<Self, Error> {
        match value.as_f64() {
            Some(n) => Ok(TypedColumnValue::Real(IsNullColumnValue::NotNullable(
                n as f32,
            ))),
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value must be a floating number: `{}`.", value),
            )),
        }
    }

    fn convert_json_value_to_smallint(value: &JsonValue) -> Result<Self, Error> {
        match value.as_i64() {
            Some(n) => Ok(TypedColumnValue::SmallInt(IsNullColumnValue::NotNullable(
                n as i16,
            ))),
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value must be an integer: `{}`.", value),
            )),
        }
    }

    fn convert_json_value_to_text(value: &JsonValue) -> Result<Self, Error> {
        match value.as_str() {
            Some(val) => Ok(TypedColumnValue::Text(IsNullColumnValue::NotNullable(
                val.to_string(),
            ))),
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value must be a string: `{}`.", value),
            )),
        }
    }

    fn convert_json_value_to_time(value: &JsonValue) -> Result<Self, Error> {
        match value.as_str() {
            Some(val) => match NaiveTime::from_str(val) {
                Ok(time) => Ok(TypedColumnValue::Time(IsNullColumnValue::NotNullable(time))),
                Err(e) => Err(Error::generate_error(
                    "INVALID_JSON_TYPE_CONVERSION",
                    format!("Value must be a valid time: `{}`. Message: `{}`.", value, e),
                )),
            },
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value must be a string: `{}`.", value),
            )),
        }
    }

    fn convert_json_value_to_timestamp(value: &JsonValue) -> Result<Self, Error> {
        match value.as_str() {
            Some(val) => match NaiveDateTime::from_str(val) {
                Ok(timestamp) => Ok(TypedColumnValue::Timestamp(IsNullColumnValue::NotNullable(
                    timestamp,
                ))),
                Err(e) => Err(Error::generate_error(
                    "INVALID_JSON_TYPE_CONVERSION",
                    format!(
                        "Value must be a valid timestamp: `{}`. Message: `{}`.",
                        value, e
                    ),
                )),
            },
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value must be a string: `{}`.", value),
            )),
        }
    }

    fn convert_json_value_to_timestamptz(value: &JsonValue) -> Result<Self, Error> {
        match value.as_str() {
            Some(val) => match DateTime::from_str(val) {
                Ok(timestamptz) => Ok(TypedColumnValue::TimestampTz(
                    IsNullColumnValue::NotNullable(timestamptz),
                )),
                Err(e) => Err(Error::generate_error(
                    "INVALID_JSON_TYPE_CONVERSION",
                    format!(
                        "Value must be a valid timestamp with time zone: `{}`. Message: `{}`.",
                        value, e
                    ),
                )),
            },
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value must be a string: `{}`.", value),
            )),
        }
    }

    fn convert_json_value_to_uuid(value: &JsonValue) -> Result<Self, Error> {
        match value.as_str() {
            Some(val) => match Uuid::parse_str(val) {
                Ok(uuid_val) => Ok(TypedColumnValue::Uuid(IsNullColumnValue::NotNullable(
                    uuid_val,
                ))),
                Err(e) => Err(Error::generate_error(
                    "INVALID_JSON_TYPE_CONVERSION",
                    format!("Value must be a valid UUID: `{}`. Message: `{}`.", value, e),
                )),
            },
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value must be a string: `{}`.", value),
            )),
        }
    }

    fn convert_json_value_to_varchar(value: &JsonValue) -> Result<Self, Error> {
        match value.as_str() {
            Some(val) => Ok(TypedColumnValue::VarChar(IsNullColumnValue::NotNullable(
                val.to_string(),
            ))),
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value must be a string: `{}`.", value),
            )),
        }
    }
}

/// A HashMap of column names and their values for a single table row.
pub type RowValues = HashMap<String, TypedColumnValue>;

/// Analyzes a table postgres row and returns the Rust-equivalent value.
pub fn row_to_row_values(row: &Row) -> Result<RowValues, Error> {
    let mut row_values = HashMap::new();
    for (i, column) in row.columns().iter().enumerate() {
        let column_type_name = column.type_().name();

        row_values.insert(
            column.name().to_string(),
            match column_type_name {
                "int8" => TypedColumnValue::BigInt(row.get(i)),
                "bool" => TypedColumnValue::Bool(row.get(i)),
                "bytea" => {
                    // byte array (binary)
                    TypedColumnValue::ByteA(row.get(i))
                }
                "bpchar" => TypedColumnValue::Char(row.get(i)), // char
                "citext" => TypedColumnValue::Citext(row.get(i)),
                "date" => TypedColumnValue::Date(row.get(i)),
                "float4" => TypedColumnValue::Real(row.get(i)),
                "float8" => TypedColumnValue::Float8(row.get(i)),
                "int2" => TypedColumnValue::SmallInt(row.get(i)),
                "int4" => TypedColumnValue::Int(row.get(i)), // int
                "json" => TypedColumnValue::Json(row.get(i)),
                "jsonb" => TypedColumnValue::JsonB(row.get(i)),
                "macaddr" => TypedColumnValue::MacAddr(row.get(i)),
                "name" => TypedColumnValue::Name(row.get(i)),
                // using rust-decimal per discussion at https://www.reddit.com/r/rust/comments/a7frqj/have_anyone_reviewed_any_of_the_decimal_crates/.
                // keep in mind that at the time of this writing, diesel uses bigdecimal
                "numeric" => TypedColumnValue::Decimal(row.get(i)),
                "oid" => TypedColumnValue::Oid(row.get(i)),
                "text" => TypedColumnValue::Text(row.get(i)),
                "time" => TypedColumnValue::Time(row.get(i)),
                "timestamp" => TypedColumnValue::Timestamp(row.get(i)),
                "timestamptz" => TypedColumnValue::TimestampTz(row.get(i)),
                "uuid" => TypedColumnValue::Uuid(row.get(i)),
                // "varbit" => {
                //     TypedColumnValue::VarBit(row.get(i))
                // }
                "varchar" => TypedColumnValue::VarChar(row.get(i)),
                _ => {
                    return Err(Error::generate_error(
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

    Ok(row_values)
}

#[derive(Debug, PartialEq)]
/// Possible values that can be passed into a prepared statement Vec.
pub enum ParsedSQLValue {
    Boolean(bool),
    Float(f64),
    Int8(i64),
    Null,
    String(String),
}

impl fmt::Display for ParsedSQLValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Boolean(v) => write!(f, "{}", v,),
            Self::Float(v) => write!(f, "{}", v,),
            Self::Int8(v) => write!(f, "{}", v,),
            Self::Null => write!(f, "null",),
            Self::String(v) => write!(f, "{}", v,),
        }
    }
}

impl From<SqlValue> for ParsedSQLValue {
    fn from(v: SqlValue) -> Self {
        match v {
            SqlValue::Boolean(v) => Self::Boolean(v),
            SqlValue::Date(v) => Self::String(v),
            SqlValue::Double(v) => Self::Float(v.into_inner()),
            SqlValue::HexStringLiteral(v) => Self::String(v),
            SqlValue::Interval { .. } => unimplemented!("Interval type not supported"),
            SqlValue::Long(v) => Self::Int8(v as i64),
            SqlValue::NationalStringLiteral(v) => Self::String(v),
            SqlValue::Null => Self::Null,
            SqlValue::SingleQuotedString(v) => Self::String(v),
            SqlValue::Time(v) => Self::String(v),
            SqlValue::Timestamp(v) => Self::String(v),
        }
    }
}

impl ParsedSQLValue {
    /// Tries to extract a prepared statement value from an Expr.
    pub fn attempt_extract_prepared_value_from_expr(expr: &Expr) -> Option<Self> {
        match expr {
            Expr::Value(val) => Some(ParsedSQLValue::from(val.clone())),
            Expr::UnaryOp {
                expr: unary_expr_box,
                op,
            } => {
                let borrowed_expr = unary_expr_box.borrow();

                if let Expr::Value(val) = borrowed_expr {
                    let mut prepared_val = ParsedSQLValue::from(val.clone());
                    match op {
                        UnaryOperator::Minus => prepared_val.invert(),
                        UnaryOperator::Not => prepared_val.invert(),
                        _ => (),
                    };

                    Some(prepared_val)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Converts a negative number to positive and vice versa. If the value is a boolean, inverts
    /// the boolean.
    pub fn invert(&mut self) {
        match self {
            Self::Boolean(v) => {
                *self = Self::Boolean(!*v);
            }
            Self::Float(v) => {
                *self = Self::Float(0.0 - *v);
            }
            Self::Int8(v) => {
                *self = Self::Int8(0 - *v);
            }
            Self::Null => (),
            Self::String(_v) => (),
        };
    }
}

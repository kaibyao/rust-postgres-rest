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
pub enum ColumnValue<T> {
    Nullable(Option<T>),
    NotNullable(T),
}

impl<'a, T> FromSql<'a> for ColumnValue<T>
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
        <T as FromSql>::from_sql(ty, raw).map(ColumnValue::NotNullable)
    }

    fn from_sql_null(_: &Type) -> Result<Self, Box<dyn StdError + Sync + Send>> {
        Ok(ColumnValue::Nullable(None))
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

impl<T> ToSql for ColumnValue<T>
where
    T: ToSql,
{
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

#[derive(Debug, PartialEq, Serialize)]
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
    Int(ColumnValue<i32>),
    Json(ColumnValue<JsonValue>),
    JsonB(ColumnValue<JsonValue>),
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

impl<'a> FromSql<'a> for ColumnTypeValue {
    fn accepts(ty: &Type) -> bool {
        match ty.name() {
            "int8" => <ColumnValue<i64> as FromSql>::accepts(ty),
            "bool" => <ColumnValue<bool> as FromSql>::accepts(ty),
            "bytea" => <ColumnValue<Vec<u8>> as FromSql>::accepts(ty),
            "bpchar" => <ColumnValue<String> as FromSql>::accepts(ty),
            "citext" => <ColumnValue<String> as FromSql>::accepts(ty),
            "date" => <ColumnValue<NaiveDate> as FromSql>::accepts(ty),
            "float4" => <ColumnValue<f32> as FromSql>::accepts(ty),
            "float8" => <ColumnValue<f64> as FromSql>::accepts(ty),
            "int2" => <ColumnValue<i16> as FromSql>::accepts(ty),
            "int4" => <ColumnValue<i32> as FromSql>::accepts(ty),
            "json" => <ColumnValue<JsonValue> as FromSql>::accepts(ty),
            "jsonb" => <ColumnValue<JsonValue> as FromSql>::accepts(ty),
            "macaddr" => <ColumnValue<MacAddress> as FromSql>::accepts(ty),
            "name" => <ColumnValue<String> as FromSql>::accepts(ty),
            "numeric" => <ColumnValue<Decimal> as FromSql>::accepts(ty),
            "oid" => <ColumnValue<u32> as FromSql>::accepts(ty),
            "text" => <ColumnValue<String> as FromSql>::accepts(ty),
            "time" => <ColumnValue<NaiveTime> as FromSql>::accepts(ty),
            "timestamp" => <ColumnValue<NaiveDateTime> as FromSql>::accepts(ty),
            "timestamptz" => <ColumnValue<DateTime<Utc>> as FromSql>::accepts(ty),
            "uuid" => <ColumnValue<Uuid> as FromSql>::accepts(ty),
            "varchar" => <ColumnValue<String> as FromSql>::accepts(ty),
            &_ => false,
        }
    }

    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn StdError + 'static + Send + Sync>> {
        match ty.name() {
            "int8" => Ok(Self::BigInt(<ColumnValue<i64> as FromSql>::from_sql(
                ty, raw,
            )?)),
            "bool" => Ok(Self::Bool(<ColumnValue<bool> as FromSql>::from_sql(
                ty, raw,
            )?)),
            "bytea" => Ok(Self::ByteA(<ColumnValue<Vec<u8>> as FromSql>::from_sql(
                ty, raw,
            )?)),
            "bpchar" => Ok(Self::Char(<ColumnValue<String> as FromSql>::from_sql(
                ty, raw,
            )?)),
            "citext" => Ok(Self::Citext(<ColumnValue<String> as FromSql>::from_sql(
                ty, raw,
            )?)),
            "date" => Ok(Self::Date(<ColumnValue<NaiveDate> as FromSql>::from_sql(
                ty, raw,
            )?)),
            "float4" => Ok(Self::Real(<ColumnValue<f32> as FromSql>::from_sql(
                ty, raw,
            )?)),
            "float8" => Ok(Self::Float8(<ColumnValue<f64> as FromSql>::from_sql(
                ty, raw,
            )?)),
            "int2" => Ok(Self::SmallInt(<ColumnValue<i16> as FromSql>::from_sql(
                ty, raw,
            )?)),
            "int4" => Ok(Self::Int(<ColumnValue<i32> as FromSql>::from_sql(ty, raw)?)),
            "json" => Ok(Self::Json(<ColumnValue<JsonValue> as FromSql>::from_sql(
                ty, raw,
            )?)),
            "jsonb" => Ok(Self::JsonB(<ColumnValue<JsonValue> as FromSql>::from_sql(
                ty, raw,
            )?)),
            "macaddr" => Ok(Self::MacAddr(
                <ColumnValue<MacAddress> as FromSql>::from_sql(ty, raw)?,
            )),
            "name" => Ok(Self::Name(<ColumnValue<String> as FromSql>::from_sql(
                ty, raw,
            )?)),
            "numeric" => Ok(Self::Decimal(<ColumnValue<Decimal> as FromSql>::from_sql(
                ty, raw,
            )?)),
            "oid" => Ok(Self::Oid(<ColumnValue<u32> as FromSql>::from_sql(ty, raw)?)),
            "text" => Ok(Self::Text(<ColumnValue<String> as FromSql>::from_sql(
                ty, raw,
            )?)),
            "time" => Ok(Self::Time(<ColumnValue<NaiveTime> as FromSql>::from_sql(
                ty, raw,
            )?)),
            "timestamp" => Ok(Self::Timestamp(
                <ColumnValue<NaiveDateTime> as FromSql>::from_sql(ty, raw)?,
            )),
            "timestamptz" => Ok(Self::TimestampTz(
                <ColumnValue<DateTime<Utc>> as FromSql>::from_sql(ty, raw)?,
            )),
            "uuid" => Ok(Self::Uuid(<ColumnValue<Uuid> as FromSql>::from_sql(
                ty, raw,
            )?)),
            "varchar" => Ok(Self::VarChar(<ColumnValue<String> as FromSql>::from_sql(
                ty, raw,
            )?)),
            &_ => Err(Box::new(
                Error::generate_error("TABLE_COLUMN_TYPE_NOT_FOUND", ty.name().to_string())
                    .compat(),
            )),
        }
    }

    fn from_sql_null(_: &Type) -> Result<Self, Box<dyn StdError + Sync + Send>> {
        Ok(Self::BigInt(ColumnValue::Nullable(None)))
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

impl ToSql for ColumnTypeValue {
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
            "int8" => <ColumnValue<i64> as ToSql>::accepts(ty),
            "bool" => <ColumnValue<bool> as ToSql>::accepts(ty),
            "bytea" => <ColumnValue<Vec<u8>> as ToSql>::accepts(ty),
            "bpchar" => <ColumnValue<String> as ToSql>::accepts(ty),
            "citext" => <ColumnValue<String> as ToSql>::accepts(ty),
            "date" => <ColumnValue<NaiveDate> as ToSql>::accepts(ty),
            "float4" => <ColumnValue<f32> as ToSql>::accepts(ty),
            "float8" => <ColumnValue<f64> as ToSql>::accepts(ty),
            "int2" => <ColumnValue<i16> as ToSql>::accepts(ty),
            "int4" => <ColumnValue<i32> as ToSql>::accepts(ty),
            "json" => <ColumnValue<JsonValue> as ToSql>::accepts(ty),
            "jsonb" => <ColumnValue<JsonValue> as ToSql>::accepts(ty),
            "macaddr" => <ColumnValue<MacAddress> as ToSql>::accepts(ty),
            "name" => <ColumnValue<String> as ToSql>::accepts(ty),
            "numeric" => <ColumnValue<Decimal> as ToSql>::accepts(ty),
            "oid" => <ColumnValue<u32> as ToSql>::accepts(ty),
            "text" => <ColumnValue<String> as ToSql>::accepts(ty),
            "time" => <ColumnValue<NaiveTime> as ToSql>::accepts(ty),
            "timestamp" => <ColumnValue<NaiveDateTime> as ToSql>::accepts(ty),
            "timestamptz" => <ColumnValue<DateTime<Utc>> as ToSql>::accepts(ty),
            "uuid" => <ColumnValue<Uuid> as ToSql>::accepts(ty),
            "varchar" => <ColumnValue<String> as ToSql>::accepts(ty),
            &_ => false,
        }
    }

    to_sql_checked!();
}

impl ColumnTypeValue {
    /// Parses a Value and returns the Rust-Typed version.
    pub fn from_json(column_type: &str, value: &JsonValue) -> Result<Self, Error> {
        match column_type {
            "int8" => Self::convert_json_value_to_bigint(column_type, value),
            "bool" => Self::convert_json_value_to_bool(column_type, value),
            "bytea" => Self::convert_json_value_to_bytea(column_type, value),
            "bpchar" => Self::convert_json_value_to_char(column_type, value),
            "citext" => Self::convert_json_value_to_citext(column_type, value),
            "date" => Self::convert_json_value_to_date(column_type, value),
            "float4" => Self::convert_json_value_to_real(column_type, value),
            "float8" => Self::convert_json_value_to_float8(column_type, value),
            "int2" => Self::convert_json_value_to_smallint(column_type, value),
            "int4" => Self::convert_json_value_to_int(column_type, value),
            "json" => Self::convert_json_value_to_json(value),
            "jsonb" => Self::convert_json_value_to_jsonb(value),
            "macaddr" => Self::convert_json_value_to_macaddr(column_type, value),
            "name" => Self::convert_json_value_to_name(column_type, value),
            "numeric" => Self::convert_json_value_to_decimal(column_type, value),
            "oid" => Self::convert_json_value_to_oid(column_type, value),
            "text" => Self::convert_json_value_to_text(column_type, value),
            "time" => Self::convert_json_value_to_time(column_type, value),
            "timestamp" => Self::convert_json_value_to_timestamp(column_type, value),
            "timestamptz" => Self::convert_json_value_to_timestamptz(column_type, value),
            "uuid" => Self::convert_json_value_to_uuid(column_type, value),
            "varchar" => Self::convert_json_value_to_varchar(column_type, value),
            _ => Err(Error::generate_error(
                "UNSUPPORTED_DATA_TYPE",
                format!("Value {} has unsupported type: {}", value, column_type),
            )),
        }
    }

    pub fn from_prepared_statement_value(
        column_type: &str,
        value: PreparedStatementValue,
    ) -> Result<Self, Error> {
        match column_type {
            "int8" => Ok(ColumnTypeValue::BigInt(match value {
                PreparedStatementValue::Int8(val) => ColumnValue::NotNullable(val),
                PreparedStatementValue::Null => ColumnValue::Nullable(None),
                _ => unimplemented!(
                    "Cannot convert from PreparedStatementValue: `{}` to int8.",
                    value
                ),
            })),
            "bool" => Ok(ColumnTypeValue::Bool(match value {
                PreparedStatementValue::Boolean(val) => ColumnValue::NotNullable(val),
                PreparedStatementValue::Null => ColumnValue::Nullable(None),
                _ => unimplemented!(
                    "Cannot convert from PreparedStatementValue: `{}` to bool.",
                    value
                ),
            })),
            "bytea" => Ok(ColumnTypeValue::ByteA(match value {
                PreparedStatementValue::Null => ColumnValue::Nullable(None),
                PreparedStatementValue::String(val) => ColumnValue::NotNullable(val.into_bytes()),
                _ => unimplemented!(
                    "Cannot convert from PreparedStatementValue: `{}` to bytea.",
                    value
                ),
            })),
            "bpchar" => Ok(ColumnTypeValue::Char(match value {
                PreparedStatementValue::Null => ColumnValue::Nullable(None),
                PreparedStatementValue::String(val) => ColumnValue::NotNullable(val),
                _ => unimplemented!(
                    "Cannot convert from PreparedStatementValue: `{}` to bpchar.",
                    value
                ),
            })),
            "citext" => Ok(ColumnTypeValue::Citext(match value {
                PreparedStatementValue::Null => ColumnValue::Nullable(None),
                PreparedStatementValue::String(val) => ColumnValue::NotNullable(val),
                _ => unimplemented!(
                    "Cannot convert from PreparedStatementValue: `{}` to citext.",
                    value
                ),
            })),
            "date" => Ok(ColumnTypeValue::Date(match value {
                PreparedStatementValue::Null => ColumnValue::Nullable(None),
                PreparedStatementValue::String(val) => {
                    ColumnValue::NotNullable(NaiveDate::from_str(&val)?)
                }
                _ => unimplemented!(
                    "Cannot convert from PreparedStatementValue: `{}` to date.",
                    value
                ),
            })),
            "float4" => Ok(ColumnTypeValue::Real(match value {
                PreparedStatementValue::Float(val) => ColumnValue::NotNullable(val as f32),
                PreparedStatementValue::Null => ColumnValue::Nullable(None),
                _ => unimplemented!(
                    "Cannot convert from PreparedStatementValue: `{}` to float4.",
                    value
                ),
            })),
            "float8" => Ok(ColumnTypeValue::Float8(match value {
                PreparedStatementValue::Float(val) => ColumnValue::NotNullable(val),
                PreparedStatementValue::Null => ColumnValue::Nullable(None),
                _ => unimplemented!(
                    "Cannot convert from PreparedStatementValue: `{}` to float8.",
                    value
                ),
            })),
            "int2" => Ok(ColumnTypeValue::SmallInt(match value {
                PreparedStatementValue::Int8(val) => ColumnValue::NotNullable(val as i16),
                PreparedStatementValue::Null => ColumnValue::Nullable(None),
                _ => unimplemented!(
                    "Cannot convert from PreparedStatementValue: `{}` to int2.",
                    value
                ),
            })),
            "int4" => Ok(ColumnTypeValue::Int(match value {
                PreparedStatementValue::Int8(val) => ColumnValue::NotNullable(val as i32),
                PreparedStatementValue::Null => ColumnValue::Nullable(None),
                _ => unimplemented!(
                    "Cannot convert from PreparedStatementValue: `{}` to int4.",
                    value
                ),
            })),
            "json" => Ok(ColumnTypeValue::Json(match value {
                PreparedStatementValue::Null => ColumnValue::Nullable(None),
                PreparedStatementValue::String(val) => {
                    ColumnValue::NotNullable(serde_json::from_str(&val)?)
                }
                _ => unimplemented!(
                    "Cannot convert from PreparedStatementValue: `{}` to json.",
                    value
                ),
            })),
            "jsonb" => Ok(ColumnTypeValue::JsonB(match value {
                PreparedStatementValue::Null => ColumnValue::Nullable(None),
                PreparedStatementValue::String(val) => {
                    ColumnValue::NotNullable(serde_json::from_str(&val)?)
                }
                _ => unimplemented!(
                    "Cannot convert from PreparedStatementValue: `{}` to json.",
                    value
                ),
            })),
            "macaddr" => Ok(ColumnTypeValue::MacAddr(match value {
                PreparedStatementValue::Null => ColumnValue::Nullable(None),
                PreparedStatementValue::String(val) => {
                    ColumnValue::NotNullable(MacAddress(Eui48MacAddress::from_str(&val)?))
                }
                _ => unimplemented!(
                    "Cannot convert from PreparedStatementValue: `{}` to macaddr.",
                    value
                ),
            })),
            "name" => Ok(ColumnTypeValue::Name(match value {
                PreparedStatementValue::Null => ColumnValue::Nullable(None),
                PreparedStatementValue::String(val) => ColumnValue::NotNullable(val),
                _ => unimplemented!(
                    "Cannot convert from PreparedStatementValue: `{}` to name.",
                    value
                ),
            })),
            "numeric" => Ok(ColumnTypeValue::Decimal(match value {
                PreparedStatementValue::Null => ColumnValue::Nullable(None),
                PreparedStatementValue::String(val) => {
                    ColumnValue::NotNullable(Decimal::from_str(&val)?)
                }
                _ => unimplemented!(
                    "Cannot convert from PreparedStatementValue: `{}` to numeric.",
                    value
                ),
            })),
            "oid" => Ok(ColumnTypeValue::Oid(match value {
                PreparedStatementValue::Int8(val) => ColumnValue::NotNullable(val as u32),
                PreparedStatementValue::Null => ColumnValue::Nullable(None),
                _ => unimplemented!(
                    "Cannot convert from PreparedStatementValue: `{}` to oid.",
                    value
                ),
            })),
            "text" => Ok(ColumnTypeValue::Text(match value {
                PreparedStatementValue::Null => ColumnValue::Nullable(None),
                PreparedStatementValue::String(val) => ColumnValue::NotNullable(val),
                _ => unimplemented!(
                    "Cannot convert from PreparedStatementValue: `{}` to text.",
                    value
                ),
            })),
            "time" => Ok(ColumnTypeValue::Time(match value {
                PreparedStatementValue::Null => ColumnValue::Nullable(None),
                PreparedStatementValue::String(val) => {
                    ColumnValue::NotNullable(NaiveTime::from_str(&val)?)
                }
                _ => unimplemented!(
                    "Cannot convert from PreparedStatementValue: `{}` to time.",
                    value
                ),
            })),
            "timestamp" => Ok(ColumnTypeValue::Timestamp(match value {
                PreparedStatementValue::Null => ColumnValue::Nullable(None),
                PreparedStatementValue::String(val) => {
                    ColumnValue::NotNullable(NaiveDateTime::from_str(&val)?)
                }
                _ => unimplemented!(
                    "Cannot convert from PreparedStatementValue: `{}` to timestamp.",
                    value
                ),
            })),
            "timestamptz" => Ok(ColumnTypeValue::TimestampTz(match value {
                PreparedStatementValue::Null => ColumnValue::Nullable(None),
                PreparedStatementValue::String(val) => {
                    let timestamp = DateTime::from_str(&val)?;
                    ColumnValue::NotNullable(timestamp)
                }
                _ => unimplemented!(
                    "Cannot convert from PreparedStatementValue: `{}` to timestamptz.",
                    value
                ),
            })),
            "uuid" => Ok(ColumnTypeValue::Uuid(match value {
                PreparedStatementValue::Null => ColumnValue::Nullable(None),
                PreparedStatementValue::String(val) => {
                    ColumnValue::NotNullable(Uuid::from_str(&val)?)
                }
                _ => unimplemented!(
                    "Cannot convert from PreparedStatementValue: `{}` to uuid.",
                    value
                ),
            })),
            "varchar" => Ok(ColumnTypeValue::VarChar(match value {
                PreparedStatementValue::Null => ColumnValue::Nullable(None),
                PreparedStatementValue::String(val) => ColumnValue::NotNullable(val),
                _ => unimplemented!(
                    "Cannot convert from PreparedStatementValue: `{}` to varchar.",
                    value
                ),
            })),
            _ => Err(Error::generate_error(
                "UNSUPPORTED_DATA_TYPE",
                format!("Value {} has unsupported type: {}", value, column_type),
            )),
        }
    }

    // Parses a given AST and returns a tuple: (String [the converted expression that uses PREPARE
    // parameters], Vec<ColumnTypeValue>).
    pub fn generate_prepared_statement_from_ast_expr(
        ast: &Expr,
        column_types: &HashMap<String, String>,
        starting_pos: Option<&mut usize>,
    ) -> Result<(String, Vec<ColumnTypeValue>), Error> {
        let mut ast = ast.clone();
        // mutates `ast`
        let prepared_values = Self::generate_prepared_values(&mut ast, column_types, starting_pos)?;

        Ok((ast.to_string(), prepared_values))
    }

    /// Extracts the values being assigned and replaces them with prepared statement position
    /// parameters (like `$1`, `$2`, etc.). Returns a Vec of prepared values.
    fn generate_prepared_values(
        ast: &mut Expr,
        column_types: &HashMap<String, String>,
        prepared_param_pos_opt: Option<&mut usize>,
    ) -> Result<Vec<ColumnTypeValue>, Error> {
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
                        Ok(Some(non_nested_column_name.clone()))
                    }
                    Expr::CompoundIdentifier(nested_fk_column_vec) => {
                        Ok(Some(nested_fk_column_vec.join(".")))
                    }
                    _ => {
                        prepared_statement_values.extend(Self::generate_prepared_values(
                            possible_column_name_expr,
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
                        column_types,
                        Some(prepared_param_pos),
                    )?);
                }

                for case_results_ast_vec in case_results_ast_vec {
                    prepared_statement_values.extend(Self::generate_prepared_values(
                        case_results_ast_vec,
                        column_types,
                        Some(prepared_param_pos),
                    )?);
                }

                if let Some(case_else_results_ast_box) = case_else_results_ast_box_opt {
                    prepared_statement_values.extend(Self::generate_prepared_values(
                        case_else_results_ast_box.borrow_mut(),
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
                    column_types,
                    Some(prepared_param_pos),
                )?);
            }
            Expr::Collate { expr, .. } => {
                prepared_statement_values.extend(Self::generate_prepared_values(
                    expr,
                    column_types,
                    Some(prepared_param_pos),
                )?);
            }
            Expr::Extract { expr, .. } => {
                prepared_statement_values.extend(Self::generate_prepared_values(
                    expr,
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
                        column_types,
                        Some(prepared_param_pos),
                    )?);
                }
            }
            Expr::InSubquery { expr: expr_box, .. } => {
                prepared_statement_values.extend(Self::generate_prepared_values(
                    expr_box.borrow_mut(),
                    column_types,
                    Some(prepared_param_pos),
                )?);
            }
            Expr::IsNotNull(null_ast_box) => {
                prepared_statement_values.extend(Self::generate_prepared_values(
                    null_ast_box.borrow_mut(),
                    column_types,
                    Some(prepared_param_pos),
                )?);
            }
            Expr::IsNull(null_ast_box) => {
                prepared_statement_values.extend(Self::generate_prepared_values(
                    null_ast_box.borrow_mut(),
                    column_types,
                    Some(prepared_param_pos),
                )?);
            }
            Expr::Nested(nested_ast_box) => {
                prepared_statement_values.extend(Self::generate_prepared_values(
                    nested_ast_box.borrow_mut(),
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
    /// value as a ColumnTypeValue.
    fn attempt_prepared_value_extraction(
        column_types: &HashMap<String, String>,
        prepared_param_pos: &mut usize,
        column_name_opt: &Option<String>,
        expr: &mut Expr,
        prepared_statement_values: &mut Vec<ColumnTypeValue>,
    ) -> Result<Option<Expr>, Error> {
        let val_opt = PreparedStatementValue::attempt_extract_prepared_value_from_expr(expr);

        if let (Some(column_name), true) = (column_name_opt, val_opt.is_some()) {
            if let Some(column_type) = column_types.get(column_name) {
                prepared_statement_values.push(Self::from_prepared_statement_value(
                    column_type,
                    val_opt.unwrap(),
                )?);
                let new_node = Expr::Identifier(format!("${}", prepared_param_pos));
                *prepared_param_pos += 1;

                return Ok(Some(new_node));
            }
        }

        prepared_statement_values.extend(Self::generate_prepared_values(
            expr,
            column_types,
            Some(prepared_param_pos),
        )?);

        Ok(None)
    }

    fn convert_json_value_to_bigint(column_type: &str, value: &JsonValue) -> Result<Self, Error> {
        match value.as_i64() {
            Some(val) => Ok(ColumnTypeValue::BigInt(ColumnValue::NotNullable(val))),
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        }
    }

    fn convert_json_value_to_bool(column_type: &str, value: &JsonValue) -> Result<Self, Error> {
        match value.as_bool() {
            Some(val) => Ok(ColumnTypeValue::Bool(ColumnValue::NotNullable(val))),
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        }
    }

    fn convert_json_value_to_bytea(column_type: &str, value: &JsonValue) -> Result<Self, Error> {
        match value.as_array() {
            Some(raw_bytea_json_vec) => {
                let bytea_conversion: Result<Vec<u8>, Error> = raw_bytea_json_vec
                    .iter()
                    .map(|json_val| match json_val.as_u64() {
                        Some(bytea_val) => Ok(bytea_val as u8),
                        None => Err(Error::generate_error(
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
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        }
    }

    fn convert_json_value_to_char(column_type: &str, value: &JsonValue) -> Result<Self, Error> {
        match value.as_str() {
            Some(val) => Ok(ColumnTypeValue::Char(ColumnValue::NotNullable(
                val.to_string(),
            ))),
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        }
    }

    fn convert_json_value_to_citext(column_type: &str, value: &JsonValue) -> Result<Self, Error> {
        match value.as_str() {
            Some(val) => Ok(ColumnTypeValue::Citext(ColumnValue::NotNullable(
                val.to_string(),
            ))),
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        }
    }

    fn convert_json_value_to_date(column_type: &str, value: &JsonValue) -> Result<Self, Error> {
        match value.as_str() {
            Some(val) => match NaiveDate::from_str(val) {
                Ok(date) => Ok(ColumnTypeValue::Date(ColumnValue::NotNullable(date))),
                Err(e) => Err(Error::generate_error(
                    "INVALID_JSON_TYPE_CONVERSION",
                    format!(
                        "Value: `{}`. Column type: `{}`. Message: `{}`.",
                        value, column_type, e
                    ),
                )),
            },
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        }
    }

    fn convert_json_value_to_decimal(column_type: &str, value: &JsonValue) -> Result<Self, Error> {
        match value.as_str() {
            Some(val) => match Decimal::from_str(val) {
                Ok(decimal) => Ok(ColumnTypeValue::Decimal(ColumnValue::NotNullable(decimal))),
                Err(e) => Err(Error::generate_error(
                    "INVALID_JSON_TYPE_CONVERSION",
                    format!(
                        "Value: `{}`. Column type: `{}`. Message: `{}`.",
                        value, column_type, e
                    ),
                )),
            },
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        }
    }

    fn convert_json_value_to_float8(column_type: &str, value: &JsonValue) -> Result<Self, Error> {
        match value.as_f64() {
            Some(n) => Ok(ColumnTypeValue::Float8(ColumnValue::NotNullable(n))),
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        }
    }

    fn convert_json_value_to_int(column_type: &str, value: &JsonValue) -> Result<Self, Error> {
        match value.as_i64() {
            Some(n) => Ok(ColumnTypeValue::Int(ColumnValue::NotNullable(n as i32))),
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        }
    }

    fn convert_json_value_to_json(value: &JsonValue) -> Result<Self, Error> {
        Ok(ColumnTypeValue::Json(ColumnValue::NotNullable(
            value.clone(),
        )))
    }

    fn convert_json_value_to_jsonb(value: &JsonValue) -> Result<Self, Error> {
        Ok(ColumnTypeValue::JsonB(ColumnValue::NotNullable(
            value.clone(),
        )))
    }

    fn convert_json_value_to_macaddr(column_type: &str, value: &JsonValue) -> Result<Self, Error> {
        match value.as_str() {
            Some(val) => match Eui48MacAddress::from_str(val) {
                Ok(mac) => Ok(ColumnTypeValue::MacAddr(ColumnValue::NotNullable(
                    MacAddress(mac),
                ))),
                Err(e) => Err(Error::generate_error(
                    "INVALID_JSON_TYPE_CONVERSION",
                    format!(
                        "Value: `{}`. Column type: `{}`. Message: `{}`.",
                        value, column_type, e
                    ),
                )),
            },
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        }
    }

    fn convert_json_value_to_name(column_type: &str, value: &JsonValue) -> Result<Self, Error> {
        match value.as_str() {
            Some(val) => Ok(ColumnTypeValue::Name(ColumnValue::NotNullable(
                val.to_string(),
            ))),
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        }
    }

    fn convert_json_value_to_oid(column_type: &str, value: &JsonValue) -> Result<Self, Error> {
        match value.as_u64() {
            Some(val) => Ok(ColumnTypeValue::Oid(ColumnValue::NotNullable(val as u32))),
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        }
    }

    fn convert_json_value_to_real(column_type: &str, value: &JsonValue) -> Result<Self, Error> {
        match value.as_f64() {
            Some(n) => Ok(ColumnTypeValue::Real(ColumnValue::NotNullable(n as f32))),
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        }
    }

    fn convert_json_value_to_smallint(column_type: &str, value: &JsonValue) -> Result<Self, Error> {
        match value.as_i64() {
            Some(n) => Ok(ColumnTypeValue::SmallInt(ColumnValue::NotNullable(
                n as i16,
            ))),
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        }
    }

    fn convert_json_value_to_text(column_type: &str, value: &JsonValue) -> Result<Self, Error> {
        match value.as_str() {
            Some(val) => Ok(ColumnTypeValue::Text(ColumnValue::NotNullable(
                val.to_string(),
            ))),
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        }
    }

    fn convert_json_value_to_time(column_type: &str, value: &JsonValue) -> Result<Self, Error> {
        match value.as_str() {
            Some(val) => match NaiveTime::from_str(val) {
                Ok(time) => Ok(ColumnTypeValue::Time(ColumnValue::NotNullable(time))),
                Err(e) => Err(Error::generate_error(
                    "INVALID_JSON_TYPE_CONVERSION",
                    format!(
                        "Value: `{}`. Column type: `{}`. Message: `{}`.",
                        value, column_type, e
                    ),
                )),
            },
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        }
    }

    fn convert_json_value_to_timestamp(
        column_type: &str,
        value: &JsonValue,
    ) -> Result<Self, Error> {
        match value.as_str() {
            Some(val) => match NaiveDateTime::from_str(val) {
                Ok(timestamp) => Ok(ColumnTypeValue::Timestamp(ColumnValue::NotNullable(
                    timestamp,
                ))),
                Err(e) => Err(Error::generate_error(
                    "INVALID_JSON_TYPE_CONVERSION",
                    format!(
                        "Value: `{}`. Column type: `{}`. Message: `{}`.",
                        value, column_type, e
                    ),
                )),
            },
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        }
    }

    fn convert_json_value_to_timestamptz(
        column_type: &str,
        value: &JsonValue,
    ) -> Result<Self, Error> {
        match value.as_str() {
            Some(val) => match DateTime::from_str(val) {
                Ok(timestamptz) => Ok(ColumnTypeValue::TimestampTz(ColumnValue::NotNullable(
                    timestamptz,
                ))),
                Err(e) => Err(Error::generate_error(
                    "INVALID_JSON_TYPE_CONVERSION",
                    format!(
                        "Value: `{}`. Column type: `{}`. Message: `{}`.",
                        value, column_type, e
                    ),
                )),
            },
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        }
    }

    fn convert_json_value_to_uuid(column_type: &str, value: &JsonValue) -> Result<Self, Error> {
        match value.as_str() {
            Some(val) => match Uuid::parse_str(val) {
                Ok(uuid_val) => Ok(ColumnTypeValue::Uuid(ColumnValue::NotNullable(uuid_val))),
                Err(e) => Err(Error::generate_error(
                    "INVALID_JSON_TYPE_CONVERSION",
                    format!(
                        "Value: `{}`. Column type: `{}`. Message: `{}`.",
                        value, column_type, e
                    ),
                )),
            },
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        }
    }

    fn convert_json_value_to_varchar(column_type: &str, value: &JsonValue) -> Result<Self, Error> {
        match value.as_str() {
            Some(val) => Ok(ColumnTypeValue::VarChar(ColumnValue::NotNullable(
                val.to_string(),
            ))),
            None => Err(Error::generate_error(
                "INVALID_JSON_TYPE_CONVERSION",
                format!("Value: `{}`. Column type: `{}`.", value, column_type),
            )),
        }
    }
}

/// The field names and their values for a single table row.
pub type RowFields = HashMap<String, ColumnTypeValue>;

/// Analyzes a table postgres row and returns the Rust-equivalent value.
pub fn convert_row_fields(row: &Row) -> Result<RowFields, Error> {
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

    Ok(row_fields)
}

#[derive(Debug, PartialEq)]
/// Possible values that can be passed into a prepared statement Vec.
pub enum PreparedStatementValue {
    Boolean(bool),
    Float(f64),
    Int8(i64),
    Null,
    String(String),
}

impl fmt::Display for PreparedStatementValue {
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

impl From<SqlValue> for PreparedStatementValue {
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

impl PreparedStatementValue {
    /// Tries to extract a prepared statement value from an Expr.
    pub fn attempt_extract_prepared_value_from_expr(expr: &Expr) -> Option<Self> {
        match expr {
            Expr::Value(val) => Some(PreparedStatementValue::from(val.clone())),
            Expr::UnaryOp {
                expr: unary_expr_box,
                op,
            } => {
                let borrowed_expr = unary_expr_box.borrow();

                if let Expr::Value(val) = borrowed_expr {
                    let mut prepared_val = PreparedStatementValue::from(val.clone());
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

/// Used for returning either number of rows or actual row values in INSERT/UPDATE statements.
pub enum UpsertResult {
    Rows(Vec<RowFields>),
    NumRowsAffected(u64),
}

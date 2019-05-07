use eui48::Eui48;
use postgres::{
    accepts,
    rows::Row,
    types::{FromSql, ToSql, Type, MACADDR},
};
use postgres_protocol::types::macaddr_from_sql;
use rust_decimal::Decimal;
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error as StdError;

/// we have to define our own MacAddress type in order for Serde to serialize it properly. Really it's a copy of eui48's MacAddress
#[derive(Debug, Serialize)]
pub struct MacAddress(Eui48);

// mostly copied from the postgres-protocol and postgres-shared libraries
impl FromSql for MacAddress {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<MacAddress, Box<StdError + Sync + Send>> {
        let bytes = macaddr_from_sql(raw)?;
        Ok(MacAddress(bytes))
    }

    accepts!(MACADDR);
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
/// Represents a single column value for a returned row. We have to have an Enum describing column data that is non-nullable vs nullable
pub enum ColumnValue<T: FromSql> {
    Nullable(Option<T>),
    NotNullable(T),
}

impl<T: FromSql> FromSql for ColumnValue<T> {
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
    Date(ColumnValue<chrono::NaiveDate>),
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
    Time(ColumnValue<chrono::NaiveTime>),
    Timestamp(ColumnValue<chrono::NaiveDateTime>),
    TimestampTz(ColumnValue<chrono::DateTime<chrono::Utc>>),
    // Unknown(ColumnValue<String>),
    Uuid(ColumnValue<uuid::Uuid>),
    // VarBit(ColumnValue<bit_vec::BitVec>),
    VarChar(ColumnValue<String>),
}

/// The field names and their values for a single table row.
pub type RowFields = HashMap<String, ColumnTypeValue>;

/// Analyzes a table postgres row and returns the Rust-equivalent value.
pub fn convert_row_fields(row: &Row) -> RowFields {
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
                _ => ColumnTypeValue::Text(ColumnValue::NotNullable(format!(
                    "Column {} has unsupported type: {}",
                    column.name(),
                    column_type_name
                ))),
            },
        );
    }

    row_fields
}

pub fn convert_json_value_to_rust(column_type: &str, value: &Value) -> impl ToSql {
    String::from("")
}

use actix_web::actix::Message;
use eui48::Eui48;
use failure::Error;
use postgres::rows::Row;
use postgres::types::{FromSql, Type, MACADDR};
use postgres_protocol::types::macaddr_from_sql;
use std::collections::HashMap;
use std::error::Error as StdError;

// get_all_table_columns types

#[derive(Serialize)]
/// Represents a single table column returned by get_all_table_columns
pub struct GetAllTableColumnsColumn {
    pub column_name: Option<String>,
    pub column_type: Option<String>,
    pub is_nullable: Option<bool>,
    pub default_value: Option<String>,
}

/// Convenience type alias
pub type GetAllTableColumnsResult = HashMap<String, Vec<GetAllTableColumnsColumn>>;

// query_table types

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
pub enum ColumnType {
    BigInt(ColumnValue<i64>),
    // Bit(bit_vec::BitVec),
    Bool(ColumnValue<bool>),
    ByteA(ColumnValue<Vec<u8>>),
    Char(ColumnValue<String>),
    Citext(ColumnValue<String>),
    Date(ColumnValue<chrono::NaiveDate>),
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

pub type RowFields = HashMap<String, ColumnType>;

/// Analyzes a table row and returns the Rust-equivalent value
pub fn convert_row_fields(row: &Row) -> RowFields {
    let mut row_fields = HashMap::new();
    for (i, column) in row.columns().iter().enumerate() {
        let column_type_name = column.type_().name();

        row_fields.insert(
            column.name().to_string(),
            match column_type_name {
                "int8" => ColumnType::BigInt(row.get(i)),
                "bool" => ColumnType::Bool(row.get(i)),
                "bytea" => {
                    // byte array (binary)
                    ColumnType::ByteA(row.get(i))
                }
                "bpchar" => ColumnType::Char(row.get(i)), // char
                "citext" => ColumnType::Citext(row.get(i)),
                "date" => ColumnType::Date(row.get(i)),
                "float4" => ColumnType::Real(row.get(i)),
                "float8" => ColumnType::Float8(row.get(i)),
                "hstore" => ColumnType::HStore(row.get(i)),
                "int2" => ColumnType::SmallInt(row.get(i)),
                "int4" => ColumnType::Int(row.get(i)), // int
                "json" => ColumnType::Json(row.get(i)),
                "jsonb" => ColumnType::JsonB(row.get(i)),
                "macaddr" => ColumnType::MacAddr(row.get(i)),
                "name" => ColumnType::Name(row.get(i)),
                "oid" => ColumnType::Oid(row.get(i)),
                "text" => ColumnType::Text(row.get(i)),
                "time" => ColumnType::Time(row.get(i)),
                "timestamp" => ColumnType::Timestamp(row.get(i)),
                "timestamptz" => ColumnType::TimestampTz(row.get(i)),
                "uuid" => ColumnType::Uuid(row.get(i)),
                // "varbit" => {
                //     ColumnType::VarBit(row.get(i))
                // }
                "varchar" => ColumnType::VarChar(row.get(i)),
                _ => ColumnType::Text(ColumnValue::NotNullable(format!(
                    "Column {} has unsupported type: {}",
                    column.name(),
                    column_type_name
                ))),
            },
        );
    }

    row_fields
}

// used for sending queries

/// Represents a single database query to be sent via DbExecutor
pub struct Query {
    pub columns: Vec<String>,
    pub conditions: Option<String>,
    pub limit: Option<i32>,
    pub order_by: Option<String>,
    pub table: String,
    pub task: QueryTasks,
}

impl Message for Query {
    type Result = Result<QueryResult, Error>;
}

/// Represents the different query tasks that is performed by this library
pub enum QueryTasks {
    GetAllTableColumns,
    // InsertIntoTable,
    // UpsertIntoTable,
    // DeleteTableRows,
    // UpdateTableRows,
    QueryTable,
}

#[derive(Serialize)]
#[serde(untagged)]
/// Represents the response from sending a QueryTask to DbExecutor
pub enum QueryResult {
    GetAllTableColumnsResult(GetAllTableColumnsResult),
    QueryTableResult(Vec<RowFields>),
    // QueryTable(Result<
    //     Vec<
    //         HashMap<String, FromSql>
    //     >,
    //     Error
    // >),
}

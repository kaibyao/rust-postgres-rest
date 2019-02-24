use super::table_stats::TableStats;
use crate::errors::ApiError;
use crate::AppState;
use actix_web::{actix::Message, HttpRequest};
use eui48::Eui48;
use postgres::{
    accepts,
    rows::Row,
    types::{FromSql, Type, MACADDR},
};
use postgres_protocol::types::macaddr_from_sql;
use rust_decimal::Decimal;
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
pub enum ColumnType {
    BigInt(ColumnValue<i64>),
    // Bit(bit_vec::BitVec),
    Bool(ColumnValue<bool>),
    ByteA(ColumnValue<Vec<u8>>),
    Char(ColumnValue<String>),
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
                // using rust-decimal per discussion at https://www.reddit.com/r/rust/comments/a7frqj/have_anyone_reviewed_any_of_the_decimal_crates/.
                // keep in mind that at the time of this writing, diesel uses bigdecimal
                "numeric" => ColumnType::Decimal(row.get(i)),
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

/// Represents a single database query
pub struct QueryParams {
    pub columns: Vec<String>,
    pub conditions: Option<String>,
    pub distinct: Option<String>,
    pub limit: i32,
    pub offset: i32,
    pub order_by: Option<String>,
    pub table: String,
}

impl QueryParams {
    pub fn from_http_request(req: &HttpRequest<AppState>) -> Self {
        let default_limit = 10000;
        let default_offset = 0;

        let query_params = req.query();

        QueryParams {
            columns: match query_params.get("columns") {
                Some(columns_str) => columns_str
                    .split(',')
                    .map(|column_str_raw| String::from(column_str_raw.trim()))
                    .collect(),
                None => vec![],
            },
            conditions: match query_params.get("where") {
                Some(where_string) => Some(where_string.clone()),
                None => None,
            },
            distinct: match query_params.get("distinct") {
                Some(distinct_string) => Some(distinct_string.clone()),
                None => None,
            },
            limit: match query_params.get("limit") {
                Some(limit_string) => match limit_string.parse() {
                    Ok(limit_i32) => limit_i32,
                    Err(_) => default_limit,
                },
                None => default_limit,
            },
            offset: match query_params.get("offset") {
                Some(offset_string) => match offset_string.parse() {
                    Ok(offset_i32) => offset_i32,
                    Err(_) => default_offset,
                },
                None => default_offset,
            },
            order_by: match query_params.get("order_by") {
                Some(order_by_str) => Some(order_by_str.clone()),
                None => None,
            },
            table: match req.match_info().query("table") {
                Ok(table_name) => table_name,
                Err(_) => "".to_string(),
            },
        }
    }
}

/// Represents a database task (w/ included query) to be performed by DbExecutor
pub struct Query {
    pub params: QueryParams,
    pub task: QueryTasks,
}

impl Message for Query {
    type Result = Result<QueryResult, ApiError>;
}

/// Represents the different query tasks that is performed by this library
pub enum QueryTasks {
    GetAllTables,
    // InsertIntoTable,
    // UpsertIntoTable,
    // DeleteTableRows,
    // UpdateTableRows,
    QueryTable,
    QueryTableStats,
}

#[derive(Serialize)]
#[serde(untagged)]
/// Represents the response from sending a QueryTask to DbExecutor
pub enum QueryResult {
    GetAllTablesResult(Vec<String>),
    QueryTableResult(Vec<RowFields>),
    TableStats(TableStats),
    // QueryTable(Result<
    //     Vec<
    //         HashMap<String, FromSql>
    //     >,
    //     Error
    // >),
}

use actix_web::actix::Message;
use eui48::Eui48;
use failure::Error;
use postgres::rows::Row;
use std::collections::HashMap;

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
pub struct MacAddress {
    pub eui: Eui48,
}

#[derive(Debug, Serialize)]
/// Represents a postgres column's type
pub enum ColumnType {
    BigInt(i64),
    // Bit(bit_vec::BitVec),
    Bool(bool),
    ByteA(Vec<u8>),
    Char(i8),
    Citext(String),
    Date(chrono::NaiveDate),
    Float8(f64),
    HStore(HashMap<String, Option<String>>),
    Int(i32),
    Json(serde_json::Value),
    JsonB(serde_json::Value),
    MacAddr(MacAddress),
    Name(String),
    Oid(u32),
    Real(f32),
    SmallInt(i16),
    Text(String),
    Time(chrono::NaiveTime),
    Timestamp(chrono::NaiveDateTime),
    TimestampTz(chrono::DateTime<chrono::Utc>),
    // Unknown(String),
    Uuid(uuid::Uuid),
    // VarBit(bit_vec::BitVec),
    VarChar(String),
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
                "float8" => ColumnType::Float8(row.get(i)),
                "hstore" => ColumnType::HStore(row.get(i)),
                "int4" => ColumnType::Int(row.get(i)), // int
                "json" => ColumnType::Json(row.get(i)),
                "jsonb" => ColumnType::JsonB(row.get(i)),
                "name" => ColumnType::Name(row.get(i)),
                "oid" => ColumnType::Oid(row.get(i)),
                "float4" => ColumnType::Real(row.get(i)),
                "int2" => ColumnType::SmallInt(row.get(i)),
                "text" => ColumnType::Text(row.get(i)),
                "uuid" => ColumnType::Uuid(row.get(i)),
                // "varbit" => {
                //     ColumnType::VarBit(row.get(i))
                // }
                "varchar" => ColumnType::VarChar(row.get(i)),
                _ => {
                    // dbg!(column_type_name);
                    ColumnType::Text(format!(
                        "Column {} has unsupported type: {}",
                        column.name(),
                        column_type_name
                    ))
                }
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

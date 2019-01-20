use actix_web::actix::Message;
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

/// Represents a database table results, returned by a query
// pub type QueryResultRow = HashMap<String, Box<FromSql>>;
// pub type QueryResultRows = Vec<HashMap<String, _>>;

#[derive(Debug, Serialize)]
pub enum ColumnType {
    BigInt(i64),
    BigSerial(i64),
    // Bit(bit_vec::BitVec),
    Bool(bool),
    ByteA(Vec<u8>),
    Char(i8),
    Citext(String),
    // Date(chrono::NaiveDate),
    Float8(f64),
    HStore(HashMap<String, Option<String>>),
    Int(i32),
    Json(serde_json::Value),
    JsonB(serde_json::Value),
    // MacAddr(eui48::MacAddress),
    Name(String),
    Oid(u32),
    Real(f32),
    Serial(i32),
    SmallInt(i16),
    SmallSerial(i16),
    Text(String),
    // Time(chrono::NaiveTime),
    // Timestamp(chrono::NaiveDateTime),
    // TimestampTz(chrono::DateTime<chrono::Utc>),
    Unknown(String),
    // Uuid(uuid::Uuid),
    // VarBit(bit_vec::BitVec),
    VarChar(String),
}

pub type RowFields = HashMap<String, ColumnType>;

/// Analyzes a table row and returns the Rust-equivalent value
pub fn convert_row_fields(row: &Row) -> RowFields {
    let mut row_fields = HashMap::new();
    for (i, column) in row.columns().iter().enumerate() {
        let column_type_name = column.type_().name();
        match column_type_name {
            "bigint" => {
                row_fields.insert(column_type_name.to_string(), ColumnType::BigInt(row.get(i)));
            }
            _ => {
                dbg!(column_type_name);
            }
        }
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

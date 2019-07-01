#![feature(async_await)]
#![recursion_limit = "128"]

mod setup;
use setup::{setup_db, start_web_server};

use lazy_static::lazy_static;
use pretty_assertions::assert_eq;
use reqwest::{self, Client, Method, StatusCode};
use serde_json::{self, json, Value};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Once,
};

lazy_static! {
    static ref SERVER_IP_PORT: &'static str = "127.0.0.1:8000";
    static ref DB_URL: &'static str = "postgresql://postgres:example@0.0.0.0:5433/postgres";
    static ref IS_RAN_SETUP: AtomicBool = AtomicBool::new(false);
    static ref RUN_SETUP: Once = Once::new();
}

fn run_setup() {
    if !IS_RAN_SETUP.load(Ordering::SeqCst) {
        RUN_SETUP.call_once(|| {
            println!("Setting up database...");
            setup_db(&DB_URL);

            println!("starting webserver...");
            start_web_server(&DB_URL, &SERVER_IP_PORT);

            IS_RAN_SETUP.store(true, Ordering::SeqCst);
        });
    }
}

#[test]
fn index() {
    run_setup();

    let url = ["http://", &SERVER_IP_PORT, "/api/"].join("");
    let res = reqwest::get(&url).unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn index_no_slash() {
    run_setup();

    let url = ["http://", &SERVER_IP_PORT, "/api"].join("");
    let res = reqwest::get(&url).unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn get_table_names() {
    run_setup();

    let url = ["http://", &SERVER_IP_PORT, "/api/table"].join("");
    let mut res = reqwest::get(&url).unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        response_body,
        json!([
            "adult",
            "child",
            "company",
            "school",
            "test_batch_insert",
            "test_fields",
            "test_insert"
        ])
    );
}

#[test]
fn get_table_stats() {
    run_setup();

    let url = ["http://", &SERVER_IP_PORT, "/api/test_fields"].join("");
    let mut res = reqwest::get(&url).unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        response_body,
        json!({"columns":[{"column_name":"id","column_type":"int8","default_value":null,"is_nullable":false,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":null,"char_octet_length":null},{"column_name":"test_bigint","column_type":"int8","default_value":null,"is_nullable":true,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":null,"char_octet_length":null},{"column_name":"test_bigserial","column_type":"int8","default_value":null,"is_nullable":false,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":null,"char_octet_length":null},{"column_name":"test_bit","column_type":"bit","default_value":null,"is_nullable":true,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":1,"char_octet_length":null},{"column_name":"test_bool","column_type":"bool","default_value":null,"is_nullable":true,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":null,"char_octet_length":null},{"column_name":"test_bytea","column_type":"bytea","default_value":null,"is_nullable":true,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":null,"char_octet_length":null},{"column_name":"test_char","column_type":"bpchar","default_value":null,"is_nullable":true,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":1,"char_octet_length":4},{"column_name":"test_citext","column_type":"citext","default_value":null,"is_nullable":true,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":null,"char_octet_length":null},{"column_name":"test_date","column_type":"date","default_value":null,"is_nullable":true,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":null,"char_octet_length":null},{"column_name":"test_decimal","column_type":"numeric","default_value":null,"is_nullable":true,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":null,"char_octet_length":null},{"column_name":"test_f64","column_type":"float8","default_value":null,"is_nullable":true,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":null,"char_octet_length":null},{"column_name":"test_float8","column_type":"float8","default_value":null,"is_nullable":true,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":null,"char_octet_length":null},{"column_name":"test_hstore","column_type":"hstore","default_value":null,"is_nullable":true,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":null,"char_octet_length":null},{"column_name":"test_int","column_type":"int4","default_value":null,"is_nullable":true,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":null,"char_octet_length":null},{"column_name":"test_json","column_type":"json","default_value":null,"is_nullable":true,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":null,"char_octet_length":null},{"column_name":"test_jsonb","column_type":"jsonb","default_value":null,"is_nullable":true,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":null,"char_octet_length":null},{"column_name":"test_macaddr","column_type":"macaddr","default_value":null,"is_nullable":true,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":null,"char_octet_length":null},{"column_name":"test_name","column_type":"name","default_value":null,"is_nullable":true,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":null,"char_octet_length":null},{"column_name":"test_numeric","column_type":"numeric","default_value":null,"is_nullable":true,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":null,"char_octet_length":null},{"column_name":"test_oid","column_type":"oid","default_value":null,"is_nullable":true,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":null,"char_octet_length":null},{"column_name":"test_real","column_type":"float4","default_value":null,"is_nullable":true,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":null,"char_octet_length":null},{"column_name":"test_serial","column_type":"int4","default_value":null,"is_nullable":false,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":null,"char_octet_length":null},{"column_name":"test_smallint","column_type":"int2","default_value":null,"is_nullable":true,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":null,"char_octet_length":null},{"column_name":"test_smallserial","column_type":"int2","default_value":null,"is_nullable":false,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":null,"char_octet_length":null},{"column_name":"test_text","column_type":"text","default_value":null,"is_nullable":true,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":null,"char_octet_length":1_073_741_824},{"column_name":"test_time","column_type":"time","default_value":null,"is_nullable":true,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":null,"char_octet_length":null},{"column_name":"test_timestamp","column_type":"timestamp","default_value":null,"is_nullable":true,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":null,"char_octet_length":null},{"column_name":"test_timestamptz","column_type":"timestamptz","default_value":null,"is_nullable":true,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":null,"char_octet_length":null},{"column_name":"test_uuid","column_type":"uuid","default_value":"gen_random_uuid()","is_nullable":false,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":null,"char_octet_length":null},{"column_name":"test_varbit","column_type":"varbit","default_value":null,"is_nullable":true,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":null,"char_octet_length":null},{"column_name":"test_varchar","column_type":"varchar","default_value":null,"is_nullable":true,"is_foreign_key":false,"foreign_key_table":null,"foreign_key_column":null,"char_max_length":null,"char_octet_length":1_073_741_824}],"constraints":[{"name":"test_fields_pkey","table":"test_fields","columns":["id"],"constraint_type":"primary_key","definition":"PRIMARY KEY (id)","fk_table":null,"fk_columns":null}],"indexes":[{"name":"test_fields_pkey","columns":["id"],"access_method":"btree","is_exclusion":false,"is_primary_key":true,"is_unique":true}],"primary_key":["id"],"referenced_by":[]})
    );
}

#[test]
fn get_table_records_unsupported_type() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP_PORT,
        "/api/test_fields?columns=id,test_bit,test_varbit",
    ]
    .join("");
    let res = reqwest::get(&url).unwrap();

    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn get_table_records_empty_where() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP_PORT,
        "/api/test_fields?columns=id&where=",
    ]
    .join("");
    let res = reqwest::get(&url).unwrap();

    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn get_table_records_simple_where() {
    run_setup();

    let url = ["http://", &SERVER_IP_PORT, "/api/test_fields?columns=id,test_name&where=id%20%3D%2046327143679919107%20AND%20test_name%20%3D%20%27a%20name%27"].join("");
    let mut res = reqwest::get(&url).unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        response_body,
        json!([{"id": 46_327_143_679_919_107i64, "test_name": "a name"}])
    );
}

#[test]
fn get_table_records_prepared_statement() {
    run_setup();

    let url = ["http://", &SERVER_IP_PORT, "/api/test_fields?columns=id,test_name&where=id%20%3D%20%241%20AND%20test_name%20%3D%20%242&prepared_values=46327143679919107,%27a%20name%27"].join("");
    let mut res = reqwest::get(&url).unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        response_body,
        json!([{"id": 46_327_143_679_919_107i64, "test_name": "a name"}])
    );
}

#[test]
fn get_table_record_aggregates() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP_PORT,
        "/api/test_fields?columns=id,COUNT(id)&group_by=id&order_by=COUNT(id)",
    ]
    .join("");
    let mut res = reqwest::get(&url).unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(
        response_body,
        json!([{"id": 46_327_143_679_919_107i64, "count": 1}])
    );
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn get_table_record_alias() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP_PORT,
        "/api/test_fields?columns=COUNT(id) AS counted_ids&group_by=id&order_by=counted_ids",
    ]
    .join("");
    let mut res = reqwest::get(&url).unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, json!([{"counted_ids": 1}]));
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn get_table_records_wildcards() {
    run_setup();

    let url = ["http://", &SERVER_IP_PORT, "/api/child?columns=*"].join("");
    let res = reqwest::get(&url).unwrap();

    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn get_table_records_foreign_keys() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP_PORT,
        "/api/adult?columns=id,name,company_id.name",
    ]
    .join("");
    let mut res = reqwest::get(&url).unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        response_body,
        json!([{
            "id": 1,
            "name": "Ned",
            "company_id.name": "Stark Corporation",
        }])
    );
}

#[test]
fn get_table_records_foreign_keys_dot_misuse() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP_PORT,
        "/api/adult?columns=id,company_id.",
    ]
    .join("");
    let res = reqwest::get(&url).unwrap();

    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn get_table_records_foreign_keys_nested() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP_PORT,
        "/api/child?columns=id,name,parent_id.name,parent_id.company_id.name",
    ]
    .join("");
    let mut res = reqwest::get(&url).unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(
        response_body,
        json!([{
            "id": 1000,
            "name": "Robb",
            "parent_id.name": "Ned",
            "parent_id.company_id.name": "Stark Corporation",
        }])
    );
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn get_table_records_foreign_keys_nested_aliases() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP_PORT,
        "/api/child?columns=id,name,parent_id.name as parent_name,parent_id.company_id.name as parent_company_name",
    ]
    .join("");
    let mut res = reqwest::get(&url).unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(
        response_body,
        json!([{
            "id": 1000,
            "name": "Robb",
            "parent_name": "Ned",
            "parent_company_name": "Stark Corporation",
        }])
    );
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn get_table_records_foreign_keys_multiple_tables() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP_PORT,
        "/api/child?columns=id,name,parent_id.name,parent_id.company_id.name,school_id.name",
    ]
    .join("");
    let mut res = reqwest::get(&url).unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(
        response_body,
        json!([{
            "id": 1000,
            "name": "Robb",
            "parent_id.name": "Ned",
            "parent_id.company_id.name": "Stark Corporation",
            "school_id.name": "Winterfell Tower",
        }])
    );
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn get_table_records_foreign_key_wildcards() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP_PORT,
        "/api/adult?columns=id,name,company_id.*",
    ]
    .join("");
    let res = reqwest::get(&url).unwrap();

    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn post_table_record() {
    run_setup();

    let url = ["http://", &SERVER_IP_PORT, "/api/test_insert"].join("");
    let mut res = Client::new()
        .request(Method::POST, &url)
        .json(&json!([{"id": 1}]))
        .send()
        .unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, json!({ "num_rows": 1 }));
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn post_table_records() {
    run_setup();

    let url = ["http://", &SERVER_IP_PORT, "/api/test_insert"].join("");
    let mut res = Client::new()
        .request(Method::POST, &url)
        .json(&json!([{"id": 2}, {"id": 3}]))
        .send()
        .unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, json!({ "num_rows": 2 }));
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn post_table_records_returning_columns() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP_PORT,
        "/api/test_insert?returning_columns=id, name",
    ]
    .join("");
    let mut res = Client::new()
        .request(Method::POST, &url)
        .json(&json!([{"id": 4, "name": "A"}, {"id": 5, "name": "b"}]))
        .send()
        .unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(
        response_body,
        json!([{ "id": 4, "name": "A" }, { "id": 5, "name": "b" }])
    );
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn post_table_records_on_conflict_do_nothing() {
    run_setup();

    let url = ["http://", &SERVER_IP_PORT, "/api/test_insert"].join("");
    let mut res = Client::new()
        .request(Method::POST, &url)
        .json(&json!([{"id": 14, "name": "A"}]))
        .send()
        .unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, json!({ "num_rows": 1 }));
    assert_eq!(res.status(), StatusCode::OK);

    // now attempt to send same request but with different name
    let url = [
        "http://",
        &SERVER_IP_PORT,
        "/api/test_insert?conflict_action=nothing&conflict_target=id",
    ]
    .join("");
    let mut res = Client::new()
        .request(Method::POST, &url)
        .json(&json!([{"id": 14, "name": "B"}]))
        .send()
        .unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, json!({ "num_rows": 0 }));
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn post_table_records_on_conflict_do_nothing_returning_columns() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP_PORT,
        "/api/test_insert?returning_columns=id, name",
    ]
    .join("");
    let mut res = Client::new()
        .request(Method::POST, &url)
        .json(&json!([{"id": 10, "name": "A"}]))
        .send()
        .unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, json!([{ "id": 10, "name": "A" }]));
    assert_eq!(res.status(), StatusCode::OK);

    // now attempt to send same request but with different name
    let url = [
        "http://",
        &SERVER_IP_PORT,
        "/api/test_insert?returning_columns=id, name&conflict_action=nothing&conflict_target=id",
    ]
    .join("");
    let mut res = Client::new()
        .request(Method::POST, &url)
        .json(&json!([{"id": 10, "name": "B"}, {"id": 11, "name": "C"}]))
        .send()
        .unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, json!([{ "id": 11, "name": "C" }]));
    assert_eq!(res.status(), StatusCode::OK);
}

#[test]
fn post_table_records_on_conflict_update() {
    run_setup();

    let url = [
        "http://",
        &SERVER_IP_PORT,
        "/api/test_insert?returning_columns=id, name",
    ]
    .join("");
    let mut res = Client::new()
        .request(Method::POST, &url)
        .json(&json!([{"id": 12, "name": "A"}]))
        .send()
        .unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(response_body, json!([{ "id": 12, "name": "A" }]));
    assert_eq!(res.status(), StatusCode::OK);

    // now attempt to send same request but with different name
    let url = [
        "http://",
        &SERVER_IP_PORT,
        "/api/test_insert?returning_columns=id, name&conflict_action=update&conflict_target=id",
    ]
    .join("");
    let mut res = Client::new()
        .request(Method::POST, &url)
        .json(&json!([{"id": 12, "name": "B"}, {"id": 13, "name": "C"}]))
        .send()
        .unwrap();
    let response_body: Value = res.json().unwrap();

    assert_eq!(
        response_body,
        json!([{"id": 12, "name": "B"}, {"id": 13, "name": "C"}])
    );
    assert_eq!(res.status(), StatusCode::OK);
}

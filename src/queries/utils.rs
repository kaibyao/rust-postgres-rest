use super::postgres_types::RowFields;
use crate::Error;
use lazy_static::lazy_static;
use regex::Regex;
use sqlparser::{
    ast::{Expr, Function, SetExpr, Statement, UnaryOperator, Value},
    dialect::PostgreSqlDialect,
    parser::Parser,
};
use std::{borrow::BorrowMut, string::ToString};
use tokio_postgres::types::ToSql;

#[derive(Debug, PartialEq)]
/// Possible values that can be passed into a prepared statement Vec.
pub enum PreparedStatementValue {
    Boolean(bool),
    Float(f64),
    Int8(i64),
    Null,
    String(String),
}

impl From<Value> for PreparedStatementValue {
    fn from(v: Value) -> Self {
        match v {
            Value::Boolean(v) => Self::Boolean(v),
            Value::Date(v) => Self::String(v),
            Value::Double(v) => Self::Float(v.into_inner()),
            Value::HexStringLiteral(v) => Self::String(v),
            Value::Interval { .. } => unimplemented!("Interval type not supported"),
            Value::Long(v) => Self::Int8(v as i64),
            Value::NationalStringLiteral(v) => Self::String(v),
            Value::Null => Self::Null,
            Value::SingleQuotedString(v) => Self::String(v),
            Value::Time(v) => Self::String(v),
            Value::Timestamp(v) => Self::String(v),
        }
    }
}

impl PreparedStatementValue {
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

    pub fn to_sql(&self) -> &dyn ToSql {
        match self {
            Self::Boolean(v) => v,
            Self::Float(v) => v,
            Self::Int8(v) => v,
            Self::Null => &None::<String>,
            Self::String(v) => v,
        }
    }
}

/// Used for returning either number of rows or actual row values in INSERT/UPDATE statements.
pub enum UpsertResult {
    Rows(Vec<RowFields>),
    NumRowsAffected(u64),
}

// Parses a given AST and returns a tuple: (String [the converted expression that uses PREPARE
// parameters], Vec<Value>).
pub fn generate_prepared_statement_from_ast_expr(
    ast: &Expr,
) -> Result<(String, Vec<PreparedStatementValue>), Error> {
    lazy_static! {
        // need to parse integer strings as i32 or i64 so we don’t run into conversion errors
        // (because rust-postgres attempts to convert really large integer strings as i32, which fails)
        static ref INTEGER_RE: Regex = Regex::new(r"^\d+$").unwrap();

        // anything in quotes should be forced as a string
        static ref STRING_RE: Regex = Regex::new(r#"^['"](.+)['"]$"#).unwrap();
    }

    let mut ast = ast.clone();
    // mutates `ast`
    let prepared_values = generate_prepared_values(&mut ast, None);

    Ok((ast.to_string(), prepared_values))
}

/// Extracts the values being assigned and replaces them with prepared statement position parameters
/// (like `$1`, `$2`, etc.). Returns a Vec of prepared values.
fn generate_prepared_values(
    ast: &mut Expr,
    prepared_param_pos_opt: Option<&mut usize>,
) -> Vec<PreparedStatementValue> {
    let mut prepared_statement_values = vec![];
    let mut default_pos = 1;
    let prepared_param_pos = if let Some(pos) = prepared_param_pos_opt {
        pos
    } else {
        &mut default_pos
    };

    // every time there's a BinaryOp, InList, or UnaryOp extract the value
    match ast {
        Expr::Between {
            expr: between_expr_ast_box,
            low: between_low_ast_box,
            high: between_high_ast_box,
            ..
        } => {
            prepared_statement_values.extend(generate_prepared_values(
                between_expr_ast_box.borrow_mut(),
                Some(prepared_param_pos),
            ));
            prepared_statement_values.extend(generate_prepared_values(
                between_low_ast_box.borrow_mut(),
                Some(prepared_param_pos),
            ));
            prepared_statement_values.extend(generate_prepared_values(
                between_high_ast_box.borrow_mut(),
                Some(prepared_param_pos),
            ));
        }
        Expr::BinaryOp {
            left: bin_left_ast_box,
            right: bin_right_ast_box,
            ..
        } => {
            prepared_statement_values.extend(generate_prepared_values(
                bin_left_ast_box.borrow_mut(),
                Some(prepared_param_pos),
            ));
            prepared_statement_values.extend(generate_prepared_values(
                bin_right_ast_box.borrow_mut(),
                Some(prepared_param_pos),
            ));
        }
        Expr::Case {
            conditions: case_conditions_ast_vec,
            results: case_results_ast_vec,
            else_result: case_else_results_ast_box_opt,
            ..
        } => {
            for case_condition_ast in case_conditions_ast_vec {
                prepared_statement_values.extend(generate_prepared_values(
                    case_condition_ast,
                    Some(prepared_param_pos),
                ));
            }

            for case_results_ast_vec in case_results_ast_vec {
                prepared_statement_values.extend(generate_prepared_values(
                    case_results_ast_vec,
                    Some(prepared_param_pos),
                ));
            }

            if let Some(case_else_results_ast_box) = case_else_results_ast_box_opt {
                prepared_statement_values.extend(generate_prepared_values(
                    case_else_results_ast_box.borrow_mut(),
                    Some(prepared_param_pos),
                ));
            }
        }
        Expr::Cast {
            expr: cast_expr_box,
            ..
        } => {
            prepared_statement_values.extend(generate_prepared_values(
                cast_expr_box,
                Some(prepared_param_pos),
            ));
        }
        Expr::Collate { expr, .. } => {
            prepared_statement_values
                .extend(generate_prepared_values(expr, Some(prepared_param_pos)));
        }
        Expr::Extract { expr, .. } => {
            prepared_statement_values
                .extend(generate_prepared_values(expr, Some(prepared_param_pos)));
        }
        Expr::Function(Function {
            args: args_ast_vec, ..
        }) => {
            for expr in args_ast_vec {
                prepared_statement_values
                    .extend(generate_prepared_values(expr, Some(prepared_param_pos)));
            }
        }
        Expr::InList {
            expr: list_expr_ast_box,
            list: list_ast_vec,
            ..
        } => {
            prepared_statement_values.extend(generate_prepared_values(
                list_expr_ast_box.borrow_mut(),
                Some(prepared_param_pos),
            ));

            for expr in list_ast_vec {
                prepared_statement_values
                    .extend(generate_prepared_values(expr, Some(prepared_param_pos)));
            }
        }
        Expr::InSubquery { expr: expr_box, .. } => {
            prepared_statement_values.extend(generate_prepared_values(
                expr_box.borrow_mut(),
                Some(prepared_param_pos),
            ));
        }
        Expr::IsNotNull(null_ast_box) => {
            prepared_statement_values.extend(generate_prepared_values(
                null_ast_box.borrow_mut(),
                Some(prepared_param_pos),
            ));
        }
        Expr::IsNull(null_ast_box) => {
            prepared_statement_values.extend(generate_prepared_values(
                null_ast_box.borrow_mut(),
                Some(prepared_param_pos),
            ));
        }
        Expr::Nested(nested_ast_box) => {
            prepared_statement_values.extend(generate_prepared_values(
                nested_ast_box.borrow_mut(),
                Some(prepared_param_pos),
            ));
        }
        Expr::Value(val) => {
            prepared_statement_values.push(PreparedStatementValue::from(val.clone()));
            *ast = Expr::Identifier(format!("${}", prepared_param_pos));
            *prepared_param_pos += 1;
        }
        Expr::UnaryOp {
            expr: unary_expr_box,
            op,
        } => {
            let borrowed_expr = unary_expr_box.borrow_mut();
            if let Expr::Value(val) = borrowed_expr {
                let mut prepared_val = PreparedStatementValue::from(val.clone());
                match op {
                    UnaryOperator::Minus => prepared_val.invert(),
                    UnaryOperator::Not => prepared_val.invert(),
                    _ => (),
                };

                prepared_statement_values.push(prepared_val);
                *ast = Expr::Identifier(format!("${}", prepared_param_pos));
                *prepared_param_pos += 1;
            } else {
                prepared_statement_values.extend(generate_prepared_values(
                    borrowed_expr,
                    Some(prepared_param_pos),
                ));
            }
        }

        // Not supported
        Expr::CompoundIdentifier(_nested_fk_column_vec) => (),
        Expr::Exists(_query_box) => (),
        Expr::Identifier(_non_nested_column_name) => (),
        Expr::QualifiedWildcard(_wildcard_vec) => (),
        Expr::Subquery(_query_box) => (),
        Expr::Wildcard => (),
    };

    prepared_statement_values
}

/// Takes a string of columns and returns the RETURNING clause of an INSERT or UPDATE statement.
pub fn generate_returning_clause(returning_columns_opt: &Option<Vec<String>>) -> Option<String> {
    if let Some(returning_columns) = returning_columns_opt {
        return Some([" RETURNING ", &returning_columns.join(", ")].join(""));
    }

    None
}

/// Given a string of column names separated by commas, convert and return a vector of lowercase
/// strings.
pub fn normalize_columns(columns_str: &str) -> Result<Vec<String>, Error> {
    columns_str
        .split(',')
        .map(|s| {
            if s == "" {
                return Err(Error::generate_error(
                    "INCORRECT_REQUEST_BODY",
                    ["`", s, "`", " is not a valid column name. Column names must be a comma-separated list and include at least one column name."].join(""),
                ));
            }
            Ok(s.trim().to_lowercase())
        })
        .collect()
}

/// Checks a table name and returns true if it is valid (false otherwise).
/// The identifier must start with a lower-case letter or underscore, and only contain
/// alphanumeric or underscore characters. (Sorry, I don’t have time or energy for UTF-8
/// shenanigans)
pub fn validate_table_name(name: &str) -> Result<(), Error> {
    // Using lazy_static so that VALID_REGEX is only compiled once total (versus compiling the regex
    // every time this function is called)
    lazy_static! {
        static ref VALID_REGEX: Regex = Regex::new(r"^[a-z_][a-z0-9_]*$").unwrap();
    }

    if name == "table" {
        return Err(Error::generate_error(
            "SQL_IDENTIFIER_KEYWORD",
            name.to_string(),
        ));
    }

    if !VALID_REGEX.is_match(name) {
        return Err(Error::generate_error(
            "INVALID_SQL_IDENTIFIER",
            name.to_string(),
        ));
    }

    Ok(())
}

/// Like `validate_table_name`, but applies to all other identifiers. Allows parentheses (for
/// functions/aggregates like `COUNT()`), periods (for foreign key traversal), and AS aliases.
pub fn validate_where_column(name: &str) -> Result<(), Error> {
    lazy_static! {
        // Rules:
        // - Starts with a letter (or underscore).
        // - Only contains letters, numbers, underscores, and parentheses.
        // - Must not end in a dot (.) or asterisk (*).
        static ref VALID_REGEX: Regex = Regex::new(r"^[A-Za-z_][A-Za-z0-9_\(\)\.\*]*[^\.\*]$").unwrap();

    }

    if name == "table" {
        return Err(Error::generate_error(
            "SQL_IDENTIFIER_KEYWORD",
            name.to_string(),
        ));
    }

    if !VALID_REGEX.is_match(name) {
        return Err(Error::generate_error(
            "INVALID_SQL_IDENTIFIER",
            name.to_string(),
        ));
    }

    Ok(())
}

/// Check for " AS ", then validate both the original, non-aliased identifier, as well as the alias
pub fn validate_alias_identifier(identifier: &str) -> Result<Option<(&str, &str)>, Error> {
    lazy_static! {
        // Searching for " AS " alias
        static ref AS_REGEX: Regex = Regex::new(r"(?i) AS ").unwrap();
    }

    if !AS_REGEX.is_match(identifier) {
        validate_where_column(identifier)?;
        return Ok(None);
    }

    let matched = AS_REGEX.find(identifier).unwrap();
    let orig = &identifier[..matched.start()];
    let alias = &identifier[matched.end()..];

    validate_where_column(orig)?;
    validate_where_column(alias)?;

    Ok(Some((orig, alias)))
}

/// Converts a WHERE clause string into an Expr.
pub fn where_clause_str_to_ast(clause: &str) -> Result<Option<Expr>, Error> {
    let full_statement = ["SELECT * FROM a_table WHERE ", clause].join("");
    let dialect = PostgreSqlDialect {};

    // convert the statement into an AST, and then extract the "WHERE" portion of the AST
    let mut parsed = Parser::parse_sql(&dialect, full_statement)?;
    let statement_ast = parsed.remove(0);

    if let Statement::Query(query_box) = statement_ast {
        return Ok(extract_where_ast_from_setexpr(query_box.to_owned().body));
    }

    Ok(None)
}

/// Finds and returns the Expr that represents the WHERE clause of a SELECT statement
fn extract_where_ast_from_setexpr(expr: SetExpr) -> Option<Expr> {
    match expr {
        SetExpr::Query(boxed_sql_query) => extract_where_ast_from_setexpr(boxed_sql_query.body),
        SetExpr::Select(select_box) => select_box.to_owned().selection,
        SetExpr::SetOperation { .. } => unimplemented!("Set operations not supported"),
        SetExpr::Values(_) => unimplemented!("Values not supported"),
    }
}

#[cfg(test)]
mod validate_table_name_tests {
    use super::*;

    #[test]
    fn simple_string() {
        assert!(validate_table_name("test").is_ok());
    }

    #[test]
    fn with_underscore() {
        assert!(validate_table_name("a_table").is_ok());
    }

    #[test]
    fn begins_with_underscore() {
        assert!(validate_table_name("_a_table").is_ok());
        assert!(validate_table_name("_0_table").is_ok());
    }

    #[test]
    fn uppercase() {
        assert!(validate_table_name("TEST").is_err());
    }

    #[test]
    fn reserved_keywords() {
        assert!(validate_table_name("table").is_err());
    }

    #[test]
    fn invalid_characters() {
        assert!(validate_table_name("ü_table").is_err());
        assert!(validate_table_name("table_ü").is_err());
    }

    #[test]
    fn empty_string() {
        assert!(validate_table_name("").is_err());
    }

    #[test]
    fn white_space() {
        assert!(validate_table_name(" ").is_err());
        assert!(validate_table_name("\n").is_err());
        assert!(validate_table_name("\t").is_err());
    }
}

#[cfg(test)]
mod validate_where_column_tests {
    use super::*;

    #[test]
    fn count() {
        assert!(validate_where_column("COUNT(*)").is_ok());
        assert!(validate_where_column("COUNT(id)").is_ok());
    }

    #[test]
    fn foreign_keys() {
        assert!(validate_where_column("user_id.company_id.name").is_ok());
        assert!(validate_where_column("user_id.company_id.").is_err());
    }
}

#[cfg(test)]
mod validate_alias_identifier_tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn as_alias() {
        // may change later
        assert_eq!(
            validate_alias_identifier("arya AS arry").unwrap(),
            Some(("arya", "arry"))
        );
        assert!(validate_alias_identifier(" AS arry").is_err());
        assert!(validate_alias_identifier("arya AS arry AS cat").is_err());
        assert!(validate_alias_identifier("arya AS  AS arry").is_err());
    }
}

#[cfg(test)]
mod where_clause_str_to_ast_tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use sqlparser::ast::BinaryOperator;

    #[test]
    fn basic() {
        let clause = "a > b";
        let expected = Expr::BinaryOp {
            left: Box::new(Expr::Identifier("a".to_string())),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::Identifier("b".to_string())),
        };
        assert_eq!(where_clause_str_to_ast(clause).unwrap().unwrap(), expected);
    }

    #[test]
    fn foreign_keys() {
        let clause = "a.b > c";
        let expected = Expr::BinaryOp {
            left: Box::new(Expr::CompoundIdentifier(vec![
                "a".to_string(),
                "b".to_string(),
            ])),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::Identifier("c".to_string())),
        };
        assert_eq!(where_clause_str_to_ast(clause).unwrap().unwrap(), expected);
    }

    #[test]
    fn empty_string_returns_error() {
        let clause = "";
        assert!(where_clause_str_to_ast(clause).is_err());
    }

    #[test]
    fn empty_parentheses_returns_err() {
        let clause = "()";
        assert!(where_clause_str_to_ast(clause).is_err());
    }

    #[test]
    fn invalid_clause_returns_err() {
        let clause = "not valid WHERE syntax";
        assert!(where_clause_str_to_ast(clause).is_err());
    }
}

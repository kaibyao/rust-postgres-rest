use super::{
    foreign_keys::{fk_ast_nodes_from_where_ast, ForeignKeyReference},
    select_table_stats::TableColumnStat,
};
use crate::Error;
use lazy_static::lazy_static;
use regex::Regex;
use sqlparser::{
    ast::{Expr, SetExpr, Statement},
    dialect::PostgreSqlDialect,
    parser::Parser,
};
use std::{collections::HashMap, string::ToString};

/// Takes a string of columns and returns the RETURNING clause of an INSERT or UPDATE statement.
// pub fn generate_returning_clause(returning_columns_opt: &Option<Vec<String>>) -> Option<String> {
//     if let Some(returning_columns) = returning_columns_opt {
//         return Some([" RETURNING ", &returning_columns.join(", ")].join(""));
//     }

//     None
// }

/// Generates the WHERE clause and a HashMap of column name : column type after taking foreign keys
/// into account. Mutates the original AST.
pub fn get_where_string<'a>(
    where_ast: &mut Expr,
    table: &str,
    stats: &[TableColumnStat],
    fks: &'a [ForeignKeyReference],
) -> (String, HashMap<String, String>) {
    let where_ast_nodes = fk_ast_nodes_from_where_ast(where_ast, true);
    let mut column_types = HashMap::new();

    for (ast_column_name, ast_node) in where_ast_nodes {
        if let (true, Some((fk_ref, fk_column))) = (
            !fks.is_empty(),
            ForeignKeyReference::find(fks, table, &ast_column_name),
        ) {
            let replacement_node = match ast_node {
                Expr::QualifiedWildcard(_wildcard_vec) => {
                    let actual_column_name =
                        vec![fk_ref.table_referred.clone(), fk_column.to_string()];
                    column_types.insert(
                        actual_column_name.join("."),
                        fk_ref.foreign_key_column_type.clone(),
                    );
                    Expr::QualifiedWildcard(actual_column_name)
                }
                Expr::CompoundIdentifier(_nested_fk_column_vec) => {
                    let actual_column_name =
                        vec![fk_ref.table_referred.clone(), fk_column.to_string()];
                    column_types.insert(
                        actual_column_name.join("."),
                        fk_ref.foreign_key_column_type.clone(),
                    );
                    Expr::CompoundIdentifier(actual_column_name)
                }
                _ => unimplemented!(
                    "The WHERE clause HashMap only contains wildcards and compound identifiers."
                ),
            };

            *ast_node = replacement_node;
        } else if let Some(stat) = stats.iter().find(|s| s.column_name == ast_column_name) {
            column_types.insert(ast_column_name, stat.column_type.clone());
        }
    }

    (where_ast.to_string(), column_types)
}

/// Extracts the "real" column name (taking foreign keys and aliases into account).
/// Returns a Vec of &str tokens that can later be used in `.extend()` or `.join("")`.
pub fn get_db_column_str<'a>(
    column: &'a str,
    table: &'a str,
    fks: &'a [ForeignKeyReference],
    // whether the column tokens should contain an " AS " alias
    is_return_alias: bool,
    is_returned_column_prefixed_with_table: bool,
) -> Result<Vec<&'a str>, Error> {
    if fks.is_empty() {
        let _ = validate_alias_identifier(column)?;
        Ok(vec![column])
    } else {
        let validate_alias_result = validate_alias_identifier(column)?;
        let (column, alias, has_alias) =
            if let Some((actual_column_ref, alias)) = validate_alias_result {
                (actual_column_ref, alias, true)
            } else {
                (&column[..], "", false)
            };

        let mut tokens = vec![];

        if let (true, Some((fk_ref, fk_column))) = (
            !fks.is_empty(),
            ForeignKeyReference::find(fks, table, column),
        ) {
            if is_returned_column_prefixed_with_table {
                tokens.push(fk_ref.table_referred.as_str());
                tokens.push(".");
            }
            tokens.push(fk_column);

            // AS syntax (to avoid ambiguous columns)
            if is_return_alias {
                tokens.push(" AS \"");
                tokens.push(if has_alias { alias } else { column });
                tokens.push("\"");
            }
        } else {
            if is_returned_column_prefixed_with_table {
                // Current column is not an FK, but we still need to use actual table names to avoid
                // ambiguous columns. Example: If I'm trying to retrieve the ID field of an employee
                // as well as its company and they're both called "id", I would get
                // an ambiguity error.
                tokens.push(table);
                tokens.push(".");
            }
            tokens.push(column);

            // AS syntax (to avoid ambiguous columns)
            if is_return_alias {
                tokens.push(" AS \"");
                tokens.push(if has_alias { alias } else { column });
                tokens.push("\"");
            }
        }

        Ok(tokens)
    }
}

/// Generates a string of column names delimited by commas. Foreign keys are correctly accounted
/// for.
pub fn get_columns_str<'a>(
    columns: &'a [String],
    table: &'a str,
    fks: &'a [ForeignKeyReference],
) -> Result<Vec<&'a str>, Error> {
    let mut statement: Vec<&str> = vec![];

    for (i, column) in columns.iter().enumerate() {
        let column_tokens = get_db_column_str(column, table, fks, true, true)?;
        statement.extend(column_tokens);

        if i < columns.len() - 1 {
            statement.push(", ");
        }
    }

    Ok(statement)
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

/// Check for " AS ", then validate both the original, non-aliased identifier, as well as the alias.
/// If it is an alias, return a tuple containing: (actual column name, column alias).
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
mod get_db_column_str_tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn foreign_keys_nested() {
        let column = "parent_id.company_id.name".to_string();
        let fks = [ForeignKeyReference {
            original_refs: vec!["parent_id.company_id.name".to_string()],
            referring_table: "child".to_string(),
            referring_column: "parent_id".to_string(),
            referring_column_type: "int8".to_string(),
            table_referred: "adult".to_string(),
            foreign_key_column: "id".to_string(),
            foreign_key_column_type: "int8".to_string(),
            nested_fks: vec![ForeignKeyReference {
                original_refs: vec!["company_id.name".to_string()],
                referring_table: "adult".to_string(),
                referring_column: "company_id".to_string(),
                referring_column_type: "int8".to_string(),
                table_referred: "company".to_string(),
                foreign_key_column: "id".to_string(),
                foreign_key_column_type: "int8".to_string(),
                nested_fks: vec![],
            }],
        }];
        let table = "child";

        let column_str = get_db_column_str(&column, table, &fks, true, true)
            .unwrap()
            .join("");
        assert_eq!(column_str, r#"company.name AS "parent_id.company_id.name""#);
    }

    #[test]
    fn foreign_keys_nested_no_return_alias() {
        let column = "parent_id.company_id.name".to_string();
        let fks = [ForeignKeyReference {
            original_refs: vec!["parent_id.company_id.name".to_string()],
            referring_table: "child".to_string(),
            referring_column: "parent_id".to_string(),
            referring_column_type: "int8".to_string(),
            table_referred: "adult".to_string(),
            foreign_key_column: "id".to_string(),
            foreign_key_column_type: "int8".to_string(),
            nested_fks: vec![ForeignKeyReference {
                original_refs: vec!["company_id.name".to_string()],
                referring_table: "adult".to_string(),
                referring_column: "company_id".to_string(),
                referring_column_type: "int8".to_string(),
                table_referred: "company".to_string(),
                foreign_key_column: "id".to_string(),
                foreign_key_column_type: "int8".to_string(),
                nested_fks: vec![],
            }],
        }];
        let table = "child";

        let column_str = get_db_column_str(&column, table, &fks, false, true)
            .unwrap()
            .join("");
        assert_eq!(column_str, "company.name");
    }

    #[test]
    fn foreign_keys_nested_no_return_alias_no_table_prefix() {
        let column = "parent_id.company_id.name".to_string();
        let fks = [ForeignKeyReference {
            original_refs: vec!["parent_id.company_id.name".to_string()],
            referring_table: "child".to_string(),
            referring_column: "parent_id".to_string(),
            referring_column_type: "int8".to_string(),
            table_referred: "adult".to_string(),
            foreign_key_column: "id".to_string(),
            foreign_key_column_type: "int8".to_string(),
            nested_fks: vec![ForeignKeyReference {
                original_refs: vec!["company_id.name".to_string()],
                referring_table: "adult".to_string(),
                referring_column: "company_id".to_string(),
                referring_column_type: "int8".to_string(),
                table_referred: "company".to_string(),
                foreign_key_column: "id".to_string(),
                foreign_key_column_type: "int8".to_string(),
                nested_fks: vec![],
            }],
        }];
        let table = "child";

        let column_str = get_db_column_str(&column, table, &fks, false, false)
            .unwrap()
            .join("");
        assert_eq!(column_str, "name");
    }

    #[test]
    fn foreign_keys_nested_alias() {
        let column = "parent_id.company_id.name AS parent_company".to_string();
        let fks = [ForeignKeyReference {
            original_refs: vec!["parent_id.company_id.name".to_string()],
            referring_table: "child".to_string(),
            referring_column: "parent_id".to_string(),
            referring_column_type: "int8".to_string(),
            table_referred: "adult".to_string(),
            foreign_key_column: "id".to_string(),
            foreign_key_column_type: "int8".to_string(),
            nested_fks: vec![ForeignKeyReference {
                original_refs: vec!["company_id.name".to_string()],
                referring_table: "adult".to_string(),
                referring_column: "company_id".to_string(),
                referring_column_type: "int8".to_string(),
                table_referred: "company".to_string(),
                foreign_key_column: "id".to_string(),
                foreign_key_column_type: "int8".to_string(),
                nested_fks: vec![],
            }],
        }];
        let table = "child";

        let column_str = get_db_column_str(&column, table, &fks, true, true)
            .unwrap()
            .join("");
        assert_eq!(column_str, r#"company.name AS "parent_company""#);
    }

    #[test]
    fn foreign_keys_nested_alias_no_return_alias() {
        let column = "parent_id.company_id.name AS parent_company".to_string();
        let fks = [ForeignKeyReference {
            original_refs: vec!["parent_id.company_id.name".to_string()],
            referring_table: "child".to_string(),
            referring_column: "parent_id".to_string(),
            referring_column_type: "int8".to_string(),
            table_referred: "adult".to_string(),
            foreign_key_column: "id".to_string(),
            foreign_key_column_type: "int8".to_string(),
            nested_fks: vec![ForeignKeyReference {
                original_refs: vec!["company_id.name".to_string()],
                referring_table: "adult".to_string(),
                referring_column: "company_id".to_string(),
                referring_column_type: "int8".to_string(),
                table_referred: "company".to_string(),
                foreign_key_column: "id".to_string(),
                foreign_key_column_type: "int8".to_string(),
                nested_fks: vec![],
            }],
        }];
        let table = "child";

        let column_str = get_db_column_str(&column, table, &fks, false, true)
            .unwrap()
            .join("");
        assert_eq!(column_str, "company.name");
    }

    #[test]
    fn foreign_keys_nested_alias_no_return_alias_no_table_prefix() {
        let column = "parent_id.company_id.name AS parent_company".to_string();
        let fks = [ForeignKeyReference {
            original_refs: vec!["parent_id.company_id.name".to_string()],
            referring_table: "child".to_string(),
            referring_column: "parent_id".to_string(),
            referring_column_type: "int8".to_string(),
            table_referred: "adult".to_string(),
            foreign_key_column: "id".to_string(),
            foreign_key_column_type: "int8".to_string(),
            nested_fks: vec![ForeignKeyReference {
                original_refs: vec!["company_id.name".to_string()],
                referring_table: "adult".to_string(),
                referring_column: "company_id".to_string(),
                referring_column_type: "int8".to_string(),
                table_referred: "company".to_string(),
                foreign_key_column: "id".to_string(),
                foreign_key_column_type: "int8".to_string(),
                nested_fks: vec![],
            }],
        }];
        let table = "child";

        let column_str = get_db_column_str(&column, table, &fks, false, false)
            .unwrap()
            .join("");
        assert_eq!(column_str, "name");
    }
}

#[cfg(test)]
mod get_column_str_tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn foreign_keys_nested() {
        let columns = vec!["id".to_string(), "parent_id.company_id.name".to_string()];
        let fks = [ForeignKeyReference {
            original_refs: vec!["parent_id.company_id.name".to_string()],
            referring_table: "child".to_string(),
            referring_column: "parent_id".to_string(),
            referring_column_type: "int8".to_string(),
            table_referred: "adult".to_string(),
            foreign_key_column: "id".to_string(),
            foreign_key_column_type: "int8".to_string(),
            nested_fks: vec![ForeignKeyReference {
                original_refs: vec!["company_id.name".to_string()],
                referring_table: "adult".to_string(),
                referring_column: "company_id".to_string(),
                referring_column_type: "int8".to_string(),
                table_referred: "company".to_string(),
                foreign_key_column: "id".to_string(),
                foreign_key_column_type: "int8".to_string(),
                nested_fks: vec![],
            }],
        }];
        let table = "child";

        let column_str = get_columns_str(&columns, table, &fks).unwrap().join("");
        assert_eq!(
            column_str,
            r#"child.id AS "id", company.name AS "parent_id.company_id.name""#
        );
    }

    #[test]
    fn foreign_keys_nested_more_than_one() {
        let columns = vec![
            "parent_id.name".to_string(),
            "parent_id.company_id.name".to_string(),
        ];
        let fks = [ForeignKeyReference {
            original_refs: vec![
                "parent_id.company_id.name".to_string(),
                "parent_id.name".to_string(),
            ],
            referring_table: "child".to_string(),
            referring_column: "parent_id".to_string(),
            referring_column_type: "int8".to_string(),
            table_referred: "adult".to_string(),
            foreign_key_column: "id".to_string(),
            foreign_key_column_type: "int8".to_string(),
            nested_fks: vec![ForeignKeyReference {
                original_refs: vec!["company_id.name".to_string()],
                referring_table: "adult".to_string(),
                referring_column: "company_id".to_string(),
                referring_column_type: "int8".to_string(),
                table_referred: "company".to_string(),
                foreign_key_column: "id".to_string(),
                foreign_key_column_type: "int8".to_string(),
                nested_fks: vec![],
            }],
        }];
        let table = "child";

        let column_str = get_columns_str(&columns, table, &fks).unwrap().join("");
        assert_eq!(
            column_str,
            r#"adult.name AS "parent_id.name", company.name AS "parent_id.company_id.name""#
        );
    }

    #[test]
    fn foreign_keys_nested_alias() {
        let columns = vec![
            "id".to_string(),
            "parent_id.company_id.name AS parent_company".to_string(),
        ];
        let fks = [ForeignKeyReference {
            original_refs: vec!["parent_id.company_id.name".to_string()],
            referring_table: "child".to_string(),
            referring_column: "parent_id".to_string(),
            referring_column_type: "int8".to_string(),
            table_referred: "adult".to_string(),
            foreign_key_column: "id".to_string(),
            foreign_key_column_type: "int8".to_string(),
            nested_fks: vec![ForeignKeyReference {
                original_refs: vec!["company_id.name".to_string()],
                referring_table: "adult".to_string(),
                referring_column: "company_id".to_string(),
                referring_column_type: "int8".to_string(),
                table_referred: "company".to_string(),
                foreign_key_column: "id".to_string(),
                foreign_key_column_type: "int8".to_string(),
                nested_fks: vec![],
            }],
        }];
        let table = "child";

        let column_str = get_columns_str(&columns, table, &fks).unwrap().join("");
        assert_eq!(
            column_str,
            r#"child.id AS "id", company.name AS "parent_company""#
        );
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

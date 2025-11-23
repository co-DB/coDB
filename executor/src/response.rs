use engine::record::Record;
use metadata::types::Type;
use planner::resolved_tree::ResolvedColumn;

use crate::error_factory::InternalExecutorError;

/// Metadata about a column in a query result set.
#[derive(Debug)]
pub struct ColumnData {
    pub name: String,
    pub ty: Type,
}

impl From<&ResolvedColumn> for ColumnData {
    fn from(value: &ResolvedColumn) -> Self {
        ColumnData {
            name: value.name.clone(),
            ty: value.ty,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum StatementType {
    Insert,
    Update,
    Delete,
    Create,
    Alter,
    Truncate,
    Drop,
}

/// Result of executing a single SQL statement.
///
/// This enum represents all possible outcomes when executing a statement:
/// - `OperationSuccessful`: Mutation operations (INSERT, UPDATE, DELETE, CREATE, etc.)
///   that completed successfully, reporting the number of affected rows
/// - `SelectSuccessful`: SELECT queries that return a result set with column metadata
///   and data rows
/// - `ParseError`: Statement could not be parsed due to syntax errors
/// - `RuntimeError`: Statement was valid but failed during execution (e.g., constraint
///   violations, type mismatches, division by zero)
#[derive(Debug)]
pub enum StatementResult {
    OperationSuccessful {
        rows_affected: usize,
        ty: StatementType,
    },
    SelectSuccessful {
        columns: Vec<ColumnData>,
        rows: Vec<Record>,
    },
    ParseError {
        error: String,
    },
    RuntimeError {
        error: String,
    },
}

impl From<&InternalExecutorError> for StatementResult {
    fn from(value: &InternalExecutorError) -> Self {
        StatementResult::RuntimeError {
            error: value.to_string(),
        }
    }
}

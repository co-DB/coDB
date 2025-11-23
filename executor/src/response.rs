use engine::record::Record;
use metadata::types::Type;
use planner::resolved_tree::ResolvedColumn;

use crate::error_factory::InternalExecutorError;

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

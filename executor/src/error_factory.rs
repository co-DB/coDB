//! error_factory - provides helpers for creating errors across `executor` crate.

use engine::heap_file::HeapFileError;
use thiserror::Error;
use types::{data::Value, schema::Type};

use crate::StatementResult;

/// Error for internal executor operations, shouldn't be exported outside of this module.
#[derive(Error, Debug)]
pub(crate) enum InternalExecutorError {
    #[error("Table '{table_name}' does not exist.")]
    TableDoesNotExist { table_name: String },
    #[error("Cannot create heap file: {reason}")]
    CannotCreateHeapFile { reason: String },
    #[error("Cannot create b-tree: {reason}")]
    CannotCreateBTree { reason: String },
    #[error("{0}")]
    HeapFileError(#[from] HeapFileError),
    #[error("Used invalid operation ({operation}) for data source")]
    InvalidOperationInDataSource { operation: String },
    #[error("Received unexpected node type ({node_type}) while processing expression")]
    InvalidNodeTypeInExpression { node_type: String },
    #[error("Invalid type (expected: {expected}, got: {got})")]
    UnexpectedType { expected: String, got: String },
    #[error("'{lhs}' and '{rhs}' are incompatible")]
    IncompatibleTypes { lhs: String, rhs: String },
    #[error("{message}")]
    ArithmeticOperationError { message: String },
    #[error("cannot cast '{from}' to '{to}'")]
    InvalidCast { from: String, to: String },
    #[error("failed to load column ({column_name}) value from context")]
    CannotLoadColumnValueFromContext { column_name: String },
    #[error("cannot compare NaN values ('{lhs}' and '{rhs}')")]
    ComparingNaNValues { lhs: String, rhs: String },
}

/// Helper to create [`StatementResult::RuntimeError`] with provided message.
pub(crate) fn runtime_error(msg: impl Into<String>) -> StatementResult {
    StatementResult::RuntimeError { error: msg.into() }
}

pub(crate) fn unexpected_type(expected: impl Into<String>, got: &Value) -> InternalExecutorError {
    InternalExecutorError::UnexpectedType {
        expected: expected.into(),
        got: format!("{:?}", got),
    }
}

pub(crate) fn incompatible_types(lhs: &Value, rhs: &Value) -> InternalExecutorError {
    InternalExecutorError::IncompatibleTypes {
        lhs: format!("{:?}", lhs),
        rhs: format!("{:?}", rhs),
    }
}

pub(crate) fn mod_by_zero() -> InternalExecutorError {
    InternalExecutorError::ArithmeticOperationError {
        message: "cannot modulo by 0".into(),
    }
}

pub(crate) fn div_by_zero() -> InternalExecutorError {
    InternalExecutorError::ArithmeticOperationError {
        message: "cannot divide by 0".into(),
    }
}

pub(crate) fn invalid_cast(child: &Value, new_type: &Type) -> InternalExecutorError {
    InternalExecutorError::InvalidCast {
        from: child.ty().to_string(),
        to: new_type.to_string(),
    }
}

pub(crate) fn invalid_node_type(node_type: impl Into<String>) -> InternalExecutorError {
    InternalExecutorError::InvalidNodeTypeInExpression {
        node_type: node_type.into(),
    }
}

pub(crate) fn cannot_load_column_from_context(
    column_name: impl Into<String>,
) -> InternalExecutorError {
    InternalExecutorError::CannotLoadColumnValueFromContext {
        column_name: column_name.into(),
    }
}

pub(crate) fn comparing_nan_values(
    lhs: impl Into<String>,
    rhs: impl Into<String>,
) -> InternalExecutorError {
    InternalExecutorError::ComparingNaNValues {
        lhs: lhs.into(),
        rhs: rhs.into(),
    }
}

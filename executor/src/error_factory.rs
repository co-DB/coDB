//! error_factory - provides helpers for creating errors across `executor` crate.

use engine::{heap_file::HeapFileError, record::Field};
use metadata::types::Type;
use thiserror::Error;

use crate::StatementResult;

/// Error for internal executor operations, shouldn't be exported outside of this module.
#[derive(Error, Debug)]
pub(crate) enum InternalExecutorError {
    #[error("Table '{table_name}' does not exist.")]
    TableDoesNotExist { table_name: String },
    #[error("Cannot create heap file: {reason}")]
    CannotCreateHeapFile { reason: String },
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
}

/// Helper to create [`StatementResult::RuntimeError`] with provided message.
pub(crate) fn runtime_error(msg: impl Into<String>) -> StatementResult {
    StatementResult::RuntimeError { error: msg.into() }
}

pub(crate) fn unexpected_type(expected: impl Into<String>, got: &Field) -> InternalExecutorError {
    InternalExecutorError::UnexpectedType {
        expected: expected.into(),
        got: format!("{:?}", got),
    }
}

pub(crate) fn incompatible_types(lhs: &Field, rhs: &Field) -> InternalExecutorError {
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

pub(crate) fn invalid_cast(child: &Field, new_type: &Type) -> InternalExecutorError {
    InternalExecutorError::InvalidCast {
        from: child.ty().to_string(),
        to: new_type.to_string(),
    }
}

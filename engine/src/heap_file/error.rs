use storage::cache::CacheError;
use thiserror::Error;
use types::serialization::DbSerializationError;

use crate::{record::RecordError, slotted_page::SlottedPageError};

#[derive(Debug, Error)]
pub enum HeapFileError {
    #[error("invalid metadata page: {error}")]
    InvalidMetadataPage { error: String },
    #[error("failed to deserialize record entry: {error}")]
    CorruptedRecordEntry { error: String },
    #[error("cache error occurred: {0}")]
    CacheError(#[from] CacheError),
    #[error("slotted page error occurred: {0}")]
    SlottedPageError(#[from] SlottedPageError),
    #[error("failed to serialize/deserialize record: {0}")]
    RecordSerializationError(#[from] RecordError),
    #[error("there was not enough space on page to perform request")]
    NotEnoughSpaceOnPage,
    #[error("column type ({column_ty}) and value type ({value_ty}) do not match")]
    ColumnAndValueTypesDontMatch { column_ty: String, value_ty: String },
    #[error("{0}")]
    DBSerializationError(#[from] DbSerializationError),
}

use serde::{Deserialize, Serialize};

/// Represents all possible types that value in coSQL can get.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Type {
    String,
    F32,
    F64,
    I32,
    I64,
    Bool,
    Date,
    DateTime,
}

impl Type {
    pub fn is_fixed_size(&self) -> bool {
        match self {
            Type::String => false,
            Type::F32 => true,
            Type::F64 => true,
            Type::I32 => true,
            Type::I64 => true,
            Type::Bool => true,
            Type::Date => true,
            Type::DateTime => true,
        }
    }
}

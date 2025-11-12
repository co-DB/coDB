use std::fmt;

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
        !matches!(self, Type::String)
    }

    /// Returns `Some(type)` where `type` can hold both values of `lhs` and `rhs`.
    /// If such type does not exist `None` is returned.
    pub fn coercion(lhs: &Type, rhs: &Type) -> Option<Type> {
        if lhs == rhs {
            return Some(*lhs);
        }

        match (lhs, rhs) {
            (Type::I32, Type::I64) | (Type::I64, Type::I32) => Some(Type::I64),
            (Type::F32, Type::F64) | (Type::F64, Type::F32) => Some(Type::F64),
            _ => None,
        }
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Type::String => write!(f, "String"),
            Type::F32 => write!(f, "Float32"),
            Type::F64 => write!(f, "Float64"),
            Type::I32 => write!(f, "Int32"),
            Type::I64 => write!(f, "Int64"),
            Type::Bool => write!(f, "Bool"),
            Type::Date => write!(f, "Date"),
            Type::DateTime => write!(f, "DateTime"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_coercion(t1: &Type, t2: &Type, expected: Type) {
        let output = Type::coercion(t1, t2).unwrap();
        assert_eq!(expected, output);
    }

    fn assert_coercion_none(t1: &Type, t2: &Type) {
        let output = Type::coercion(t1, t2);
        assert!(output.is_none());
    }

    #[test]
    fn type_coercion_same_type() {
        let t1 = Type::String;
        let t2 = Type::String;
        assert_coercion(&t1, &t2, Type::String);
        let t1 = Type::F32;
        let t2 = Type::F32;
        assert_coercion(&t1, &t2, Type::F32);
        let t1 = Type::F64;
        let t2 = Type::F64;
        assert_coercion(&t1, &t2, Type::F64);
        let t1 = Type::I32;
        let t2 = Type::I32;
        assert_coercion(&t1, &t2, Type::I32);
        let t1 = Type::I64;
        let t2 = Type::I64;
        assert_coercion(&t1, &t2, Type::I64);
        let t1 = Type::Bool;
        let t2 = Type::Bool;
        assert_coercion(&t1, &t2, Type::Bool);
        let t1 = Type::Date;
        let t2 = Type::Date;
        assert_coercion(&t1, &t2, Type::Date);
        let t1 = Type::DateTime;
        let t2 = Type::DateTime;
        assert_coercion(&t1, &t2, Type::DateTime);
    }

    #[test]
    fn type_coercion_i32() {
        let t1 = Type::I32;
        let t2 = Type::I64;
        assert_coercion(&t1, &t2, Type::I64);

        let t1 = Type::I64;
        let t2 = Type::I32;
        assert_coercion(&t1, &t2, Type::I64);
    }

    #[test]
    fn type_coercion_f32() {
        let t1 = Type::F32;
        let t2 = Type::F64;
        assert_coercion(&t1, &t2, Type::F64);

        let t1 = Type::F64;
        let t2 = Type::F32;
        assert_coercion(&t1, &t2, Type::F64);
    }

    #[test]
    fn type_coercion_none() {
        let t1 = Type::F32;
        let t2 = Type::String;
        assert_coercion_none(&t1, &t2);

        let t1 = Type::Date;
        let t2 = Type::DateTime;
        assert_coercion_none(&t1, &t2);

        let t1 = Type::DateTime;
        let t2 = Type::F64;
        assert_coercion_none(&t1, &t2);

        let t1 = Type::Bool;
        let t2 = Type::I32;
        assert_coercion_none(&t1, &t2);
    }
}

use core::fmt;

use metadata::types::Type;

#[derive(Debug, Clone, Copy)]
pub(crate) enum BinaryOperator {
    Plus,
    Minus,
    Star,
    Slash,
    Modulo,
    Equal,
    NotEqual,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum LogicalOperator {
    And,
    Or,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum UnaryOperator {
    Plus,
    Minus,
    Bang,
}

/// [`SupportsType`] should be implement for each of coDB operator. It's used for checking compatibiliy between operator and [`Type`].
pub(crate) trait SupportsType {
    /// Returns `true` if `ty` can be used with `self`.
    fn supports_type(&self, ty: &Type) -> bool;
}

impl SupportsType for BinaryOperator {
    fn supports_type(&self, ty: &Type) -> bool {
        match (self, ty) {
            (BinaryOperator::Plus, Type::Bool) => false,
            (BinaryOperator::Plus, Type::Date) => false,
            (BinaryOperator::Plus, Type::DateTime) => false,
            (BinaryOperator::Plus, _) => true,
            (BinaryOperator::Minus, Type::Bool) => false,
            (BinaryOperator::Minus, Type::Date) => false,
            (BinaryOperator::Minus, Type::DateTime) => false,
            (BinaryOperator::Minus, Type::String) => false,
            (BinaryOperator::Minus, _) => true,
            (BinaryOperator::Star, Type::String) => false,
            (BinaryOperator::Star, Type::Bool) => false,
            (BinaryOperator::Star, Type::Date) => false,
            (BinaryOperator::Star, Type::DateTime) => false,
            (BinaryOperator::Star, _) => true,
            (BinaryOperator::Slash, Type::String) => false,
            (BinaryOperator::Slash, Type::Bool) => false,
            (BinaryOperator::Slash, Type::Date) => false,
            (BinaryOperator::Slash, Type::DateTime) => false,
            (BinaryOperator::Slash, _) => true,
            (BinaryOperator::Modulo, Type::I32) => true,
            (BinaryOperator::Modulo, Type::I64) => true,
            (BinaryOperator::Modulo, _) => false,
            (BinaryOperator::Equal, _) => true,
            (BinaryOperator::NotEqual, _) => true,
            (BinaryOperator::Greater, Type::String) => false,
            (BinaryOperator::Greater, _) => true,
            (BinaryOperator::GreaterEqual, Type::String) => false,
            (BinaryOperator::GreaterEqual, _) => true,
            (BinaryOperator::Less, Type::String) => false,
            (BinaryOperator::Less, _) => true,
            (BinaryOperator::LessEqual, Type::String) => false,
            (BinaryOperator::LessEqual, _) => true,
        }
    }
}

impl SupportsType for LogicalOperator {
    fn supports_type(&self, ty: &Type) -> bool {
        matches!(ty, Type::Bool)
    }
}

impl SupportsType for UnaryOperator {
    fn supports_type(&self, ty: &Type) -> bool {
        match (self, ty) {
            (UnaryOperator::Plus, Type::Date) => false,
            (UnaryOperator::Plus, Type::DateTime) => false,
            (UnaryOperator::Plus, _) => true,
            (UnaryOperator::Minus, Type::F32) => true,
            (UnaryOperator::Minus, Type::F64) => true,
            (UnaryOperator::Minus, Type::I32) => true,
            (UnaryOperator::Minus, Type::I64) => true,
            (UnaryOperator::Minus, _) => false,
            (UnaryOperator::Bang, Type::Bool) => true,
            (UnaryOperator::Bang, _) => false,
        }
    }
}

impl fmt::Display for BinaryOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BinaryOperator::Plus => write!(f, "BinaryPlus"),
            BinaryOperator::Minus => write!(f, "BinaryMinus"),
            BinaryOperator::Star => write!(f, "Start"),
            BinaryOperator::Slash => write!(f, "Slash"),
            BinaryOperator::Modulo => write!(f, "Modulo"),
            BinaryOperator::Equal => write!(f, "Equal"),
            BinaryOperator::NotEqual => write!(f, "NotEqual"),
            BinaryOperator::Greater => write!(f, "Greater"),
            BinaryOperator::GreaterEqual => write!(f, "GreaterEqual"),
            BinaryOperator::Less => write!(f, "Less"),
            BinaryOperator::LessEqual => write!(f, "LessEqual"),
        }
    }
}

impl fmt::Display for LogicalOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogicalOperator::And => write!(f, "And"),
            LogicalOperator::Or => write!(f, "Or"),
        }
    }
}

impl fmt::Display for UnaryOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnaryOperator::Plus => write!(f, "UnaryPlus"),
            UnaryOperator::Minus => write!(f, "UnaryMinus"),
            UnaryOperator::Bang => write!(f, "Bang"),
        }
    }
}

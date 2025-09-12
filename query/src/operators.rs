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

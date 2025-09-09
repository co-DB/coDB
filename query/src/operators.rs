#[derive(Debug)]
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

#[derive(Debug)]
pub(crate) enum LogicalOperator {
    And,
    Or,
}

#[derive(Debug)]
pub enum UnaryOperator {
    Plus,
    Minus,
    Bang,
}

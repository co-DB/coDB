use crate::parser::Precedence;
use std::fmt;

#[derive(Debug, PartialEq, Clone)]
pub enum TokenType {
    Ident(String),
    Int(i64),
    Float(f64),
    String(String),
    LParen,
    RParen,
    Illegal(String),
    EOF,
    Comma,
    NotEqual,
    Equal,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
    Semicolon,
    Bang,
    Plus,
    Minus,
    Divide,
    Mod,
    True,
    False,
    Select,
    Insert,
    Delete,
    Update,
    Where,
    From,
    Into,
    Set,
    Star, // This is used for both select statements and multiplication - needs to be handled correctly in parser
    Values,
    And,
    Or,
}

impl TokenType {
    /// Returns the operator precedence for this token type.
    ///
    /// Used by the parser to determine the order in which expressions
    /// should be evaluated. Higher precedence values bind tighter in
    /// expressions (e.g., multiplication before addition).
    /// Non-operator tokens default to the lowest precedence.
    pub(super) fn precedence(&self) -> Precedence {
        match self {
            TokenType::Or => Precedence::LogicalOr,
            TokenType::And => Precedence::LogicalAnd,
            TokenType::Equal | TokenType::NotEqual => Precedence::Equality,

            TokenType::Less
            | TokenType::LessEqual
            | TokenType::Greater
            | TokenType::GreaterEqual => Precedence::Comparison,

            TokenType::Plus | TokenType::Minus => Precedence::Additive,

            TokenType::Star | TokenType::Divide | TokenType::Mod => Precedence::Multiplicative,

            TokenType::LParen => Precedence::Primary,

            // Everything else defaults to the lowest precedence
            _ => Precedence::Lowest,
        }
    }
}
impl fmt::Display for TokenType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TokenType::Ident(s) => write!(f, "identifier `{s}`"),
            TokenType::Int(i) => write!(f, "integer `{i}`"),
            TokenType::Float(fl) => write!(f, "float `{fl}`"),
            TokenType::String(s) => write!(f, "string \"{s}\""),
            TokenType::Illegal(e) => write!(f, "illegal token `{e}`"),
            TokenType::LParen => write!(f, "("),
            TokenType::RParen => write!(f, ")"),
            TokenType::EOF => write!(f, "end of file"),
            TokenType::Comma => write!(f, ","),
            TokenType::NotEqual => write!(f, "!="),
            TokenType::Equal => write!(f, "="),
            TokenType::Greater => write!(f, ">"),
            TokenType::GreaterEqual => write!(f, ">="),
            TokenType::Less => write!(f, "<"),
            TokenType::LessEqual => write!(f, "<="),
            TokenType::Semicolon => write!(f, ";"),
            TokenType::Bang => write!(f, "!"),
            TokenType::Plus => write!(f, "+"),
            TokenType::Minus => write!(f, "-"),
            TokenType::Divide => write!(f, "/"),
            TokenType::Mod => write!(f, "%"),
            TokenType::True => write!(f, "TRUE"),
            TokenType::False => write!(f, "FALSE"),
            TokenType::Select => write!(f, "SELECT"),
            TokenType::Insert => write!(f, "INSERT"),
            TokenType::Delete => write!(f, "DELETE"),
            TokenType::Update => write!(f, "UPDATE"),
            TokenType::Where => write!(f, "WHERE"),
            TokenType::From => write!(f, "FROM"),
            TokenType::Into => write!(f, "INTO"),
            TokenType::Set => write!(f, "SET"),
            TokenType::Star => write!(f, "*"),
            TokenType::Values => write!(f, "VALUES"),
            TokenType::And => write!(f, "AND"),
            TokenType::Or => write!(f, "OR"),
        }
    }
}

#[derive(Clone)]
pub struct Token {
    pub token_type: TokenType,
    pub column: usize,
    pub line: usize,
}

impl Token {
    pub fn new(token_type: TokenType, column: usize, line: usize) -> Token {
        Token {
            token_type,
            column,
            line,
        }
    }
}

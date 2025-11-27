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
    Create,
    Alter,
    Truncate,
    Drop,
    Where,
    From,
    Into,
    Set,
    Star, // This is used for both select statements and multiplication - needs to be handled correctly in parser
    Values,
    And,
    Or,
    PrimaryKey,
    Table,
    Column,
    Add,
    Rename,
    To,
    Int32Type,
    Int64Type,
    Float32Type,
    Float64Type,
    BoolType,
    StringType,
    DateType,
    DateTimeType,
    As,
    Dot,
    Order,
    By,
    Limit,
    Asc,
    Desc,
    Offset,
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
            TokenType::Create => write!(f, "CREATE"),
            TokenType::Alter => write!(f, "ALTER"),
            TokenType::Truncate => write!(f, "TRUNCATE"),
            TokenType::Drop => write!(f, "DROP"),
            TokenType::PrimaryKey => write!(f, "PRIMARY_KEY"),
            TokenType::Table => write!(f, "TABLE"),
            TokenType::Column => write!(f, "COLUMN"),
            TokenType::Add => write!(f, "ADD"),
            TokenType::Rename => write!(f, "RENAME"),
            TokenType::To => write!(f, "TO"),
            TokenType::Int32Type => write!(f, "INT32"),
            TokenType::Int64Type => write!(f, "INT64"),
            TokenType::Float32Type => write!(f, "FLOAT32"),
            TokenType::Float64Type => write!(f, "FLOAT64"),
            TokenType::BoolType => write!(f, "BOOL"),
            TokenType::StringType => write!(f, "STRING"),
            TokenType::DateType => write!(f, "DATE"),
            TokenType::DateTimeType => write!(f, "DATETIME"),
            TokenType::As => write!(f, "AS"),
            TokenType::Dot => write!(f, "."),
            TokenType::Order => write!(f, "Order"),
            TokenType::By => write!(f, "By"),
            TokenType::Limit => write!(f, "Limit"),
            TokenType::Asc => write!(f, "Asc"),
            TokenType::Desc => write!(f, "Desc"),
            TokenType::Offset => write!(f, "Offset"),
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

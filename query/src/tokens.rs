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
    And,
    Or
}

pub struct Token {
    pub token_type: TokenType,
    pub column: usize,
    pub line: usize,
}

impl Token {
    pub fn new(token_type: TokenType, column: usize, line: usize) -> Token {
        Token{
            token_type,
            column,
            line
        }
    }
}
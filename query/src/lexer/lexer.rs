use super::tokens::Token;

pub struct Lexer {
    input: Vec<char>,
    pos: usize,
    read_pos: usize,
    ch: char,
    line: usize,
}

impl Lexer {
    pub fn new(input: String) -> Lexer {
        let mut lexer = Lexer {
            input: input.chars().collect(),
            pos: 0,
            read_pos: 0,
            ch: '\0',
            line: 1,
        };
        lexer.read_char();
        lexer
    }

    fn read_char(&mut self) {
        if self.input.len() <= self.read_pos {
            self.ch = '\0';
        } else {
            self.ch = self.input[self.read_pos];
            if self.ch == '\n' {
                self.line += 1;
            }
        }
        self.pos = self.read_pos;
        self.read_pos += 1;
    }

    fn skip_whitespace(&mut self) {
        while self.ch.is_whitespace() {
            self.read_char();
        }
    }
    fn match_multichar_keywords(ident: String) -> Token {
        match ident.to_lowercase().as_str() {
            "true" => Token::True,
            "false" => Token::False,
            "select" => Token::Select,
            "insert" => Token::Insert,
            "delete" => Token::Delete,
            "update" => Token::Update,
            "where" => Token::Where,
            "from" => Token::From,
            "set" => Token::Set,
            "and" => Token::And,
            "or" => Token::Or,
            "into" => Token::Into,
            _ => Token::Ident(ident),
        }
    }

    fn read_identifier(&mut self) -> String {
        let pos = self.pos;
        while self.ch.is_alphanumeric() || self.ch == '_' {
            self.read_char();
        }
        self.input[pos..self.pos].iter().collect()
    }

    fn read_numeric(&mut self) -> Token {
        let pos = self.pos;
        while self.ch.is_numeric() {
            self.read_char();
        }
        if self.ch == '.' {
            self.read_char();
            while self.ch.is_numeric() {
                self.read_char();
            }
            let float: String = self.input[pos..self.pos].iter().collect();
            match float.parse::<f64>() {
                Ok(float) => Token::Float(float),
                Err(_) => Token::Illegal(self.line, String::from(format!("Invalid float literal: '{}'", float))),
            }
        } else {
            let integer: String = self.input[pos..self.pos].iter().collect();
            match integer.parse::<i64>() {
                Ok(integer) => Token::Int(integer),
                Err(_) => Token::Illegal(self.line, format!("Invalid integer literal: '{}'", integer)),
            }
        }
    }

    fn read_string(&mut self) -> Token {
        let mut result = String::new();
        let mut escaped = false;
        self.read_char();
        while self.ch != '\0' {
            if self.ch == '\'' && !escaped {
                break;
            }

            if escaped {
                let escaped_char = match self.ch {
                    '\\' => '\\',
                    '\'' => '\'',
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    _ => return Token::Illegal(self.line, format!("Invalid escape sequence: '{}'", self.ch)),
                };
                result.push(escaped_char);
                escaped = false;
            } else if self.ch == '\\' {
                escaped = true;
            } else {
                result.push(self.ch);
            }
            self.read_char();
        }

        if self.ch != '\'' {
            return Token::Illegal(self.line, String::from("Missing closing quote"));
        }
        self.read_char();

        Token::String(result)
    }
    fn peek_char(&self) -> char {
        if self.read_pos >= self.input.len() {
            return '\0';
        }
        self.input[self.read_pos]
    }

    pub fn next_token(&mut self) -> Token {
        self.skip_whitespace();
        let token = match self.ch {
            '!' => {
                match self.peek_char() {
                    '=' => {
                        self.read_char();
                        Token::NotEqual
                    }
                    _ => Token::Illegal(self.line, String::from("Invalid character literal")),
                }
            }
            '>' => {
                match self.peek_char() {
                    '=' => {
                        self.read_char();
                        Token::GreaterEqual
                    }
                    _ => Token::Greater,
                }
            }
            '<' => {
                match self.peek_char() {
                    '=' => {
                        self.read_char();
                        Token::LessEqual
                    }
                    _ => Token::Less,
                }
            }
            '+' => Token::Plus,
            '-' => Token::Minus,
            '/' => Token::Divide,
            '(' => Token::LParen,
            ')' => Token::RParen,
            '%' => Token::Mod,
            '*' => Token::Star,
            ',' => Token::Comma,
            ';' => Token::Semicolon,
            '\0' => Token::EOF,
            '=' => Token::Equal,
            // In the remaining cases we return from the function instead of just from the match
            // statement, because those helper functions already read till the next valid char, so
            // the self.read_char() at the end isn't needed.
            '\'' => return self.read_string(),
            _ => {
                if self.ch.is_alphabetic() {
                    let ident = self.read_identifier();
                    return Self::match_multichar_keywords(ident);
                } else if self.ch.is_numeric() {
                    return self.read_numeric()
                } else {
                    Token::Illegal(self.line, String::from(format!("Unrecognized character: '{}'", self.ch)))
                }
            }
        };
        self.read_char();
        token
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::tokens::Token;

    fn assert_works(lexer: &mut Lexer,expected_tokens: &Vec<Token>){
        for token in expected_tokens {
            let actual_token = lexer.next_token();
            assert_eq!(token, &actual_token,
                       "Token {:?} doesn't match actual token {:?}",
                       token, actual_token);
        }
    }
    #[test]
    fn test_single_char_tokens() {
        let input = "()+-*,;";

        let mut lexer = Lexer::new(String::from(input));

        let expected_tokens = vec![
            Token::LParen,
            Token::RParen,
            Token::Plus,
            Token::Minus,
            Token::Star,
            Token::Comma,
            Token::Semicolon,
            Token::EOF,
        ];

        assert_works(&mut lexer,&expected_tokens);
    }

    #[test]
    fn test_empty() {
        let input = "";
        let mut lexer = Lexer::new(String::from(input));
        let expected_tokens = vec![Token::EOF];

        assert_works(&mut lexer,&expected_tokens);
    }

    #[test]
    fn test_keyword_tokens() {
        let input = "Select foobar from users;";

        let mut lexer = Lexer::new(String::from(input));

        let expected_tokens = vec![
            Token::Select,
            Token::Ident(String::from("foobar")),
            Token::From,
            Token::Ident(String::from("users")),
            Token::Semicolon,
            Token::EOF,
        ];

        assert_works(&mut lexer,&expected_tokens);
    }

    #[test]
    fn test_multichar_tokens() {
        let input = "Select name from users where age >= 18 and score != 5.052;";

        let mut lexer = Lexer::new(String::from(input));

        let expected_tokens = vec![
            Token::Select,
            Token::Ident(String::from("name")),
            Token::From,
            Token::Ident(String::from("users")),
            Token::Where,
            Token::Ident(String::from("age")),
            Token::GreaterEqual,
            Token::Int(18),
            Token::And,
            Token::Ident(String::from("score")),
            Token::NotEqual,
            Token::Float(5.052),
            Token::Semicolon,
            Token::EOF,
        ];

        assert_works(&mut lexer,&expected_tokens);
    }

    #[test]
    fn test_string_tokens() {
        let input = "Insert into users set name = 'John Doe'; update users set bio = 'Line 1\\nLine 2' where id = 1;";

        let mut lexer = Lexer::new(String::from(input));

        let expected_tokens = vec![
            Token::Insert,
            Token::Into,
            Token::Ident(String::from("users")),
            Token::Set,
            Token::Ident(String::from("name")),
            Token::Equal,
            Token::String(String::from("John Doe")),
            Token::Semicolon,
            Token::Update,
            Token::Ident(String::from("users")),
            Token::Set,
            Token::Ident(String::from("bio")),
            Token::Equal,
            Token::String(String::from("Line 1\nLine 2")),
            Token::Where,
            Token::Ident(String::from("id")),
            Token::Equal,
            Token::Int(1),
            Token::Semicolon,
            Token::EOF,
        ];

        assert_works(&mut lexer,&expected_tokens);
    }

    #[test]
    fn test_sql_query() {
        let input = "Select * from users where name = 'Alice' and age > 25 or status = true;";

        let mut lexer = Lexer::new(String::from(input));

        let expected_tokens = vec![
            Token::Select,
            Token::Star,
            Token::From,
            Token::Ident(String::from("users")),
            Token::Where,
            Token::Ident(String::from("name")),
            Token::Equal,
            Token::String(String::from("Alice")),
            Token::And,
            Token::Ident(String::from("age")),
            Token::Greater,
            Token::Int(25),
            Token::Or,
            Token::Ident(String::from("status")),
            Token::Equal,
            Token::True,
            Token::Semicolon,
            Token::EOF,
        ];

        assert_works(&mut lexer,&expected_tokens);
    }
}
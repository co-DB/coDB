use super::tokens::{Token, TokenType};

/// Responsible for transforming an input CoSQL string into a stream of [`Token`]s.
///
/// The [`Lexer`] processes the input character-by-character, keeping track of current position,
/// line, and column information for more informative error reporting. This component acts as the
/// first stage in an interpreter pipeline.
pub struct Lexer {
    /// Input characters collected into a vector to simplify access logic.
    input: Vec<char>,
    /// Current character index.
    pos: usize,
    /// Next character index to be read (always pos + 1 in practice).
    /// This could be calculated as pos + 1, but having it separate makes the ['Lexer']s
    /// reading logic more explicit.
    read_pos: usize,
    /// Current character being processed.
    ch: char,
    /// Current column number (used for error handling).
    column: usize,
    /// Current line number (used for error handling).
    line: usize,
}

impl Lexer {
    /// Creates a new [`Lexer`] from the given input string.
    ///
    /// The input is converted to a vector of characters for easy indexing.
    /// The internal cursor is initialized and prepared for tokenization.
    pub fn new(input: &str) -> Lexer {
        let mut lexer = Lexer {
            input: input.chars().collect(),
            pos: 0,
            read_pos: 0,
            column: 0,
            ch: '\0',
            line: 1,
        };
        lexer.read_char();
        lexer
    }

    /// Creates tokens from the token type that automatically includes column and line from
    /// ['Lexer'] internal state.
    fn create_token(&self, token_type: TokenType) -> Token {
        Token::new(token_type, self.column, self.line)
    }

    /// Reads the next char and updates ['Lexer'] internal state, while accounting for EOFs and
    /// new lines.
    fn read_char(&mut self) {
        if self.is_at_end() {
            self.ch = '\0';
            self.pos = self.read_pos;
            return;
        }
        self.ch = self.input[self.read_pos];
        if self.ch == '\n' {
            self.line += 1;
            self.column = 0;
        }
        self.pos = self.read_pos;
        self.read_pos += 1;
        self.column += 1;
    }

    /// Checks if current position is at the end of input.
    pub fn is_at_end(&self) -> bool {
        self.read_pos >= self.input.len()
    }

    /// Advances the current character position until a non-whitespace character is found.
    ///
    /// This method reads and discards all consecutive whitespace characters
    /// (e.g., spaces, tabs, newlines) from the current position in the input.
    fn skip_whitespace(&mut self) {
        while self.ch.is_whitespace() {
            self.read_char();
        }
    }

    /// Matches a multi-character identifier against known keywords.
    ///
    /// If it matches a keyword returns the corresponding token, else returns identifier token
    fn match_multichar_keywords(&self, ident: String) -> Token {
        match ident.to_lowercase().as_str() {
            "true" => self.create_token(TokenType::True),
            "false" => self.create_token(TokenType::False),
            "select" => self.create_token(TokenType::Select),
            "insert" => self.create_token(TokenType::Insert),
            "delete" => self.create_token(TokenType::Delete),
            "update" => self.create_token(TokenType::Update),
            "where" => self.create_token(TokenType::Where),
            "from" => self.create_token(TokenType::From),
            "set" => self.create_token(TokenType::Set),
            "values" => self.create_token(TokenType::Values),
            "and" => self.create_token(TokenType::And),
            "or" => self.create_token(TokenType::Or),
            "into" => self.create_token(TokenType::Into),
            "create" => self.create_token(TokenType::Create),
            "alter" => self.create_token(TokenType::Alter),
            "truncate" => self.create_token(TokenType::Truncate),
            "drop" => self.create_token(TokenType::Drop),
            "primary_key" => self.create_token(TokenType::PrimaryKey),
            "table" => self.create_token(TokenType::Table),
            "column" => self.create_token(TokenType::Column),
            "add" => self.create_token(TokenType::Add),
            "rename" => self.create_token(TokenType::Rename),
            "to" => self.create_token(TokenType::To),
            "int32" => self.create_token(TokenType::Int32Type),
            "int64" => self.create_token(TokenType::Int64Type),
            "float32" => self.create_token(TokenType::Float32Type),
            "float64" => self.create_token(TokenType::Float64Type),
            "bool" => self.create_token(TokenType::BoolType),
            "string" => self.create_token(TokenType::StringType),
            "date" => self.create_token(TokenType::DateType),
            "datetime" => self.create_token(TokenType::DateTimeType),
            "as" => self.create_token(TokenType::As),
            _ => self.create_token(TokenType::Ident(ident)),
        }
    }

    /// Reads an identifier from current position and returns a string containing it.
    fn read_identifier(&mut self) -> String {
        let pos = self.pos;
        while self.ch.is_ascii_alphanumeric() || self.ch == '_' {
            self.read_char();
        }
        self.input[pos..self.pos].iter().collect()
    }

    /// Reads a numeric literal (integer or floating-point) from the current position in the input.
    ///
    /// Depending on whether the literal contains a decimal point or not the method returns a float
    /// or integer. If parsing those fails it returns an illegal token.
    fn read_numeric(&mut self) -> Token {
        let pos = self.pos;
        while self.ch.is_ascii_digit() {
            self.read_char();
        }
        if self.ch == '.' {
            self.read_char();
            while self.ch.is_ascii_digit() {
                self.read_char();
            }
            let float: String = self.input[pos..self.pos].iter().collect();
            match float.parse::<f64>() {
                Ok(float) => self.create_token(TokenType::Float(float)),
                Err(_) => self.create_token(TokenType::Illegal(format!(
                    "invalid float literal: '{float}'"
                ))),
            }
        } else {
            let integer: String = self.input[pos..self.pos].iter().collect();
            match integer.parse::<i64>() {
                Ok(integer) => self.create_token(TokenType::Int(integer)),
                Err(_) => self.create_token(TokenType::Illegal(format!(
                    "Invalid integer literal: '{integer}'"
                ))),
            }
        }
    }

    /// Reads a string literal enclosed in single quotes from the input.
    ///
    /// Supports escape sequences for backslash (`\\`), single quote (`\'`),
    /// newline (`\n`), carriage return (`\r`), and tab (`\t`).
    ///
    /// The function consumes characters until it finds an unescaped closing single quote.
    /// If the closing quote is missing or an invalid escape sequence is encountered,
    /// it returns an illegal token with an error message.
    fn read_string(&mut self) -> Token {
        let mut result = String::new();
        let mut escaped = false;
        self.read_char();
        while self.ch != '\0' {
            // Closing quote found
            if self.ch == '\'' && !escaped {
                break;
            }

            if escaped {
                match self.ch {
                    '\\' => result.push('\\'),
                    '\'' => result.push('\''),
                    'n' => result.push('\n'),
                    'r' => result.push('\r'),
                    't' => result.push('\t'),
                    ch => {
                        // Unknown escape: treat it as a normal part of the string
                        result.push('\\');
                        result.push(ch);
                    }
                }
                escaped = false;
            } else if self.ch == '\\' {
                escaped = true;
            } else {
                result.push(self.ch);
            }
            self.read_char();
        }

        if self.ch != '\'' {
            return self.create_token(TokenType::Illegal(String::from("missing closing quote")));
        }
        self.read_char();

        self.create_token(TokenType::String(result))
    }

    /// Returns the next token to be read. Useful for checking if the current character is a part of
    /// a multi-character token.
    fn peek_char(&self) -> char {
        if self.is_at_end() {
            return '\0';
        }
        self.input[self.read_pos]
    }

    /// Returns the next token from the input stream.
    ///
    /// This method skips any leading whitespaces, then matches the current character
    /// to produce the corresponding token.
    ///
    /// Some tokens consume multiple characters internally (e.g., `read_string`, `read_identifier`, `read_numeric`)
    /// and return immediately without advancing the character again.
    pub fn next_token(&mut self) -> Token {
        self.skip_whitespace();
        let token = match self.ch {
            '!' => match self.peek_char() {
                '=' => {
                    self.read_char();
                    self.create_token(TokenType::NotEqual)
                }
                _ => self.create_token(TokenType::Bang),
            },
            '>' => match self.peek_char() {
                '=' => {
                    self.read_char();
                    self.create_token(TokenType::GreaterEqual)
                }
                _ => self.create_token(TokenType::Greater),
            },
            '<' => match self.peek_char() {
                '=' => {
                    self.read_char();
                    self.create_token(TokenType::LessEqual)
                }
                _ => self.create_token(TokenType::Less),
            },
            '.' => match self.peek_char() {
                peak_ch if peak_ch.is_ascii_digit() => self.read_numeric(),
                _ => self.create_token(TokenType::Dot),
            },
            '+' => self.create_token(TokenType::Plus),
            '-' => self.create_token(TokenType::Minus),
            '/' => self.create_token(TokenType::Divide),
            '(' => self.create_token(TokenType::LParen),
            ')' => self.create_token(TokenType::RParen),
            '%' => self.create_token(TokenType::Mod),
            '*' => self.create_token(TokenType::Star),
            ',' => self.create_token(TokenType::Comma),
            ';' => self.create_token(TokenType::Semicolon),
            '\0' => self.create_token(TokenType::EOF),
            '=' => self.create_token(TokenType::Equal),
            // In the remaining cases we return from the function instead of just from the match
            // statement, because those helper functions already read till the next valid char, so
            // the self.read_char() at the end isn't needed.
            '\'' => return self.read_string(),
            _ => {
                if self.ch.is_alphabetic() {
                    let ident = self.read_identifier();
                    return self.match_multichar_keywords(ident);
                } else if self.ch.is_ascii_digit() {
                    return self.read_numeric();
                } else {
                    self.create_token(TokenType::Illegal(format!(
                        "unrecognized character: '{}'",
                        self.ch
                    )))
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
    use crate::lexer::TokenType;

    fn assert_works(lexer: &mut Lexer, expected_tokens: &[TokenType]) {
        for token in expected_tokens {
            let actual_token = lexer.next_token();
            assert_eq!(
                token, &actual_token.token_type,
                "Token {token:?} doesn't match actual token {:?}",
                &actual_token.token_type
            );
        }
        assert!(
            lexer.is_at_end(),
            "Not all tokens were consumed from lexer."
        );
    }

    #[test]
    fn test_single_char_tokens() {
        let input = "()+-*,;";

        let mut lexer = Lexer::new(input);

        let expected_tokens = [
            TokenType::LParen,
            TokenType::RParen,
            TokenType::Plus,
            TokenType::Minus,
            TokenType::Star,
            TokenType::Comma,
            TokenType::Semicolon,
            TokenType::EOF,
        ];

        assert_works(&mut lexer, &expected_tokens);
    }

    #[test]
    fn test_upper_lower_case() {
        let input = "select Select SELECT sElEcT insert INSERT WHERE wHerE";

        let mut lexer = Lexer::new(input);

        let expected_tokens = [
            TokenType::Select,
            TokenType::Select,
            TokenType::Select,
            TokenType::Select,
            TokenType::Insert,
            TokenType::Insert,
            TokenType::Where,
            TokenType::Where,
        ];
        assert_works(&mut lexer, &expected_tokens);
    }
    #[test]
    fn test_empty() {
        let input = "";
        let mut lexer = Lexer::new(input);
        let expected_tokens = [TokenType::EOF];

        assert_works(&mut lexer, &expected_tokens);
    }

    #[test]
    fn test_keyword_tokens() {
        let input = "Select foobar from users;";

        let mut lexer = Lexer::new(input);

        let expected_tokens = [
            TokenType::Select,
            TokenType::Ident(String::from("foobar")),
            TokenType::From,
            TokenType::Ident(String::from("users")),
            TokenType::Semicolon,
            TokenType::EOF,
        ];

        assert_works(&mut lexer, &expected_tokens);
    }

    #[test]
    fn test_multichar_tokens() {
        let input = "Select name from users where age >= 18 and score != 5.052;";

        let mut lexer = Lexer::new(input);

        let expected_tokens = [
            TokenType::Select,
            TokenType::Ident(String::from("name")),
            TokenType::From,
            TokenType::Ident(String::from("users")),
            TokenType::Where,
            TokenType::Ident(String::from("age")),
            TokenType::GreaterEqual,
            TokenType::Int(18),
            TokenType::And,
            TokenType::Ident(String::from("score")),
            TokenType::NotEqual,
            TokenType::Float(5.052),
            TokenType::Semicolon,
            TokenType::EOF,
        ];

        assert_works(&mut lexer, &expected_tokens);
    }

    #[test]
    fn test_string_tokens() {
        let input = "Insert into users set name = 'John Doe'; update users set bio = 'Line 1\\nLine 2' where id = 1;";

        let mut lexer = Lexer::new(input);

        let expected_tokens = [
            TokenType::Insert,
            TokenType::Into,
            TokenType::Ident(String::from("users")),
            TokenType::Set,
            TokenType::Ident(String::from("name")),
            TokenType::Equal,
            TokenType::String(String::from("John Doe")),
            TokenType::Semicolon,
            TokenType::Update,
            TokenType::Ident(String::from("users")),
            TokenType::Set,
            TokenType::Ident(String::from("bio")),
            TokenType::Equal,
            TokenType::String(String::from("Line 1\nLine 2")),
            TokenType::Where,
            TokenType::Ident(String::from("id")),
            TokenType::Equal,
            TokenType::Int(1),
            TokenType::Semicolon,
            TokenType::EOF,
        ];

        assert_works(&mut lexer, &expected_tokens);
    }

    #[test]
    fn test_sql_query() {
        let input = "Select * from users where name = 'Alice' and age > 25 or status = true;";

        let mut lexer = Lexer::new(input);

        let expected_tokens = [
            TokenType::Select,
            TokenType::Star,
            TokenType::From,
            TokenType::Ident(String::from("users")),
            TokenType::Where,
            TokenType::Ident(String::from("name")),
            TokenType::Equal,
            TokenType::String(String::from("Alice")),
            TokenType::And,
            TokenType::Ident(String::from("age")),
            TokenType::Greater,
            TokenType::Int(25),
            TokenType::Or,
            TokenType::Ident(String::from("status")),
            TokenType::Equal,
            TokenType::True,
            TokenType::Semicolon,
            TokenType::EOF,
        ];

        assert_works(&mut lexer, &expected_tokens);
    }

    #[test]
    fn test_illegal_characters() {
        let input = "select @ from # users $ where;";

        let mut lexer = Lexer::new(input);

        let expected_tokens = [
            TokenType::Select,
            TokenType::Illegal(String::from("unrecognized character: '@'")),
            TokenType::From,
            TokenType::Illegal(String::from("unrecognized character: '#'")),
            TokenType::Ident(String::from("users")),
            TokenType::Illegal(String::from("unrecognized character: '$'")),
            TokenType::Where,
            TokenType::Semicolon,
            TokenType::EOF,
        ];

        assert_works(&mut lexer, &expected_tokens);
    }

    #[test]
    fn test_unterminated_string() {
        let input = "select name from users where bio = 'This string has no end";

        let mut lexer = Lexer::new(input);

        let expected_tokens = [
            TokenType::Select,
            TokenType::Ident(String::from("name")),
            TokenType::From,
            TokenType::Ident(String::from("users")),
            TokenType::Where,
            TokenType::Ident(String::from("bio")),
            TokenType::Equal,
            TokenType::Illegal(String::from("missing closing quote")),
            TokenType::EOF,
        ];

        assert_works(&mut lexer, &expected_tokens);
    }

    #[test]
    fn test_valid_escape_sequences() {
        let input = "select 'Tab:\\t' from users where desc = 'Quote:\\' and slash:\\\\ \\x';";

        let mut lexer = Lexer::new(input);

        let expected_tokens = [
            TokenType::Select,
            TokenType::String(String::from("Tab:\t")),
            TokenType::From,
            TokenType::Ident(String::from("users")),
            TokenType::Where,
            TokenType::Ident(String::from("desc")),
            TokenType::Equal,
            TokenType::String(String::from("Quote:' and slash:\\ \\x")),
            TokenType::Semicolon,
            TokenType::EOF,
        ];

        assert_works(&mut lexer, &expected_tokens);
    }

    #[test]
    fn test_numbers_edge_cases() {
        let input = "select 123 456.789 999. .123 . from users;";

        let mut lexer = Lexer::new(input);

        let expected_tokens = [
            TokenType::Select,
            TokenType::Int(123),
            TokenType::Float(456.789),
            TokenType::Float(999.),
            TokenType::Float(0.123),
            TokenType::Dot,
            TokenType::From,
            TokenType::Ident(String::from("users")),
            TokenType::Semicolon,
            TokenType::EOF,
        ];

        assert_works(&mut lexer, &expected_tokens);
    }

    #[test]
    fn test_create_table_query() {
        let input = "create table example column1 int32 primary_key column2 string;";

        let expected_tokens = [
            TokenType::Create,
            TokenType::Table,
            TokenType::Ident("example".into()),
            TokenType::Ident("column1".into()),
            TokenType::Int32Type,
            TokenType::PrimaryKey,
            TokenType::Ident("column2".into()),
            TokenType::StringType,
            TokenType::Semicolon,
            TokenType::EOF,
        ];

        let mut lexer = Lexer::new(input);

        assert_works(&mut lexer, &expected_tokens);
    }

    #[test]
    fn test_alter_add_query() {
        let input = "alter table example ADD column3 date;";

        let expected_tokens = [
            TokenType::Alter,
            TokenType::Table,
            TokenType::Ident("example".into()),
            TokenType::Add,
            TokenType::Ident("column3".into()),
            TokenType::DateType,
            TokenType::Semicolon,
            TokenType::EOF,
        ];

        let mut lexer = Lexer::new(input);

        assert_works(&mut lexer, &expected_tokens);
    }

    #[test]
    fn test_alter_rename_query() {
        let input = "alter table rename column column_prev to column_new;";

        let expected_tokens = [
            TokenType::Alter,
            TokenType::Table,
            TokenType::Rename,
            TokenType::Column,
            TokenType::Ident("column_prev".into()),
            TokenType::To,
            TokenType::Ident("column_new".into()),
            TokenType::Semicolon,
            TokenType::EOF,
        ];

        let mut lexer = Lexer::new(input);

        assert_works(&mut lexer, &expected_tokens);
    }

    #[test]
    fn test_alter_drop_query() {
        let input = "alter table drop column column4;";

        let expected_tokens = [
            TokenType::Alter,
            TokenType::Table,
            TokenType::Drop,
            TokenType::Column,
            TokenType::Ident("column4".into()),
            TokenType::Semicolon,
            TokenType::EOF,
        ];

        let mut lexer = Lexer::new(input);

        assert_works(&mut lexer, &expected_tokens);
    }

    #[test]
    fn test_truncate_query() {
        let input = "truncate table example;";

        let expected_tokens = [
            TokenType::Truncate,
            TokenType::Table,
            TokenType::Ident("example".into()),
            TokenType::Semicolon,
            TokenType::EOF,
        ];

        let mut lexer = Lexer::new(input);

        assert_works(&mut lexer, &expected_tokens);
    }

    #[test]
    fn test_select_with_alias() {
        let input = "SELECT u.id, u.age FROM users AS u WHERE u.id = 200";

        let expected_tokens = [
            TokenType::Select,
            TokenType::Ident("u".into()),
            TokenType::Dot,
            TokenType::Ident("id".into()),
            TokenType::Comma,
            TokenType::Ident("u".into()),
            TokenType::Dot,
            TokenType::Ident("age".into()),
            TokenType::From,
            TokenType::Ident("users".into()),
            TokenType::As,
            TokenType::Ident("u".into()),
            TokenType::Where,
            TokenType::Ident("u".into()),
            TokenType::Dot,
            TokenType::Ident("id".into()),
            TokenType::Equal,
            TokenType::Int(200),
        ];

        let mut lexer = Lexer::new(input);

        assert_works(&mut lexer, &expected_tokens);
    }
}

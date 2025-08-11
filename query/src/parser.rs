use crate::ast::{
    Ast, DeleteStatement, Expression, IdentifierNode, InsertStatement, Literal, LiteralNode,
    NodeId, SelectStatement, Statement, UpdateStatement,
};
use crate::lexer::Lexer;
use crate::tokens::{Token, TokenType};
use std::mem;
use thiserror::Error;

#[derive(PartialEq, PartialOrd)]
enum Precedence {
    Lowest = 0,
    LogicalOr,      // OR
    LogicalAnd,     // AND
    Equality,       // =, !=, <>
    Comparison,     // <, <=, >, >=
    Additive,       // +, -
    Multiplicative, // *, /, %
    Unary,          // unary -, NOT
    Primary,        // literals, identifiers, function calls, parentheses
}

#[derive(Error, Debug)]
pub enum ParserError {
    #[error("Unexpected token: expected {expected}, found {found} at line {line}, column {column}")]
    UnexpectedToken {
        expected: String,
        found: String,
        line: usize,
        column: usize,
    },

    #[error("Illegal token '{token}' at line {line}, column {column}")]
    IllegalToken {
        token: String,
        line: usize,
        column: usize,
    },
}

struct Parser {
    lexer: Lexer,
    ast: Ast,
    errors: Vec<ParserError>,
    curr_token: Token,
    peek_token: Token,
}

impl Parser {
    pub fn new(input: &str) -> Parser {
        let mut lexer = Lexer::new(input);
        let first_token = lexer.next_token();
        let second_token = lexer.next_token();
        Self {
            lexer,
            ast: Ast::new(),
            errors: Vec::new(),
            curr_token: first_token,
            peek_token: second_token,
        }
    }
    pub fn parse_program(mut self) -> Result<Ast, Vec<ParserError>> {
        while self.peek_token.token_type != TokenType::EOF {
            let stmt = self.parse_statement();
            match stmt {
                Err(err) => {
                    self.errors.push(err);
                    self.recover_to_semicolon();
                }
                Ok(stmt) => {
                    if self.peek_token.token_type == TokenType::Semicolon {
                        self.ast.add_statement(stmt);
                    } else {
                        self.errors.push(ParserError::UnexpectedToken {
                            expected: TokenType::Semicolon.to_string(),
                            found: self.peek_token.token_type.to_string(),
                            line: self.peek_token.line,
                            column: self.peek_token.column,
                        });
                        self.recover_to_semicolon();
                    }
                }
            }
            if self.peek_token.token_type == TokenType::Semicolon {
                let _ = self.read_token();
            };
        }
        if !self.errors.is_empty() {
            return Err(self.errors);
        }
        Ok(self.ast)
    }

    fn recover_to_semicolon(&mut self) {
        while self.peek_token.token_type != TokenType::Semicolon
            && self.peek_token.token_type != TokenType::EOF
        {
            if let Err(err) = self.read_token() {
                self.errors.push(err);
            }
        }
    }

    fn parse_expression(&mut self) -> Result<NodeId, ParserError> {
        Ok(self.ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::String("String".to_string()),
        })))
    }
    fn parse_statement(&mut self) -> Result<Statement, ParserError> {
        match self.curr_token.token_type {
            TokenType::Select => self.parse_select_statement(),
            TokenType::Insert => self.parse_insert_statement(),
            TokenType::Update => self.parse_update_statement(),
            TokenType::Delete => self.parse_delete_statement(),
            _ => Err(self.unexpected_token_error("one of INSERT, SELECT, UPDATE, DELETE")),
        }
    }

    fn parse_columns_common(&mut self) -> Result<Vec<NodeId>, ParserError> {
        let mut columns = Vec::new();
        let first_col = self.expect_string()?;
        columns.push(
            self.ast
                .add_node(Expression::Identifier(IdentifierNode { value: first_col })),
        );

        while self.peek_token.token_type == TokenType::Comma {
            self.read_token()?;
            let column = self.expect_string()?;
            columns.push(
                self.ast
                    .add_node(Expression::Identifier(IdentifierNode { value: column })),
            );
        }
        Ok(columns)
    }

    fn parse_delete_statement(&mut self) -> Result<Statement, ParserError> {
        self.expect_token(TokenType::From)?;
        let table_name_id = self.parse_table_name()?;
        let where_clause_id = self.parse_where_clause()?;
        Ok(Statement::Delete(DeleteStatement {
            table_name_id,
            where_clause_id,
        }))
    }

    fn parse_update_statement(&mut self) -> Result<Statement, ParserError> {
        let table_name_id = self.parse_table_name()?;
        self.expect_token(TokenType::Set)?;
        let column_setters = self.parse_column_setters()?;
        let where_clause_id = self.parse_where_clause()?;
        Ok(Statement::Update(UpdateStatement {
            table_name_id,
            column_setters,
            where_clause_id,
        }))
    }

    fn parse_column_setters(&mut self) -> Result<Vec<(NodeId, NodeId)>, ParserError> {
        let mut column_setters = Vec::new();
        column_setters.push(self.parse_column_setter()?);
        while self.peek_token.token_type == TokenType::Comma {
            self.read_token()?;
            column_setters.push(self.parse_column_setter()?);
        }
        Ok(column_setters)
    }

    fn parse_column_setter(&mut self) -> Result<(NodeId, NodeId), ParserError> {
        let column = self.expect_string()?;
        let column_id = self
            .ast
            .add_node(Expression::Identifier(IdentifierNode { value: column }));
        self.expect_token(TokenType::Equal)?;
        let value_id = self.parse_expression()?;
        Ok((column_id, value_id))
    }
    fn parse_insert_statement(&mut self) -> Result<Statement, ParserError> {
        self.expect_token(TokenType::Into)?;
        let table_name_id = self.parse_table_name()?;
        let column_ids = self.parse_insert_columns()?;
        self.expect_token(TokenType::Values)?;
        let value_ids = self.parse_insert_values()?;
        Ok(Statement::Insert(InsertStatement {
            table_name_id,
            column_ids,
            value_ids,
        }))
    }

    fn parse_insert_values(&mut self) -> Result<Vec<NodeId>, ParserError> {
        self.expect_token(TokenType::LParen)?;
        let mut values = Vec::new();
        values.push(self.parse_expression()?);
        while self.peek_token.token_type == TokenType::Comma {
            self.read_token()?;
            values.push(self.parse_expression()?);
        }
        self.expect_token(TokenType::RParen)?;
        Ok(values)
    }
    fn parse_insert_columns(&mut self) -> Result<Option<Vec<NodeId>>, ParserError> {
        if self.peek_token.token_type == TokenType::Values {
            return Ok(None);
        }

        self.expect_token(TokenType::LParen)?;

        let columns = self.parse_columns_common()?;

        self.expect_token(TokenType::RParen)?;
        Ok(Some(columns))
    }
    fn parse_select_statement(&mut self) -> Result<Statement, ParserError> {
        let column_ids = self.parse_select_columns()?;
        self.expect_token(TokenType::From)?;
        let table_name_id = self.parse_table_name()?;
        let where_clause_id = self.parse_where_clause()?;
        Ok(Statement::Select(SelectStatement {
            column_ids,
            table_name_id,
            where_clause_id,
        }))
    }

    fn parse_where_clause(&mut self) -> Result<Option<NodeId>, ParserError> {
        if self.peek_token.token_type != TokenType::Where {
            return Ok(None);
        }
        self.expect_token(TokenType::Where)?;
        Ok(Some(self.parse_expression()?))
    }
    fn parse_table_name(&mut self) -> Result<NodeId, ParserError> {
        let table_name = self.expect_string()?;
        let node_id = self
            .ast
            .add_node(Expression::Identifier(IdentifierNode { value: table_name }));
        Ok(node_id)
    }
    fn parse_select_columns(&mut self) -> Result<Option<Vec<NodeId>>, ParserError> {
        if self.peek_token.token_type == TokenType::Star {
            return Ok(None);
        }
        let columns = self.parse_columns_common()?;
        Ok(Some(columns))
    }
    fn read_token(&mut self) -> Result<(), ParserError> {
        self.curr_token = mem::replace(&mut self.peek_token, self.lexer.next_token());

        if let TokenType::Illegal(err) = &self.curr_token.token_type {
            return Err(ParserError::IllegalToken {
                token: err.clone(),
                line: self.curr_token.line,
                column: self.curr_token.column,
            });
        }

        Ok(())
    }

    fn expect_token(&mut self, expected_type: TokenType) -> Result<(), ParserError> {
        if self.peek_token.token_type == expected_type {
            self.read_token()?;
            Ok(())
        } else {
            Err(self.unexpected_token_error(&expected_type.to_string()))
        }
    }

    fn expect_string(&mut self) -> Result<String, ParserError> {
        if let TokenType::String(s) = &self.peek_token.token_type {
            let value = s.clone(); // clone first
            self.read_token()?; // then advance
            Ok(value)
        } else {
            Err(self.unexpected_token_error("string"))
        }
    }

    fn unexpected_token_error(&self, expected: &str) -> ParserError {
        ParserError::UnexpectedToken {
            expected: expected.to_string(),
            found: self.peek_token.token_type.to_string(),
            line: self.peek_token.line,
            column: self.peek_token.column,
        }
    }
}

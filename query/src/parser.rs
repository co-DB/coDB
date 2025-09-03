use super::ast::*;
use super::lexer::Lexer;
use super::tokens::{Token, TokenType};
use std::mem;
use thiserror::Error;

type PrefixFn = fn(&mut Parser) -> Result<NodeId, ParserError>;

type InfixFn = fn(&mut Parser, NodeId) -> Result<NodeId, ParserError>;

/// Operator precedence levels, ordered from lowest to highest.
/// Used in the Pratt parsing algorithm to decide whether to
/// continue parsing an expression or return control to the caller.
#[derive(PartialEq, PartialOrd)]
pub(crate) enum Precedence {
    Lowest = 0,
    LogicalOr,      // OR
    LogicalAnd,     // AND
    Equality,       // =, !=
    Comparison,     // <, <=, >, >=
    Additive,       // +, -
    Multiplicative, // *, /, %
    Unary,          // unary -, +, !
    Primary,        // literals, identifiers, function calls, parentheses
}

/// Error for [`Parser`] related operations.
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

    #[error("Unexpected token in expression: {found} at line {line}, column {column}")]
    NoParseFn {
        found: String,
        line: usize,
        column: usize,
    },
}

struct Parser {
    /// lexer to supply the tokens
    lexer: Lexer,
    /// The ast being constructed
    ast: Ast,
    /// list of errors, filled during parsing
    errors: Vec<ParserError>,
    /// the token currently being processed
    curr_token: Token,
    /// the next token to be processed
    peek_token: Token,
}

/// Responsible for transforming a stream of tokens from [`Lexer`] into an abstract syntax tree ([`Ast`]).
impl Parser {
    /// Creates a new parser from the given input string.
    /// It initializes the lexer and pre-reads two tokens so that
    /// `curr_token` and `peek_token` are always valid during parsing.
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

    /// Main entry point for parsing a full CoSQL program.
    ///
    /// - Repeatedly calls `parse_statement` until EOF is reached.
    /// - After each statement, ensures it ends with a semicolon.
    /// - On errors, records them and attempts to recover by skipping to the next semicolon.
    ///
    /// Returns:
    /// - `Ok(Ast)` if parsing completed without errors.
    /// - `Err(Vec<ParserError>)` if one or more errors were encountered.
    pub fn parse_program(mut self) -> Result<Ast, Vec<ParserError>> {
        loop {
            let stmt = self.parse_statement();
            match stmt {
                Err(err) => {
                    self.errors.push(err);
                    self.recover_to_semicolon();
                }
                // Since in our definition a statement ends on a semicolon, even correctly parsed
                // statements that are immediately followed by something other than semicolon will
                // be treated as errors.
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
                self.read_token().unwrap();
            }
            if self.peek_token.token_type == TokenType::EOF {
                break;
            }
            // We read a token here to move the next statement keyword into curr_token.
            if let Err(err) = self.read_token() {
                self.errors.push(err);
            }
        }
        if !self.errors.is_empty() {
            return Err(self.errors);
        }
        Ok(self.ast)
    }

    /// Skips tokens until a semicolon or EOF is reached. This allows the parser to continue parsing
    /// subsequent statements after an error instead of aborting entirely.
    fn recover_to_semicolon(&mut self) {
        while self.peek_token.token_type != TokenType::Semicolon
            && self.peek_token.token_type != TokenType::EOF
        {
            if let Err(err) = self.read_token() {
                self.errors.push(err);
            }
        }
    }

    /// Gets the correct prefix parsing function based on `token_type`.
    ///
    /// Can fail if there exists no prefix function for the given `token_type`
    fn prefix_function(&self, token_type: &TokenType) -> Result<PrefixFn, ParserError> {
        match token_type {
            TokenType::Minus => Ok(|p| p.parse_unary_op(UnaryOperator::Minus)),
            TokenType::Plus => Ok(|p| p.parse_unary_op(UnaryOperator::Plus)),
            TokenType::Bang => Ok(|p| p.parse_unary_op(UnaryOperator::Bang)),
            TokenType::Ident(_) => Ok(Self::parse_prefix_ident),
            TokenType::Int(_) => Ok(Self::parse_prefix_int),
            TokenType::Float(_) => Ok(Self::parse_prefix_float),
            TokenType::String(_) => Ok(Self::parse_prefix_string),
            TokenType::False | TokenType::True => Ok(Self::parse_prefix_bool),
            TokenType::LParen => Ok(Self::parse_grouped_expression),
            _ => Err(ParserError::NoParseFn {
                found: token_type.to_string(),
                column: self.curr_token.column,
                line: self.curr_token.line,
            }),
        }
    }

    /// Parses a unary expression: `-x`, `+x`, or `!x` with operator `unary_op`.
    fn parse_unary_op(&mut self, unary_op: UnaryOperator) -> Result<NodeId, ParserError> {
        let node_id = self.parse_expression(Precedence::Unary)?;
        let expression = Expression::Unary(UnaryExpressionNode {
            op: unary_op,
            expression_id: node_id,
        });
        Ok(self.ast.add_node(expression))
    }

    /// Parses an identifier as an expression (e.g., column or table names).
    fn parse_prefix_ident(&mut self) -> Result<NodeId, ParserError> {
        if let TokenType::Ident(s) = &self.curr_token.token_type {
            return Ok(self
                .ast
                .add_node(Expression::Identifier(IdentifierNode { value: s.clone() })));
        }
        Err(Self::unexpected_token_error(self, "identifier"))
    }

    /// Parses an integer literal as an expression.
    fn parse_prefix_int(&mut self) -> Result<NodeId, ParserError> {
        if let TokenType::Int(s) = &self.curr_token.token_type {
            return Ok(self.ast.add_node(Expression::Literal(LiteralNode {
                value: Literal::Int(*s),
            })));
        }
        Err(Self::unexpected_token_error(self, "integer"))
    }

    /// Parses a floating-point literal.
    fn parse_prefix_float(&mut self) -> Result<NodeId, ParserError> {
        if let TokenType::Float(float) = &self.curr_token.token_type {
            return Ok(self.ast.add_node(Expression::Literal(LiteralNode {
                value: Literal::Float(*float),
            })));
        }
        Err(Self::unexpected_token_error(self, "float"))
    }

    /// Parses a string literal.
    fn parse_prefix_string(&mut self) -> Result<NodeId, ParserError> {
        if let TokenType::String(s) = &self.curr_token.token_type {
            return Ok(self.ast.add_node(Expression::Literal(LiteralNode {
                value: Literal::String(s.clone()),
            })));
        }
        Err(Self::unexpected_token_error(self, "string"))
    }

    /// Parses a boolean literal (`true` or `false`).
    fn parse_prefix_bool(&mut self) -> Result<NodeId, ParserError> {
        if let TokenType::False = &self.curr_token.token_type {
            return Ok(self.ast.add_node(Expression::Literal(LiteralNode {
                value: Literal::Bool(false),
            })));
        } else if let TokenType::True = &self.curr_token.token_type {
            return Ok(self.ast.add_node(Expression::Literal(LiteralNode {
                value: Literal::Bool(true),
            })));
        }
        Err(Self::unexpected_token_error(self, "boolean"))
    }

    /// Parses an expression enclosed in parentheses: `( ... )`.
    /// This allows nested expressions and changes precedence.
    fn parse_grouped_expression(&mut self) -> Result<NodeId, ParserError> {
        // We use the lowest precedence here, because we want to parse anything inside the parentheses
        let expression_id = self.parse_expression(Precedence::Lowest)?;
        self.expect_token(TokenType::RParen)?;
        Ok(expression_id)
    }

    /// Gets the correct infix parsing function based on `token_type`.
    ///
    /// Can fail if there exists no infix function for the given `token_type`
    fn infix_function(&self, token_type: &TokenType) -> Result<InfixFn, ParserError> {
        match token_type {
            TokenType::Plus => Ok(|parser, left_id| {
                parser.parse_binary_op(BinaryOperator::Plus, Precedence::Additive, left_id)
            }),
            TokenType::Minus => Ok(|parser, left_id| {
                parser.parse_binary_op(BinaryOperator::Minus, Precedence::Additive, left_id)
            }),
            TokenType::Star => Ok(|parser, left_id| {
                parser.parse_binary_op(BinaryOperator::Star, Precedence::Multiplicative, left_id)
            }),
            TokenType::Divide => Ok(|parser, left_id| {
                parser.parse_binary_op(BinaryOperator::Slash, Precedence::Multiplicative, left_id)
            }),
            TokenType::Mod => Ok(|parser, left_id| {
                parser.parse_binary_op(BinaryOperator::Modulo, Precedence::Multiplicative, left_id)
            }),

            TokenType::Equal => Ok(|parser, left_id| {
                parser.parse_binary_op(BinaryOperator::Equal, Precedence::Equality, left_id)
            }),
            TokenType::NotEqual => Ok(|parser, left_id| {
                parser.parse_binary_op(BinaryOperator::NotEqual, Precedence::Equality, left_id)
            }),

            TokenType::Greater => Ok(|parser, left_id| {
                parser.parse_binary_op(BinaryOperator::Greater, Precedence::Comparison, left_id)
            }),
            TokenType::GreaterEqual => Ok(|parser, left_id| {
                parser.parse_binary_op(
                    BinaryOperator::GreaterEqual,
                    Precedence::Comparison,
                    left_id,
                )
            }),
            TokenType::Less => Ok(|parser, left_id| {
                parser.parse_binary_op(BinaryOperator::Less, Precedence::Comparison, left_id)
            }),
            TokenType::LessEqual => Ok(|parser, left_id| {
                parser.parse_binary_op(BinaryOperator::LessEqual, Precedence::Comparison, left_id)
            }),
            TokenType::And => Ok(|parser, left_id| {
                parser.parse_logical_op(LogicalOperator::And, Precedence::LogicalAnd, left_id)
            }),
            TokenType::Or => Ok(|parser, left_id| {
                parser.parse_logical_op(LogicalOperator::Or, Precedence::LogicalOr, left_id)
            }),
            TokenType::LParen => Ok(Self::parse_function_call),
            _ => Err(ParserError::NoParseFn {
                found: token_type.to_string(),
                column: self.curr_token.column,
                line: self.curr_token.line,
            }),
        }
    }

    /// Parses a binary operator expression (`left <op> right`).
    ///
    /// - `op` is the binary operator type (e.g., Plus, Minus, Equal).
    /// - `precedence` is used to control how far the parser continues parsing the right-hand side.
    /// - `left_id` is the AST node ID of the already-parsed left-hand expression.
    fn parse_binary_op(
        &mut self,
        op: BinaryOperator,
        precedence: Precedence,
        left_id: NodeId,
    ) -> Result<NodeId, ParserError> {
        let right_id = self.parse_expression(precedence)?;
        let exp = Expression::Binary(BinaryExpressionNode {
            left_id,
            right_id,
            op,
        });
        Ok(self.ast.add_node(exp))
    }

    /// Parses a logical operator expression (`left AND right`, `left OR right`).
    ///
    /// Works similarly to `parse_binary_op`
    fn parse_logical_op(
        &mut self,
        op: LogicalOperator,
        precedence: Precedence,
        left_id: NodeId,
    ) -> Result<NodeId, ParserError> {
        let right_id = self.parse_expression(precedence)?;
        let exp = Expression::Logical(LogicalExpressionNode {
            left_id,
            right_id,
            op,
        });
        Ok(self.ast.add_node(exp))
    }

    /// Parses a function call expression (`(x,y)`)
    ///
    /// - `left_id` is the identifier for the function name (e.g., "func").
    /// - Parses zero or more arguments separated by commas.
    fn parse_function_call(&mut self, left_id: NodeId) -> Result<NodeId, ParserError> {
        let mut args = Vec::new();
        if self.peek_token.token_type != TokenType::RParen {
            args.push(self.parse_expression(Precedence::Lowest)?);
            while self.peek_token.token_type == TokenType::Comma {
                self.expect_token(TokenType::Comma)?;
                args.push(self.parse_expression(Precedence::Lowest)?);
            }
        }
        self.expect_token(TokenType::RParen)?;
        let expression = Expression::FunctionCall(FunctionCallNode {
            identifier_id: left_id,
            argument_ids: args,
        });
        Ok(self.ast.add_node(expression))
    }

    /// Parses an expression using Pratt parsing.
    ///
    /// Flow:
    /// 1. Reads the next token to become `curr_token`.
    /// 2. Finds the matching prefix function and parses the initial part of the expression.
    /// 3. While the next token has higher precedence than the current one,
    ///    calls the matching infix function to extend the expression.
    fn parse_expression(&mut self, precedence: Precedence) -> Result<NodeId, ParserError> {
        // Assume we are starting this function in position where the expression we want to parse
        // starts from the peek token
        self.read_token()?;
        let prefix_function = self.prefix_function(&self.curr_token.token_type)?;
        let mut expression_node_id = prefix_function(self)?;

        while self.peek_token.token_type.precedence() > precedence {
            self.read_token()?;
            let infix_function = self.infix_function(&self.curr_token.token_type)?;
            expression_node_id = infix_function(self, expression_node_id)?;
        }

        Ok(expression_node_id)
    }

    /// Parses type and transforms it to matching [`Ast::Type`].
    fn parse_type(&mut self) -> Result<Type, ParserError> {
        let ty = match self.peek_token.token_type {
            TokenType::Int32Type => Type::Int32,
            TokenType::Int64Type => Type::Int64,
            TokenType::Float32Type => Type::Float32,
            TokenType::Float64Type => Type::Float64,
            TokenType::BoolType => Type::Bool,
            TokenType::StringType => Type::String,
            TokenType::DateType => Type::Date,
            TokenType::DateTimeType => Type::DateTime,
            _ => {
                return Err(self.unexpected_token_error(
                    "any type (INT32, INT64, FLOAT32, FLOAT64, BOOL, STRING, DATE, DATETIME",
                ));
            }
        };
        self.read_token()?;
        Ok(ty)
    }

    /// Determines the correct statement parser based on the current token and runs it.
    fn parse_statement(&mut self) -> Result<Statement, ParserError> {
        match self.curr_token.token_type {
            TokenType::Select => self.parse_select_statement(),
            TokenType::Insert => self.parse_insert_statement(),
            TokenType::Update => self.parse_update_statement(),
            TokenType::Delete => self.parse_delete_statement(),
            TokenType::Create => self.parse_create_statement(),
            TokenType::Alter => self.parse_alter_statement(),
            TokenType::Truncate => self.parse_truncate_statement(),
            TokenType::Drop => self.parse_drop_statement(),
            _ => Err(self.unexpected_token_error(
                "one of INSERT, SELECT, UPDATE, DELETE, CREATE, ALTER, TRUNCATE, DROP",
            )),
        }
    }

    /// Parses a comma-separated list of column identifiers.
    fn parse_columns_common(&mut self) -> Result<Vec<NodeId>, ParserError> {
        let mut columns = Vec::new();
        let first_col = self.parse_column_name()?;
        columns.push(first_col);

        while self.peek_token.token_type == TokenType::Comma {
            self.read_token()?;
            let column = self.parse_column_name()?;
            columns.push(column);
        }
        Ok(columns)
    }

    /// Parses a DELETE statement:
    ///
    /// Syntax: `DELETE FROM <table> [WHERE <expression>]`
    fn parse_delete_statement(&mut self) -> Result<Statement, ParserError> {
        self.expect_token(TokenType::From)?;
        let table_name = self.parse_table_name()?;
        let where_clause = self.parse_where_clause()?;
        Ok(Statement::Delete(DeleteStatement {
            table_name,
            where_clause,
        }))
    }

    /// Parses an UPDATE statement:
    ///
    /// Syntax: `UPDATE <table> SET col1 = val1, col2 = val2 [WHERE <expression>]`
    fn parse_update_statement(&mut self) -> Result<Statement, ParserError> {
        let table_name = self.parse_table_name()?;
        self.expect_token(TokenType::Set)?;
        let column_setters = self.parse_column_setters()?;
        let where_clause = self.parse_where_clause()?;
        Ok(Statement::Update(UpdateStatement {
            table_name,
            column_setters,
            where_clause,
        }))
    }

    /// Parses a comma-separated list of `column = value` pairs for UPDATE statements.
    fn parse_column_setters(&mut self) -> Result<Vec<(NodeId, NodeId)>, ParserError> {
        let mut column_setters = Vec::new();
        column_setters.push(self.parse_column_setter()?);
        while self.peek_token.token_type == TokenType::Comma {
            self.read_token()?;
            column_setters.push(self.parse_column_setter()?);
        }
        Ok(column_setters)
    }

    /// Parses a single column-value pair for an UPDATE statement.
    fn parse_column_setter(&mut self) -> Result<(NodeId, NodeId), ParserError> {
        let column_id = self.parse_column_name()?;
        self.expect_token(TokenType::Equal)?;
        let value_id = self.parse_expression(Precedence::Lowest)?;
        Ok((column_id, value_id))
    }

    /// Parses an INSERT statement:
    ///
    /// Syntax:
    /// `INSERT INTO <table> [(col1, col2, ...)] VALUES (val1, val2, ...)`
    fn parse_insert_statement(&mut self) -> Result<Statement, ParserError> {
        self.expect_token(TokenType::Into)?;
        let table_name = self.parse_table_name()?;
        let columns = self.parse_insert_columns()?;
        self.expect_token(TokenType::Values)?;
        let values = self.parse_insert_values()?;
        Ok(Statement::Insert(InsertStatement {
            table_name,
            columns,
            values,
        }))
    }

    /// Parses the VALUES clause of an INSERT statement.
    fn parse_insert_values(&mut self) -> Result<Vec<NodeId>, ParserError> {
        self.expect_token(TokenType::LParen)?;
        let mut values = Vec::new();
        values.push(self.parse_expression(Precedence::Lowest)?);
        while self.peek_token.token_type == TokenType::Comma {
            self.read_token()?;
            values.push(self.parse_expression(Precedence::Lowest)?);
        }
        self.expect_token(TokenType::RParen)?;
        Ok(values)
    }

    /// Parses the optional column list in an INSERT statement.
    ///
    /// Returns:
    /// - `None` if no column list is provided (implying all columns).
    /// - `Some(Vec<NodeId>)` if a column list is explicitly provided.
    fn parse_insert_columns(&mut self) -> Result<Option<Vec<NodeId>>, ParserError> {
        if self.peek_token.token_type == TokenType::Values {
            self.read_token()?;
            return Ok(None);
        }

        self.expect_token(TokenType::LParen)?;

        let columns = self.parse_columns_common()?;

        self.expect_token(TokenType::RParen)?;
        Ok(Some(columns))
    }

    /// Parses a SELECT statement:
    ///
    /// Syntax:
    /// `SELECT <columns> FROM <table> [WHERE <expression>]`
    fn parse_select_statement(&mut self) -> Result<Statement, ParserError> {
        let columns = self.parse_select_columns()?;
        self.expect_token(TokenType::From)?;
        let table_name = self.parse_table_name()?;
        let where_clause = self.parse_where_clause()?;
        Ok(Statement::Select(SelectStatement {
            columns,
            table_name,
            where_clause,
        }))
    }

    /// Parses an optional WHERE clause, returning the expression node ID if present.
    fn parse_where_clause(&mut self) -> Result<Option<NodeId>, ParserError> {
        if self.peek_token.token_type != TokenType::Where {
            return Ok(None);
        }
        self.expect_token(TokenType::Where)?;
        Ok(Some(self.parse_expression(Precedence::Lowest)?))
    }

    /// Parses a table name as an identifier expression.
    fn parse_table_name(&mut self) -> Result<NodeId, ParserError> {
        let table_name = self.expect_ident()?;
        let node_id = self
            .ast
            .add_node(Expression::Identifier(IdentifierNode { value: table_name }));
        Ok(node_id)
    }

    /// Parses a column name as an identifier expression.
    fn parse_column_name(&mut self) -> Result<NodeId, ParserError> {
        let column_name = self.expect_ident()?;
        let node_id = self.ast.add_node(Expression::Identifier(IdentifierNode {
            value: column_name,
        }));
        Ok(node_id)
    }

    /// Parses the column list in a SELECT statement.
    ///
    /// Returns:
    /// - `None` if `*` is used (all columns).
    /// - `Some(Vec<NodeId>)` for explicit columns.
    fn parse_select_columns(&mut self) -> Result<Option<Vec<NodeId>>, ParserError> {
        if self.peek_token.token_type == TokenType::Star {
            self.read_token()?;
            return Ok(None);
        }
        let columns = self.parse_columns_common()?;
        Ok(Some(columns))
    }

    /// Parses a CREATE statement:
    ///
    /// Syntax:
    /// `CREATE TABLE <table> (column_descriptor_1, column_descriptor_2,...)`
    fn parse_create_statement(&mut self) -> Result<Statement, ParserError> {
        self.expect_token(TokenType::Table)?;
        let table_name = self.parse_table_name()?;
        let columns = self.parse_create_column_descriptors()?;
        Ok(Statement::Create(CreateStatement {
            table_name,
            columns,
        }))
    }

    /// Parses list of column descriptors inside parenthesis.
    fn parse_create_column_descriptors(
        &mut self,
    ) -> Result<Vec<CreateColumnDescriptor>, ParserError> {
        self.expect_token(TokenType::LParen)?;
        let mut column_descriptors = vec![];
        let first = self.parse_create_single_column_descriptor()?;
        column_descriptors.push(first);
        while self.peek_token.token_type == TokenType::Comma {
            self.read_token()?;
            column_descriptors.push(self.parse_create_single_column_descriptor()?);
        }
        self.expect_token(TokenType::RParen)?;
        Ok(column_descriptors)
    }

    /// Parses single column descriptor.
    ///
    /// Syntax:
    /// `column_descriptor -> <column_name> <type> <addon>`
    fn parse_create_single_column_descriptor(
        &mut self,
    ) -> Result<CreateColumnDescriptor, ParserError> {
        let name = self.parse_column_name()?;
        let ty = self.parse_type()?;
        let addon = self.parse_create_addon()?;
        Ok(CreateColumnDescriptor { name, ty, addon })
    }

    /// Parses column descriptor addon.
    fn parse_create_addon(&mut self) -> Result<CreateColumnAddon, ParserError> {
        let addon = match self.peek_token.token_type {
            TokenType::PrimaryKey => CreateColumnAddon::PrimaryKey,
            _ => CreateColumnAddon::None,
        };
        if addon != CreateColumnAddon::None {
            self.read_token()?;
        }
        Ok(addon)
    }

    /// Parses an ALTER statement:
    ///
    /// Syntax:
    /// `ALTER TABLE <table> <alter_action>`
    fn parse_alter_statement(&mut self) -> Result<Statement, ParserError> {
        self.expect_token(TokenType::Table)?;
        let table_name = self.parse_table_name()?;
        let action = match self.peek_token.token_type {
            TokenType::Add => self.parse_alter_add()?,
            TokenType::Rename => self.parse_alter_rename()?,
            TokenType::Drop => self.parse_alter_drop()?,
            _ => return Err(self.unexpected_token_error("one of ADD, RENAME, DROP")),
        };
        Ok(Statement::Alter(AlterStatement { table_name, action }))
    }

    /// Parses ADD variant of the ALTER statement.
    ///
    /// Syntax:
    /// `<add_alter_action> -> ADD <column> <type>`
    fn parse_alter_add(&mut self) -> Result<AlterAction, ParserError> {
        self.expect_token(TokenType::Add)?;
        let column_name = self.parse_column_name()?;
        let column_type = self.parse_type()?;
        Ok(AlterAction::Add(AddAlterAction {
            column_name,
            column_type,
        }))
    }

    /// Parses RENAME variant of the ALTER statement.
    ///
    /// Syntax:
    /// `<rename_column_alter_action> -> RENAME COLUMN <prev> TO <new>`
    /// `rename_table_alter_action -> RENAME TABLE TO <new>`
    fn parse_alter_rename(&mut self) -> Result<AlterAction, ParserError> {
        self.expect_token(TokenType::Rename)?;
        match self.peek_token.token_type {
            TokenType::Column => {
                self.read_token()?;
                let previous_name = self.parse_column_name()?;
                self.expect_token(TokenType::To)?;
                let new_name = self.parse_column_name()?;
                Ok(AlterAction::RenameColumn(RenameColumnAlterAction {
                    previous_name,
                    new_name,
                }))
            }
            TokenType::Table => {
                self.read_token()?;
                self.expect_token(TokenType::To)?;
                let new_name = self.parse_table_name()?;
                Ok(AlterAction::RenameTable(RenameTableAlterAction {
                    new_name,
                }))
            }
            _ => Err(self.unexpected_token_error("one of COLUMN, TABLE")),
        }
    }

    /// Parses DROP variant of the ALTER statement.
    ///
    /// Syntax:
    /// `<drop_alter_action> -> DROP COLUMN <column>`
    fn parse_alter_drop(&mut self) -> Result<AlterAction, ParserError> {
        self.expect_token(TokenType::Drop)?;
        self.expect_token(TokenType::Column)?;
        let column_name = self.parse_column_name()?;
        Ok(AlterAction::Drop(DropAlterAction { column_name }))
    }

    /// Parses a TRUNCATE statement.
    ///
    /// Syntax:
    /// `TRUNCATE TABLE <table>`
    fn parse_truncate_statement(&mut self) -> Result<Statement, ParserError> {
        self.expect_token(TokenType::Table)?;
        let table_name = self.parse_table_name()?;
        Ok(Statement::Truncate(TruncateStatement { table_name }))
    }

    fn parse_drop_statement(&mut self) -> Result<Statement, ParserError> {
        todo!()
    }

    /// Advances the parser by one token.
    /// Also checks for illegal tokens from the lexer and returns an error if found.
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

    /// Consumes the next token if it matches the expected type.
    /// Otherwise, returns an `UnexpectedToken` error.
    fn expect_token(&mut self, expected_type: TokenType) -> Result<(), ParserError> {
        if self.peek_token.token_type == expected_type {
            self.read_token()?;
            Ok(())
        } else {
            Err(self.unexpected_token_error(&expected_type.to_string()))
        }
    }

    /// Consumes the next token if it’s an identifier and returns its string value.
    fn expect_ident(&mut self) -> Result<String, ParserError> {
        if let TokenType::Ident(s) = &self.peek_token.token_type {
            let value = s.clone();
            self.read_token()?;
            Ok(value)
        } else {
            Err(self.unexpected_token_error("string"))
        }
    }

    /// Helper to create a consistent `UnexpectedToken` error.
    fn unexpected_token_error(&self, expected: &str) -> ParserError {
        ParserError::UnexpectedToken {
            expected: expected.to_string(),
            found: self.peek_token.token_type.to_string(),
            line: self.peek_token.line,
            column: self.peek_token.column,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_error_on_empty_input() {
        let parser = Parser::new("");
        let result = parser.parse_program();
        assert!(result.is_err());
        let errors = result.err().unwrap();
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors.first().unwrap(),
            ParserError::UnexpectedToken { .. }
        ));
    }

    #[test]
    fn parses_select_statement_correctly() {
        let parser = Parser::new("Select * from table_name;");
        let ast = parser.parse_program().unwrap();
        assert_eq!(ast.statements.len(), 1);

        let Statement::Select(select_stmt) = &ast.statements[0] else {
            panic!("Expected Select statement, got {:#?}", ast.statements[0]);
        };
        assert!(select_stmt.where_clause.is_none());

        let Expression::Identifier(table_ident) = ast.node(select_stmt.table_name) else {
            panic!(
                "Expected Identifier for table, got {:#?}",
                ast.node(select_stmt.table_name)
            );
        };
        assert_eq!(table_ident.value, "table_name");
    }

    #[test]
    fn parses_delete_statement_correctly() {
        let parser = Parser::new("DELETE FROM table_name;");
        let ast = parser.parse_program().unwrap();
        assert_eq!(ast.statements.len(), 1);

        let Statement::Delete(delete_stmt) = &ast.statements[0] else {
            panic!("Expected Delete statement, got {:?}", &ast.statements[0]);
        };
        assert!(delete_stmt.where_clause.is_none());

        let Expression::Identifier(ident) = ast.node(delete_stmt.table_name) else {
            panic!(
                "Expected Identifier, got {:?}",
                ast.node(delete_stmt.table_name)
            );
        };
        assert_eq!(ident.value, "table_name");
    }

    #[test]
    fn parses_update_statement_correctly() {
        let parser = Parser::new("UPDATE table_name SET col1 = 4, col2 = 6.1;");
        let ast = parser.parse_program().unwrap();
        assert_eq!(ast.statements.len(), 1);

        let Statement::Update(update_stmt) = &ast.statements[0] else {
            panic!("Expected Update statement, got {:#?}", ast.statements[0]);
        };
        assert!(update_stmt.where_clause.is_none());

        let Expression::Identifier(table_ident) = ast.node(update_stmt.table_name) else {
            panic!(
                "Expected Identifier for table, got {:#?}",
                ast.node(update_stmt.table_name)
            );
        };
        assert_eq!(table_ident.value, "table_name");

        assert_eq!(update_stmt.column_setters.len(), 2);

        let (col1_id, col1_val_id) = update_stmt.column_setters[0];
        let Expression::Identifier(col1_ident) = ast.node(col1_id) else {
            panic!("Expected Identifier for col1, got {:#?}", ast.node(col1_id));
        };
        assert_eq!(col1_ident.value, "col1");

        let Expression::Literal(col1_literal) = ast.node(col1_val_id) else {
            panic!(
                "Expected Literal for col1 value, got {:#?}",
                ast.node(col1_val_id)
            );
        };
        let Literal::Int(i) = col1_literal.value else {
            panic!(
                "Expected Int literal for col1, got {:#?}",
                col1_literal.value
            );
        };
        assert_eq!(i, 4);

        let (col2_id, col2_val_id) = update_stmt.column_setters[1];
        let Expression::Identifier(col2_ident) = ast.node(col2_id) else {
            panic!("Expected Identifier for col2, got {:#?}", ast.node(col2_id));
        };
        assert_eq!(col2_ident.value, "col2");

        let Expression::Literal(col2_literal) = ast.node(col2_val_id) else {
            panic!(
                "Expected Literal for col2 value, got {:#?}",
                ast.node(col2_val_id)
            );
        };
        let Literal::Float(f) = col2_literal.value else {
            panic!(
                "Expected Float literal for col2, got {:#?}",
                col2_literal.value
            );
        };
        assert_eq!(f, 6.1);
    }

    #[test]
    fn parses_insert_statement_correctly() {
        let parser = Parser::new("INSERT INTO table_name (col1, col2) VALUES (1, 2.5);");
        let ast = parser.parse_program().unwrap();
        assert_eq!(ast.statements.len(), 1);

        let Statement::Insert(insert_stmt) = &ast.statements[0] else {
            panic!("Expected Insert statement, got {:#?}", ast.statements[0]);
        };

        let Expression::Identifier(table_ident) = ast.node(insert_stmt.table_name) else {
            panic!(
                "Expected Identifier for table, got {:#?}",
                ast.node(insert_stmt.table_name)
            );
        };
        assert_eq!(table_ident.value, "table_name");

        let column_ids = insert_stmt.columns.as_ref().unwrap();
        assert_eq!(column_ids.len(), 2);

        let Expression::Identifier(col1_ident) = ast.node(column_ids[0]) else {
            panic!(
                "Expected Identifier for col1, got {:#?}",
                ast.node(column_ids[0])
            );
        };
        assert_eq!(col1_ident.value, "col1");

        let Expression::Identifier(col2_ident) = ast.node(column_ids[1]) else {
            panic!(
                "Expected Identifier for col2, got {:#?}",
                ast.node(column_ids[1])
            );
        };
        assert_eq!(col2_ident.value, "col2");

        let Expression::Literal(val1) = ast.node(insert_stmt.values[0]) else {
            panic!(
                "Expected Literal for value1, got {:#?}",
                ast.node(insert_stmt.values[0])
            );
        };
        let Literal::Int(i1) = val1.value else {
            panic!("Expected Int literal for value1, got {:#?}", val1.value);
        };
        assert_eq!(i1, 1);

        let Expression::Literal(val2) = ast.node(insert_stmt.values[1]) else {
            panic!(
                "Expected Literal for value2, got {:#?}",
                ast.node(insert_stmt.values[1])
            );
        };
        let Literal::Float(f2) = val2.value else {
            panic!("Expected Float literal for value2, got {:#?}", val2.value);
        };
        assert_eq!(f2, 2.5);
    }

    #[test]
    fn parses_select_with_complex_where_clause() {
        let parser = Parser::new("SELECT * FROM table_name WHERE a + 3 * b > 10;");
        let ast = parser.parse_program().unwrap();
        assert_eq!(ast.statements.len(), 1);

        let Statement::Select(select_stmt) = &ast.statements[0] else {
            panic!("Expected Select statement, got {:?}", ast.statements[0]);
        };

        let Expression::Identifier(table_ident) = ast.node(select_stmt.table_name) else {
            panic!("Expected Identifier for table name");
        };
        assert_eq!(table_ident.value, "table_name");

        let where_id = select_stmt.where_clause.expect("Expected WHERE clause");

        let Expression::Binary(where_binary) = ast.node(where_id) else {
            panic!("Expected Binary expression at WHERE clause root");
        };
        assert!(matches!(where_binary.op, BinaryOperator::Greater));

        let Expression::Binary(add_expr) = ast.node(where_binary.left_id) else {
            panic!("Expected Binary expression (Plus) on left of Greater");
        };
        assert!(matches!(add_expr.op, BinaryOperator::Plus));

        let Expression::Identifier(a_ident) = ast.node(add_expr.left_id) else {
            panic!("Expected Identifier on left of Plus");
        };
        assert_eq!(a_ident.value, "a");

        let Expression::Binary(mul_expr) = ast.node(add_expr.right_id) else {
            panic!("Expected Binary expression (Star) on right of Plus");
        };
        assert!(matches!(mul_expr.op, BinaryOperator::Star));

        let Expression::Literal(lit_three) = ast.node(mul_expr.left_id) else {
            panic!("Expected Literal on left of Star");
        };
        assert!(matches!(lit_three.value, Literal::Int(3)));

        let Expression::Identifier(b_ident) = ast.node(mul_expr.right_id) else {
            panic!("Expected Identifier on right of Star");
        };
        assert_eq!(b_ident.value, "b");

        let Expression::Literal(lit_ten) = ast.node(where_binary.right_id) else {
            panic!("Expected Literal on right of Greater");
        };
        assert!(matches!(lit_ten.value, Literal::Int(10)));
    }

    #[test]
    fn parses_where_with_function_call_and_parentheses() {
        let parser = Parser::new("SELECT * FROM table_name WHERE (LENGTH(name) + 3) * 2 > 10;");
        let result = parser.parse_program();
        assert!(result.is_ok(), "Parser error: {:?}", result.err());
        let ast = result.unwrap();

        let Statement::Select(select_stmt) = &ast.statements[0] else {
            panic!("Expected SELECT statement, got {:?}", ast.statements[0]);
        };

        let where_id = select_stmt.where_clause.expect("Expected WHERE clause");
        let root_expr = ast.node(where_id);
        let Expression::Binary(root_bin) = root_expr else {
            panic!("Expected Binary expression at root, got {root_expr:?}");
        };
        assert!(matches!(root_bin.op, BinaryOperator::Greater));

        match ast.node(root_bin.right_id) {
            Expression::Literal(lit) => match &lit.value {
                Literal::Int(i) => assert_eq!(*i, 10),
                other => panic!("Expected int literal, got {other:?}"),
            },
            other => panic!("Expected literal, got {other:?}"),
        }

        let Expression::Binary(mul_bin) = ast.node(root_bin.left_id) else {
            panic!(
                "Expected multiplication, got {:?}",
                ast.node(root_bin.left_id)
            );
        };
        assert!(matches!(mul_bin.op, BinaryOperator::Star));

        match ast.node(mul_bin.right_id) {
            Expression::Literal(lit) => match &lit.value {
                Literal::Int(i) => assert_eq!(*i, 2),
                other => panic!("Expected int literal, got {other:?}"),
            },
            other => panic!("Expected literal, got {other:?}"),
        }

        let Expression::Binary(add_bin) = ast.node(mul_bin.left_id) else {
            panic!("Expected addition, got {:?}", ast.node(mul_bin.left_id));
        };
        assert!(matches!(add_bin.op, BinaryOperator::Plus));

        match ast.node(add_bin.right_id) {
            Expression::Literal(lit) => match &lit.value {
                Literal::Int(i) => assert_eq!(*i, 3),
                other => panic!("Expected int literal, got {other:?}"),
            },
            other => panic!("Expected literal, got {other:?}"),
        }

        let Expression::FunctionCall(func_call) = ast.node(add_bin.left_id) else {
            panic!(
                "Expected function call, got {:?}",
                ast.node(add_bin.left_id)
            );
        };

        match ast.node(func_call.identifier_id) {
            Expression::Identifier(ident) => assert_eq!(ident.value, "LENGTH"),
            other => panic!("Expected identifier for function name, got {other:?}"),
        }

        assert_eq!(func_call.argument_ids.len(), 1);
        match ast.node(func_call.argument_ids[0]) {
            Expression::Identifier(ident) => assert_eq!(ident.value, "name"),
            other => panic!("Expected identifier for function argument, got {other:?}"),
        }
    }

    #[test]
    fn parses_unary_expression_correctly() {
        let parser = Parser::new("SELECT * FROM products WHERE (-price + Length(name)) * 2 > 10;");
        let result = parser.parse_program();
        assert!(result.is_ok());
        let ast = result.unwrap();
        assert_eq!(ast.statements.len(), 1);

        let Statement::Select(select_stmt) = &ast.statements[0] else {
            panic!("Expected Select statement, got {:?}", ast.statements[0]);
        };
        assert!(select_stmt.where_clause.is_some());

        let where_id = select_stmt.where_clause.unwrap();

        let Expression::Binary(greater_expr) = ast.node(where_id) else {
            panic!(
                "Expected top-level BinaryExpression (>), got {:?}",
                ast.node(where_id)
            );
        };
        assert!(matches!(greater_expr.op, BinaryOperator::Greater));

        let Expression::Binary(mul_expr) = ast.node(greater_expr.left_id) else {
            panic!(
                "Expected BinaryExpression (*), got {:?}",
                ast.node(greater_expr.left_id)
            );
        };
        assert!(matches!(mul_expr.op, BinaryOperator::Star));

        let Expression::Binary(add_expr) = ast.node(mul_expr.left_id) else {
            panic!(
                "Expected BinaryExpression (+), got {:?}",
                ast.node(mul_expr.left_id)
            );
        };
        assert!(matches!(add_expr.op, BinaryOperator::Plus));

        let Expression::Unary(unary_expr) = ast.node(add_expr.left_id) else {
            panic!(
                "Expected UnaryExpression (-), got {:?}",
                ast.node(add_expr.left_id)
            );
        };
        assert!(matches!(unary_expr.op, UnaryOperator::Minus));

        let Expression::Identifier(price_ident) = ast.node(unary_expr.expression_id) else {
            panic!(
                "Expected Identifier inside unary, got {:?}",
                ast.node(unary_expr.expression_id)
            );
        };
        assert_eq!(price_ident.value, "price");

        let Expression::FunctionCall(func_node) = ast.node(add_expr.right_id) else {
            panic!(
                "Expected FunctionCallExpression, got {:?}",
                ast.node(add_expr.right_id)
            );
        };
        let Expression::Identifier(func_ident) = ast.node(func_node.identifier_id) else {
            panic!(
                "Expected Identifier for function name, got {:?}",
                ast.node(func_node.identifier_id)
            );
        };
        assert_eq!(func_ident.value, "Length");

        let arg_id = func_node.argument_ids[0];
        let Expression::Identifier(arg) = ast.node(arg_id) else {
            panic!(
                "Expected Identifier as function argument, got {:?}",
                ast.node(arg_id)
            );
        };
        assert_eq!(arg.value, "name");

        let Expression::Literal(literal_node) = ast.node(mul_expr.right_id) else {
            panic!(
                "Expected LiteralExpression (2), got {:?}",
                ast.node(mul_expr.right_id)
            );
        };
        let Literal::Int(i) = literal_node.value else {
            panic!("Expected Int literal, got {:?}", literal_node.value);
        };
        assert_eq!(i, 2);

        let Expression::Literal(literal_node) = ast.node(greater_expr.right_id) else {
            panic!(
                "Expected LiteralExpression (10), got {:?}",
                ast.node(greater_expr.right_id)
            );
        };
        let Literal::Int(i) = literal_node.value else {
            panic!("Expected Int literal, got {:?}", literal_node.value);
        };
        assert_eq!(i, 10);
    }

    #[test]
    fn parses_multiple_statements() {
        let parser =
            Parser::new("SELECT * FROM products; Select * FROM users; DELETE FROM table_name;");
        let result = parser.parse_program();
        assert!(result.is_ok());
        let ast = result.unwrap();
        assert_eq!(ast.statements.len(), 3);
        assert!(matches!(ast.statements[0], Statement::Select(_)));
        assert!(matches!(ast.statements[1], Statement::Select(_)));
        assert!(matches!(ast.statements[2], Statement::Delete(_)));
    }

    #[test]
    fn correctly_recovers_from_statements_with_error_and_continues() {
        let parser = Parser::new("SELECT * FROM; Select * FROM users; DELETE table_name;");
        let result = parser.parse_program();
        assert!(result.is_err());
        let error = result.err().unwrap();
        assert_eq!(error.len(), 2);
    }

    #[test]
    fn parses_create_table_statement_correctly() {
        let parser = Parser::new("CREATE TABLE users (id INT64 PRIMARY_KEY, name STRING);");
        let ast = parser.parse_program().unwrap();
        assert_eq!(ast.statements.len(), 1);

        let Statement::Create(create_stmt) = &ast.statements[0] else {
            panic!("Expected Create statement, got {:#?}", ast.statements[0]);
        };

        let Expression::Identifier(table_ident) = ast.node(create_stmt.table_name) else {
            panic!(
                "Expected Identifier for table, got {:#?}",
                ast.node(create_stmt.table_name)
            );
        };
        assert_eq!(table_ident.value, "users");

        assert_eq!(create_stmt.columns.len(), 2);

        let col0 = &create_stmt.columns[0];
        let Expression::Identifier(col0_ident) = ast.node(col0.name) else {
            panic!(
                "Expected Identifier for first column, got {:#?}",
                ast.node(col0.name)
            );
        };
        assert_eq!(col0_ident.value, "id");
        assert!(matches!(col0.ty, Type::Int64));
        assert!(matches!(col0.addon, CreateColumnAddon::PrimaryKey));

        let col1 = &create_stmt.columns[1];
        let Expression::Identifier(col1_ident) = ast.node(col1.name) else {
            panic!(
                "Expected Identifier for second column, got {:#?}",
                ast.node(col1.name)
            );
        };
        assert_eq!(col1_ident.value, "name");
        assert!(matches!(col1.ty, Type::String));
        assert!(matches!(col1.addon, CreateColumnAddon::None));
    }

    #[test]
    fn parses_alter_add_correctly() {
        let parser = Parser::new("ALTER TABLE users ADD age INT32;");
        let ast = parser.parse_program().unwrap();
        assert_eq!(ast.statements.len(), 1);

        let Statement::Alter(alter_stmt) = &ast.statements[0] else {
            panic!("Expected Alter statement, got {:#?}", ast.statements[0]);
        };

        let Expression::Identifier(table_ident) = ast.node(alter_stmt.table_name) else {
            panic!(
                "Expected Identifier for table, got {:#?}",
                ast.node(alter_stmt.table_name)
            );
        };
        assert_eq!(table_ident.value, "users");

        match &alter_stmt.action {
            AlterAction::Add(add) => {
                let Expression::Identifier(col_ident) = ast.node(add.column_name) else {
                    panic!(
                        "Expected Identifier for added column, got {:#?}",
                        ast.node(add.column_name)
                    );
                };
                assert_eq!(col_ident.value, "age");
                assert!(matches!(add.column_type, Type::Int32));
            }
            other => panic!("Expected Add action, got {:#?}", other),
        }
    }

    #[test]
    fn parses_alter_rename_column_correctly() {
        let parser = Parser::new("ALTER TABLE users RENAME COLUMN old_name TO new_name;");
        let ast = parser.parse_program().unwrap();
        assert_eq!(ast.statements.len(), 1);

        let Statement::Alter(alter_stmt) = &ast.statements[0] else {
            panic!("Expected Alter statement, got {:#?}", ast.statements[0]);
        };

        let Expression::Identifier(table_ident) = ast.node(alter_stmt.table_name) else {
            panic!(
                "Expected Identifier for table, got {:#?}",
                ast.node(alter_stmt.table_name)
            );
        };
        assert_eq!(table_ident.value, "users");

        match &alter_stmt.action {
            AlterAction::RenameColumn(rename) => {
                let Expression::Identifier(prev_ident) = ast.node(rename.previous_name) else {
                    panic!(
                        "Expected Identifier for previous column name, got {:#?}",
                        ast.node(rename.previous_name)
                    );
                };
                assert_eq!(prev_ident.value, "old_name");

                let Expression::Identifier(new_ident) = ast.node(rename.new_name) else {
                    panic!(
                        "Expected Identifier for new column name, got {:#?}",
                        ast.node(rename.new_name)
                    );
                };
                assert_eq!(new_ident.value, "new_name");
            }
            other => panic!("Expected Rename action, got {:#?}", other),
        }
    }

    #[test]
    fn parses_alter_rename_table_correctly() {
        let parser = Parser::new("ALTER TABLE users RENAME TABLE TO new_users;");
        let ast = parser.parse_program().unwrap();
        assert_eq!(ast.statements.len(), 1);

        let Statement::Alter(alter_stmt) = &ast.statements[0] else {
            panic!("Expected Alter statement, got {:#?}", ast.statements[0]);
        };

        let Expression::Identifier(table_ident) = ast.node(alter_stmt.table_name) else {
            panic!(
                "Expected Identifier for table, got {:#?}",
                ast.node(alter_stmt.table_name)
            );
        };
        assert_eq!(table_ident.value, "users");

        match &alter_stmt.action {
            AlterAction::RenameTable(rename) => {
                let Expression::Identifier(new_ident) = ast.node(rename.new_name) else {
                    panic!(
                        "Expected Identifier for new table name, got {:#?}",
                        ast.node(rename.new_name)
                    );
                };
                assert_eq!(new_ident.value, "new_users");
            }
            other => panic!("Expected Rename action, got {:#?}", other),
        }
    }

    #[test]
    fn parses_alter_drop_correctly() {
        let parser = Parser::new("ALTER TABLE users DROP COLUMN age;");
        let ast = parser.parse_program().unwrap();
        assert_eq!(ast.statements.len(), 1);

        let Statement::Alter(alter_stmt) = &ast.statements[0] else {
            panic!("Expected Alter statement, got {:#?}", ast.statements[0]);
        };

        let Expression::Identifier(table_ident) = ast.node(alter_stmt.table_name) else {
            panic!(
                "Expected Identifier for table, got {:#?}",
                ast.node(alter_stmt.table_name)
            );
        };
        assert_eq!(table_ident.value, "users");

        match &alter_stmt.action {
            AlterAction::Drop(drop) => {
                let Expression::Identifier(col_ident) = ast.node(drop.column_name) else {
                    panic!(
                        "Expected Identifier for dropped column, got {:#?}",
                        ast.node(drop.column_name)
                    );
                };
                assert_eq!(col_ident.value, "age");
            }
            other => panic!("Expected Drop action, got {:#?}", other),
        }
    }

    #[test]
    fn parses_truncate_statement_correctly() {
        let parser = Parser::new("TRUNCATE TABLE sessions;");
        let ast = parser.parse_program().unwrap();
        assert_eq!(ast.statements.len(), 1);

        let Statement::Truncate(trunc_stmt) = &ast.statements[0] else {
            panic!("Expected Truncate statement, got {:#?}", ast.statements[0]);
        };

        let Expression::Identifier(table_ident) = ast.node(trunc_stmt.table_name) else {
            panic!(
                "Expected Identifier for table, got {:#?}",
                ast.node(trunc_stmt.table_name)
            );
        };
        assert_eq!(table_ident.value, "sessions");
    }
}

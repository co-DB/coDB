//! AST module - definition of coSQL syntax tree nodes and statements.

use thiserror::Error;

/// [`Ast`] represents query as list of statements ([`Ast::statements`]). Each statement is built using nodes defined in [`Ast::nodes`]. Every statement inserted to [`Ast`] is guaranteed to have every node id valid - user of this module does not need to assert it.
///
/// When executing, statements should be run in order of appearance.
pub struct Ast {
    nodes: Vec<Expression>,
    statements: Vec<Statement>,
}

/// Error for [`Ast`] related operations.
#[derive(Error, Debug)]
pub enum AstError {
    /// Provided node id was invalid, e.g. index was out of bound
    #[error("invalid node id: {0}")]
    InvalidNodeId(usize),
}

impl Ast {
    /// Create new, empty [`Ast`].
    pub fn new() -> Self {
        Ast {
            nodes: vec![],
            statements: vec![],
        }
    }

    /// Returns all statements stored in [`Ast`]. They should be executed in the order of appearance.
    pub fn statements(&self) -> &[Statement] {
        &self.statements
    }

    /// Returns node with `node_id`. Can fail if `node_id` is invalid. Note that every element in ast is guaranteed to have valid `node_id`.
    pub fn node(&self, node_id: usize) -> Result<&Expression, AstError> {
        match self.is_valid_node_id(node_id) {
            true => Ok(&self.nodes[node_id]),
            false => Err(AstError::InvalidNodeId(node_id)),
        }
    }

    /// Adds node to [`Ast`] and returns its `node_id`. Can fail if [`AstElementValidator::validate`] implementation for node returns error.
    pub(crate) fn add_node(&mut self, node: Expression) -> Result<usize, AstError> {
        node.validate(self)?;
        self.nodes.push(node);
        Ok(self.nodes.len() - 1)
    }

    /// Adds statement to [`Ast`] and returns its `statement_id`. Can fail if [`AstElementValidator::validate`] implementation for statement returns error.
    pub(crate) fn add_statement(&mut self, statement: Statement) -> Result<usize, AstError> {
        statement.validate(self)?;
        self.statements.push(statement);
        Ok(self.statements.len() - 1)
    }

    /// Helper for testing if `node_id` is valid for given [`Ast`].
    fn is_valid_node_id(&self, node_id: usize) -> bool {
        node_id < self.nodes.len()
    }
}

impl Default for Ast {
    fn default() -> Self {
        Self::new()
    }
}

/// A common trait for validating if element can be added to [`Ast`]
trait AstElementValidator {
    fn validate(&self, ast: &Ast) -> Result<(), AstError>;
}

fn validate_id(ast: &Ast, id: usize) -> Result<(), AstError> {
    match ast.is_valid_node_id(id) {
        true => Ok(()),
        false => Err(AstError::InvalidNodeId(id)),
    }
}

fn validate_optional_id(ast: &Ast, id: &Option<usize>) -> Result<(), AstError> {
    match id {
        Some(id) => validate_id(ast, *id),
        None => Ok(()),
    }
}

fn validate_ids(ast: &Ast, ids: &[usize]) -> Result<(), AstError> {
    ids.iter().try_for_each(|&id| validate_id(ast, id))
}

fn validate_optional_ids(ast: &Ast, ids: &Option<Vec<usize>>) -> Result<(), AstError> {
    match ids {
        Some(ids) => validate_ids(ast, ids),
        None => Ok(()),
    }
}

pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
}

impl AstElementValidator for Statement {
    fn validate(&self, ast: &Ast) -> Result<(), AstError> {
        match self {
            Statement::Select(statement) => statement.validate(ast),
            Statement::Insert(statement) => statement.validate(ast),
            Statement::Update(statement) => statement.validate(ast),
            Statement::Delete(statement) => statement.validate(ast),
        }
    }
}

pub struct SelectStatement {
    pub column_ids: Option<Vec<usize>>,
    pub table_name_id: usize,
    pub where_clause_id: Option<usize>,
}

impl AstElementValidator for SelectStatement {
    fn validate(&self, ast: &Ast) -> Result<(), AstError> {
        validate_optional_ids(ast, &self.column_ids)?;
        validate_id(ast, self.table_name_id)?;
        validate_optional_id(ast, &self.where_clause_id)?;
        Ok(())
    }
}

pub struct InsertStatement {
    pub table_name_id: usize,
    pub column_ids: Option<Vec<usize>>,
    pub value_ids: Vec<usize>,
}

impl AstElementValidator for InsertStatement {
    fn validate(&self, ast: &Ast) -> Result<(), AstError> {
        validate_id(ast, self.table_name_id)?;
        validate_optional_ids(ast, &self.column_ids)?;
        validate_ids(ast, &self.value_ids)?;
        Ok(())
    }
}

pub struct UpdateStatement {
    pub table_name_id: usize,
    pub column_setters: Vec<(usize, usize)>,
    pub where_clause_id: Option<usize>,
}

impl AstElementValidator for UpdateStatement {
    fn validate(&self, ast: &Ast) -> Result<(), AstError> {
        validate_id(ast, self.table_name_id)?;
        let column_setter_ids: Vec<_> = self
            .column_setters
            .iter()
            .copied()
            .flat_map(|(lhs, rhs)| [lhs, rhs])
            .collect();
        validate_ids(ast, &column_setter_ids)?;
        validate_optional_id(ast, &self.where_clause_id)?;
        Ok(())
    }
}

pub struct DeleteStatement {
    pub table_name_id: usize,
    pub where_clause_id: Option<usize>,
}

impl AstElementValidator for DeleteStatement {
    fn validate(&self, ast: &Ast) -> Result<(), AstError> {
        validate_id(ast, self.table_name_id)?;
        validate_optional_id(ast, &self.where_clause_id)?;
        Ok(())
    }
}

pub enum Type {
    String,
    F32,
    F64,
    I32,
    I64,
    Bool,
}

pub enum Literal {
    String(String),
    F32(f32),
    F64(f64),
    I32(i32),
    I64(i64),
    Bool(bool),
}

pub enum Expression {
    Logical(LogicalExpressionNode),
    Binary(BinaryExpressionNode),
    Unary(UnaryExpressionNode),
    FunctionCall(FunctionCallNode),
    Literal(LiteralNode),
    Identifier(IdentifierNode),
}

impl AstElementValidator for Expression {
    fn validate(&self, ast: &Ast) -> Result<(), AstError> {
        match self {
            Expression::Logical(node) => node.validate(ast),
            Expression::Binary(node) => node.validate(ast),
            Expression::Unary(node) => node.validate(ast),
            Expression::FunctionCall(node) => node.validate(ast),
            Expression::Literal(node) => node.validate(ast),
            Expression::Identifier(node) => node.validate(ast),
        }
    }
}

pub enum LogicalOperator {
    And,
    Or,
}

pub struct LogicalExpressionNode {
    pub left_id: usize,
    pub right_id: usize,
    pub op: LogicalOperator,
    pub ty: Type,
}

impl AstElementValidator for LogicalExpressionNode {
    fn validate(&self, ast: &Ast) -> Result<(), AstError> {
        validate_id(ast, self.left_id)?;
        validate_id(ast, self.right_id)?;
        Ok(())
    }
}

pub enum BinaryOperator {
    Plus,
    Minus,
    Start,
    Slash,
    Modulo,
    Equal,
    NotEqual,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
}

pub struct BinaryExpressionNode {
    pub left_id: usize,
    pub right_id: usize,
    pub op: BinaryOperator,
    pub ty: Type,
}

impl AstElementValidator for BinaryExpressionNode {
    fn validate(&self, ast: &Ast) -> Result<(), AstError> {
        validate_id(ast, self.left_id)?;
        validate_id(ast, self.right_id)?;
        Ok(())
    }
}

pub enum UnaryOperator {
    Plus,
    Minus,
}

pub struct UnaryExpressionNode {
    pub expression_id: usize,
    pub op: UnaryOperator,
    pub ty: Type,
}

impl AstElementValidator for UnaryExpressionNode {
    fn validate(&self, ast: &Ast) -> Result<(), AstError> {
        validate_id(ast, self.expression_id)?;
        Ok(())
    }
}

pub struct FunctionCallNode {
    pub identifier_id: usize,
    pub argument_ids: Vec<usize>,
    pub ty: Type,
}

impl AstElementValidator for FunctionCallNode {
    fn validate(&self, ast: &Ast) -> Result<(), AstError> {
        validate_id(ast, self.identifier_id)?;
        validate_ids(ast, &self.argument_ids)?;
        Ok(())
    }
}

pub struct LiteralNode {
    pub value: Literal,
    pub ty: Type,
}

impl AstElementValidator for LiteralNode {
    fn validate(&self, _ast: &Ast) -> Result<(), AstError> {
        Ok(())
    }
}

pub struct IdentifierNode {
    pub value: String,
    pub ty: Type,
}

impl AstElementValidator for IdentifierNode {
    fn validate(&self, _ast: &Ast) -> Result<(), AstError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to create a simple identifier node and return its id
    fn add_identifier(ast: &mut Ast, name: &str, ty: Type) -> usize {
        ast.add_node(Expression::Identifier(IdentifierNode {
            value: name.to_string(),
            ty,
        }))
        .unwrap()
    }

    // Helper to create a literal node and return its id
    fn add_literal(ast: &mut Ast, lit: Literal, ty: Type) -> usize {
        ast.add_node(Expression::Literal(LiteralNode { value: lit, ty }))
            .unwrap()
    }

    #[test]
    fn ast_add_identifier_node() {
        // given a new AST and an identifier node
        let mut ast = Ast::new();
        let id = add_identifier(&mut ast, "foo", Type::I32);

        // when retrieving the node by id
        let expr = ast.node(id).unwrap();

        // then it is the expected identifier node
        match expr {
            Expression::Identifier(node) => assert_eq!(node.value, "foo"),
            _ => panic!("Expected IdentifierNode"),
        }
    }

    #[test]
    fn ast_add_select_statement() {
        // given a new AST and valid column/table nodes
        let mut ast = Ast::new();
        let col_id = add_identifier(&mut ast, "col", Type::I32);
        let table_id = add_identifier(&mut ast, "table", Type::String);

        // when adding a valid select statement
        let stmt = Statement::Select(SelectStatement {
            column_ids: Some(vec![col_id]),
            table_name_id: table_id,
            where_clause_id: None,
        });

        // then it succeeds
        assert!(ast.add_statement(stmt).is_ok());
    }

    #[test]
    fn ast_add_select_statement_with_invalid_column_id() {
        // given a new AST and a valid table node
        let mut ast = Ast::new();
        let table_id = add_identifier(&mut ast, "table", Type::String);

        // when adding a select statement with an invalid column id
        let stmt = Statement::Select(SelectStatement {
            column_ids: Some(vec![999]),
            table_name_id: table_id,
            where_clause_id: None,
        });

        // then it fails
        assert!(ast.add_statement(stmt).is_err());
    }

    #[test]
    fn ast_add_binary_expression_with_invalid_id() {
        // given a new AST and a valid left literal node
        let mut ast = Ast::new();
        let left = add_literal(&mut ast, Literal::I32(1), Type::I32);

        // when adding a binary expression with an invalid right id
        let expr = Expression::Binary(BinaryExpressionNode {
            left_id: left,
            right_id: 1234,
            op: BinaryOperator::Plus,
            ty: Type::I32,
        });

        // then it fails
        assert!(ast.add_node(expr).is_err());
    }

    #[test]
    fn ast_add_function_call() {
        // given a new AST, a function identifier, and two argument literals
        let mut ast = Ast::new();
        let func_id = add_identifier(&mut ast, "SUM", Type::I32);
        let arg1 = add_literal(&mut ast, Literal::I32(10), Type::I32);
        let arg2 = add_literal(&mut ast, Literal::I32(20), Type::I32);

        // when adding a function call node with those arguments
        let call_id = ast
            .add_node(Expression::FunctionCall(FunctionCallNode {
                identifier_id: func_id,
                argument_ids: vec![arg1, arg2],
                ty: Type::I32,
            }))
            .unwrap();

        // then the node is present and has the correct arguments
        let expr = ast.node(call_id).unwrap();
        match expr {
            Expression::FunctionCall(node) => {
                assert_eq!(node.argument_ids.len(), 2);
            }
            _ => panic!("Expected FunctionCallNode"),
        }
    }
}

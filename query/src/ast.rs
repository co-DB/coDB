//! AST module - definition of coSQL syntax tree nodes and statements.

use std::fmt;

use metadata::types::Type;
use thiserror::Error;

use crate::operators::{BinaryOperator, LogicalOperator, UnaryOperator};

/// Error for [`Ast`] related operations.
#[derive(Error, Debug)]
pub(crate) enum AstError {
    #[error("invalid node type(expected {expected}, got: {got})")]
    InvalidNodeType { expected: String, got: String },
}

/// [`Ast`] represents query as list of statements ([`Ast::statements`]). Each statement is built using nodes defined in [`Ast::nodes`].
///
/// When executing, statements should be run in order of appearance.
#[derive(Default)]
pub(crate) struct Ast {
    pub(crate) nodes: Vec<Expression>,
    pub(crate) statements: Vec<Statement>,
}

impl Ast {
    /// Returns all statements stored in [`Ast`]. They should be executed in the order of appearance.
    pub(crate) fn statements(&self) -> &[Statement] {
        &self.statements
    }

    /// Returns node with `node_id`.
    pub(crate) fn node(&self, node_id: NodeId) -> &Expression {
        &self.nodes[node_id.0]
    }

    /// Adds node to [`Ast`] and returns its id.
    pub(crate) fn add_node(&mut self, node: Expression) -> NodeId {
        self.nodes.push(node);
        NodeId::new(self.nodes.len() - 1)
    }

    /// Adds statement to [`Ast`] and returns its id.
    pub(crate) fn add_statement(&mut self, statement: Statement) -> NodeId {
        self.statements.push(statement);
        NodeId::new(self.statements.len() - 1)
    }

    /// Returns [`IdentifierNode`] with id `indentifier_id`.
    /// If `identifier_id` points to a node that is not a [`IdentifierNode`] an error is returned.
    pub(crate) fn identifier(&self, identifier_id: NodeId) -> Result<&IdentifierNode, AstError> {
        let identifier_expression = self.node(identifier_id);
        match identifier_expression {
            Expression::Identifier(id) => Ok(id),
            _ => Err(AstError::InvalidNodeType {
                expected: "Identifier".into(),
                got: format!("{identifier_expression}"),
            }),
        }
    }
}

/// [`NodeId`] is used for indexing nodes and statements inside [`Ast`].
///
/// It's a wrapper around `usize`, but thanks to it being our custom type, fact that it can only be created inside `ast` ([`NodeId::new`] is private)
/// and a fact that we don't allow to remove nodes once added to [`Ast`] we can assume that each [`NodeId`] is correct and don't need to validate it each time we want to add new node.
#[derive(Debug, Clone, Copy)]
pub struct NodeId(usize);

impl NodeId {
    fn new(id: usize) -> Self {
        NodeId(id)
    }
}

#[derive(Debug)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Create(CreateStatement),
    Alter(AlterStatement),
    Truncate(TruncateStatement),
    Drop(DropStatement),
}

#[derive(Debug)]
pub struct SelectStatement {
    pub columns: Option<Vec<NodeId>>,
    pub table_name: NodeId,
    pub where_clause: Option<NodeId>,
}

#[derive(Debug)]
pub struct InsertStatement {
    pub table_name: NodeId,
    pub columns: Option<Vec<NodeId>>,
    pub values: Vec<NodeId>,
}

#[derive(Debug)]
pub struct UpdateStatement {
    pub table_name: NodeId,
    pub column_setters: Vec<(NodeId, NodeId)>,
    pub where_clause: Option<NodeId>,
}

#[derive(Debug)]
pub struct DeleteStatement {
    pub table_name: NodeId,
    pub where_clause: Option<NodeId>,
}

#[derive(Debug)]
pub struct CreateStatement {
    pub table_name: NodeId,
    pub columns: Vec<CreateColumnDescriptor>,
}

#[derive(Debug)]
pub struct CreateColumnDescriptor {
    pub name: NodeId,
    pub ty: Type,
    pub addon: CreateColumnAddon,
}

#[derive(Debug, PartialEq, Eq)]
pub enum CreateColumnAddon {
    PrimaryKey,
    None,
}

#[derive(Debug)]
pub struct AlterStatement {
    pub table_name: NodeId,
    pub action: AlterAction,
}

#[derive(Debug)]
pub enum AlterAction {
    Add(AddAlterAction),
    RenameColumn(RenameColumnAlterAction),
    RenameTable(RenameTableAlterAction),
    Drop(DropAlterAction),
}

#[derive(Debug)]
pub struct AddAlterAction {
    pub column_name: NodeId,
    pub column_type: Type,
}

#[derive(Debug)]
pub struct RenameColumnAlterAction {
    pub previous_name: NodeId,
    pub new_name: NodeId,
}

#[derive(Debug)]
pub struct RenameTableAlterAction {
    pub new_name: NodeId,
}

#[derive(Debug)]
pub struct DropAlterAction {
    pub column_name: NodeId,
}

#[derive(Debug)]
pub struct TruncateStatement {
    pub table_name: NodeId,
}

#[derive(Debug)]
pub struct DropStatement {
    pub table_name: NodeId,
}

#[derive(Debug)]
pub enum Literal {
    String(String),
    Float(f64),
    Int(i64),
    Bool(bool),
}

#[derive(Debug)]
pub enum Expression {
    Logical(LogicalExpressionNode),
    Binary(BinaryExpressionNode),
    Unary(UnaryExpressionNode),
    FunctionCall(FunctionCallNode),
    Literal(LiteralNode),
    Identifier(IdentifierNode),
    TableIdentifier(TableIdentifierNode),
    ColumnIdentifier(ColumnIdentifierNode),
}

impl fmt::Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expression::Logical(_) => write!(f, "Logical"),
            Expression::Binary(_) => write!(f, "Binary"),
            Expression::Unary(_) => write!(f, "Unary"),
            Expression::FunctionCall(_) => write!(f, "FunctionCall"),
            Expression::Literal(_) => write!(f, "Literal"),
            Expression::Identifier(_) => write!(f, "Identifier"),
            Expression::TableIdentifier(_) => write!(f, "TableIdentifier"),
            Expression::ColumnIdentifier(_) => write!(f, "ColumnIdentifier"),
        }
    }
}

#[derive(Debug)]
pub struct LogicalExpressionNode {
    pub left_id: NodeId,
    pub right_id: NodeId,
    pub op: LogicalOperator,
}

#[derive(Debug)]
pub struct BinaryExpressionNode {
    pub left_id: NodeId,
    pub right_id: NodeId,
    pub op: BinaryOperator,
}

#[derive(Debug)]
pub struct UnaryExpressionNode {
    pub expression_id: NodeId,
    pub op: UnaryOperator,
}

#[derive(Debug)]
pub struct FunctionCallNode {
    pub identifier_id: NodeId,
    pub argument_ids: Vec<NodeId>,
}

#[derive(Debug)]
pub struct LiteralNode {
    pub value: Literal,
}

#[derive(Debug)]
pub struct IdentifierNode {
    pub value: String,
}

#[derive(Debug)]
pub struct TableIdentifierNode {
    pub identifier: NodeId,
    pub alias: Option<NodeId>,
}

#[derive(Debug)]
pub struct ColumnIdentifierNode {
    pub identifier: NodeId,
    pub table_alias: Option<NodeId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to create a simple identifier node and return its id
    fn add_identifier(ast: &mut Ast, name: &str) -> NodeId {
        ast.add_node(Expression::Identifier(IdentifierNode {
            value: name.to_string(),
        }))
    }

    // Helper to create a literal node and return its id
    fn add_literal(ast: &mut Ast, lit: Literal) -> NodeId {
        ast.add_node(Expression::Literal(LiteralNode { value: lit }))
    }

    #[test]
    fn ast_add_identifier_node() {
        // given a new AST and an identifier node
        let mut ast = Ast::default();
        let id = add_identifier(&mut ast, "foo");

        // when retrieving the node by id
        let expr = ast.node(id);

        // then it is the expected identifier node
        match expr {
            Expression::Identifier(node) => assert_eq!(node.value, "foo"),
            _ => panic!("Expected IdentifierNode"),
        }
    }

    #[test]
    fn ast_add_binary_expression() {
        // given a new AST and two literal nodes
        let mut ast = Ast::default();
        let left = add_literal(&mut ast, Literal::Int(1));
        let right = add_literal(&mut ast, Literal::Int(2));

        // when adding a binary expression
        let expr_id = ast.add_node(Expression::Binary(BinaryExpressionNode {
            left_id: left,
            right_id: right,
            op: BinaryOperator::Plus,
        }));

        // then the node is present and correct
        let expr = ast.node(expr_id);
        match expr {
            Expression::Binary(node) => {
                assert_eq!(node.left_id.0, left.0);
                assert_eq!(node.right_id.0, right.0);
            }
            _ => panic!("Expected BinaryExpressionNode"),
        }
    }

    #[test]
    fn ast_add_function_call() {
        // given a new AST, a function identifier, and two argument literals
        let mut ast = Ast::default();
        let func_id = add_identifier(&mut ast, "SUM");
        let arg1 = add_literal(&mut ast, Literal::Int(10));
        let arg2 = add_literal(&mut ast, Literal::Int(20));

        // when adding a function call node with those arguments
        let call_id = ast.add_node(Expression::FunctionCall(FunctionCallNode {
            identifier_id: func_id,
            argument_ids: vec![arg1, arg2],
        }));

        // then the node is present and has the correct arguments
        let expr = ast.node(call_id);
        match expr {
            Expression::FunctionCall(node) => {
                assert_eq!(node.argument_ids.len(), 2);
                assert_eq!(node.argument_ids[0].0, arg1.0);
                assert_eq!(node.argument_ids[1].0, arg2.0);
            }
            _ => panic!("Expected FunctionCallNode"),
        }
    }
}

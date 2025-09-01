//! AST module - definition of coSQL syntax tree nodes and statements.

/// [`Ast`] represents query as list of statements ([`Ast::statements`]). Each statement is built using nodes defined in [`Ast::nodes`]. Every statement inserted to [`Ast`] is guaranteed to have every node id valid - user of this module does not need to assert it.
///
/// When executing, statements should be run in order of appearance.
pub struct Ast {
    pub nodes: Vec<Expression>,
    pub statements: Vec<Statement>,
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

    /// Returns node with `node_id`.
    pub fn node(&self, node_id: NodeId) -> &Expression {
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
}

impl Default for Ast {
    fn default() -> Self {
        Self::new()
    }
}

/// [`NodeId`] is used for indexing nodes inside [`Ast`].
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
    pub addon: Option<CreateColumnAddon>,
}

#[derive(Debug)]
pub enum CreateColumnAddon {
    PrimaryKey,
}

#[derive(Debug)]
pub struct AlterStatement {
    pub table_name: NodeId,
    pub action: AlterAction,
}

#[derive(Debug)]
pub enum AlterAction {
    Add(AddAlterAction),
    Rename(RenameAlterAction),
    Drop(DropAlterAction),
}

#[derive(Debug)]
pub struct AddAlterAction {
    pub column_name: NodeId,
    pub column_type: Type,
}

#[derive(Debug)]
pub struct RenameAlterAction {
    pub previous_name: NodeId,
    pub new_name: NodeId,
    pub ty: RenameType,
}

#[derive(Debug)]
pub enum RenameType {
    Table,
    Column,
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
pub enum Type {
    Int32,
    Int64,
    Float32,
    Float64,
    Bool,
    String,
    Date,
    DateTime,
}

#[derive(Debug)]
pub enum Expression {
    Logical(LogicalExpressionNode),
    Binary(BinaryExpressionNode),
    Unary(UnaryExpressionNode),
    FunctionCall(FunctionCallNode),
    Literal(LiteralNode),
    Identifier(IdentifierNode),
}

#[derive(Debug)]
pub enum LogicalOperator {
    And,
    Or,
}

#[derive(Debug)]
pub struct LogicalExpressionNode {
    pub left_id: NodeId,
    pub right_id: NodeId,
    pub op: LogicalOperator,
}

#[derive(Debug)]
pub enum BinaryOperator {
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
pub struct BinaryExpressionNode {
    pub left_id: NodeId,
    pub right_id: NodeId,
    pub op: BinaryOperator,
}

#[derive(Debug)]
pub enum UnaryOperator {
    Plus,
    Minus,
    Bang,
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
        let mut ast = Ast::new();
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
        let mut ast = Ast::new();
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
        let mut ast = Ast::new();
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

    #[test]
    fn ast_add_select_statement() {
        // given a new AST and valid column/table nodes
        let mut ast = Ast::new();
        let col_id = add_identifier(&mut ast, "col");
        let table_id = add_identifier(&mut ast, "table");

        // when adding a valid select statement
        let stmt = Statement::Select(SelectStatement {
            columns: Some(vec![col_id]),
            table_name: table_id,
            where_clause: None,
        });

        // then it is added to the AST
        ast.add_statement(stmt);
        assert_eq!(ast.statements().len(), 1);
    }

    #[test]
    fn ast_add_insert_statement() {
        // given a new AST, a table node, column nodes, and value nodes
        let mut ast = Ast::new();
        let table_id = add_identifier(&mut ast, "users");
        let col_id1 = add_identifier(&mut ast, "id");
        let col_id2 = add_identifier(&mut ast, "name");
        let val_id1 = add_literal(&mut ast, Literal::Int(1));
        let val_id2 = add_literal(&mut ast, Literal::String("Alice".to_string()));

        // when adding an insert statement
        let stmt = Statement::Insert(InsertStatement {
            table_name: table_id,
            columns: Some(vec![col_id1, col_id2]),
            values: vec![val_id1, val_id2],
        });

        // then it is added to the AST
        ast.add_statement(stmt);
        assert_eq!(ast.statements().len(), 1);
    }

    #[test]
    fn ast_add_update_statement() {
        // given a new AST, a table node, column/value nodes, and a where clause
        let mut ast = Ast::new();
        let table_id = add_identifier(&mut ast, "users");
        let col_id = add_identifier(&mut ast, "name");
        let val_id = add_literal(&mut ast, Literal::String("Bob".to_string()));
        let where_id = add_literal(&mut ast, Literal::Int(1));

        // when adding an update statement
        let stmt = Statement::Update(UpdateStatement {
            table_name: table_id,
            column_setters: vec![(col_id, val_id)],
            where_clause: Some(where_id),
        });

        // then it is added to the AST
        ast.add_statement(stmt);
        assert_eq!(ast.statements().len(), 1);
    }

    #[test]
    fn ast_add_delete_statement() {
        // given a new AST, a table node, and a where clause
        let mut ast = Ast::new();
        let table_id = add_identifier(&mut ast, "users");
        let where_id = add_literal(&mut ast, Literal::Int(1));

        // when adding a delete statement
        let stmt = Statement::Delete(DeleteStatement {
            table_name: table_id,
            where_clause: Some(where_id),
        });

        // then it is added to the AST
        ast.add_statement(stmt);
        assert_eq!(ast.statements().len(), 1);
    }
}

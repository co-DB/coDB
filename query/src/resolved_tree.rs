use metadata::types::Type;

use crate::operators::{BinaryOperator, LogicalOperator, UnaryOperator};

/// [`ResolvedTree`] is a semantically analyzed version of [`Ast`].
#[derive(Default, Debug)]
pub(crate) struct ResolvedTree {
    pub(crate) nodes: Vec<ResolvedExpression>,
    pub(crate) statements: Vec<ResolvedStatement>,
}

impl ResolvedTree {
    pub(crate) fn add_node(&mut self, node: ResolvedExpression) -> ResolvedNodeId {
        self.nodes.push(node);
        ResolvedNodeId::new(self.nodes.len() - 1)
    }

    pub(crate) fn add_statement(&mut self, statement: ResolvedStatement) -> ResolvedNodeId {
        self.statements.push(statement);
        ResolvedNodeId::new(self.statements.len() - 1)
    }

    pub(crate) fn node(&self, id: ResolvedNodeId) -> &ResolvedExpression {
        &self.nodes[id.0]
    }

    pub(crate) fn statement(&self, id: ResolvedNodeId) -> &ResolvedStatement {
        &self.statements[id.0]
    }

    pub(crate) fn statements(&self) -> &[ResolvedStatement] {
        &self.statements
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ResolvedNodeId(usize);

impl ResolvedNodeId {
    fn new(id: usize) -> Self {
        ResolvedNodeId(id)
    }
}

#[derive(Debug)]
pub(crate) enum ResolvedStatement {
    Select(ResolvedSelectStatement),
}

#[derive(Debug)]
pub(crate) struct ResolvedSelectStatement {
    pub(crate) table: ResolvedNodeId,
    pub(crate) columns: Vec<ResolvedNodeId>,
    pub(crate) where_clause: Option<ResolvedNodeId>,
}

#[derive(Debug)]
pub(crate) enum ResolvedExpression {
    TableRef(ResolvedTable),
    ColumnRef(ResolvedColumn),
    FunctionRef(ResolvedFunction),
    Logical(ResolvedLogicalExpression),
    Binary(ResolvedBinaryExpression),
    Unary(ResolvedUnaryExpression),
    Literal(ResolvedLiteral),
}

#[derive(Debug)]
pub(crate) struct ResolvedTable {
    pub(crate) name: String,
    pub(crate) primary_key_name: String,
}

#[derive(Debug)]
pub(crate) struct ResolvedColumn {
    pub(crate) name: String,
    pub(crate) ty: Type,
}

#[derive(Debug)]
pub(crate) struct ResolvedFunction {
    pub(crate) name: String,
    pub(crate) args: Vec<ResolvedNodeId>,
    pub(crate) output_ty: Type,
}

#[derive(Debug)]
pub(crate) struct ResolvedLogicalExpression {
    pub(crate) left: ResolvedNodeId,
    pub(crate) right: ResolvedNodeId,
    pub(crate) op: LogicalOperator,
}

#[derive(Debug)]
pub(crate) struct ResolvedBinaryExpression {
    pub(crate) left: ResolvedNodeId,
    pub(crate) right: ResolvedNodeId,
    pub(crate) op: BinaryOperator,
    pub(crate) ty: Type,
}

#[derive(Debug)]
pub(crate) struct ResolvedUnaryExpression {
    pub(crate) expression: ResolvedNodeId,
    pub(crate) op: UnaryOperator,
    pub(crate) ty: Type,
}

#[derive(Debug)]
pub(crate) enum ResolvedLiteral {
    String(String),
    Float32(f32),
    Float64(f64),
    Int32(i32),
    Int64(i64),
    Bool(bool),
    // TODO:
    // Date()
    // DateTime
}

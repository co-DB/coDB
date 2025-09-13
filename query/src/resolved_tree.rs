use std::fmt;

use metadata::types::Type;
use time::{Date, PrimitiveDateTime};

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
    Cast(ResolvedCast),
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
pub(crate) struct ResolvedCast {
    pub(crate) child: ResolvedNodeId,
    pub(crate) new_ty: Type,
}

#[derive(Debug)]
pub(crate) enum ResolvedLiteral {
    String(String),
    Float32(f32),
    Float64(f64),
    Int32(i32),
    Int64(i64),
    Bool(bool),
    Date(Date),
    DateTime(PrimitiveDateTime),
}

impl From<&ResolvedLiteral> for Type {
    fn from(value: &ResolvedLiteral) -> Self {
        match value {
            ResolvedLiteral::String(_) => Type::String,
            ResolvedLiteral::Float32(_) => Type::F32,
            ResolvedLiteral::Float64(_) => Type::F64,
            ResolvedLiteral::Int32(_) => Type::I32,
            ResolvedLiteral::Int64(_) => Type::I64,
            ResolvedLiteral::Bool(_) => Type::Bool,
            ResolvedLiteral::Date(_) => Type::Date,
            ResolvedLiteral::DateTime(_) => Type::DateTime,
        }
    }
}

#[derive(PartialEq, Eq)]
pub(crate) enum ResolvedType {
    LiteralType(Type),
    TableRef,
}

impl ResolvedExpression {
    pub(crate) fn resolved_type(&self) -> ResolvedType {
        match self {
            ResolvedExpression::TableRef(_) => ResolvedType::TableRef,
            ResolvedExpression::ColumnRef(cr) => ResolvedType::LiteralType(cr.ty),
            ResolvedExpression::FunctionRef(resolved_function) => {
                ResolvedType::LiteralType(resolved_function.output_ty)
            }
            ResolvedExpression::Logical(_) => ResolvedType::LiteralType(Type::Bool),
            ResolvedExpression::Binary(resolved_binary_expression) => {
                ResolvedType::LiteralType(resolved_binary_expression.ty)
            }
            ResolvedExpression::Unary(resolved_unary_expression) => {
                ResolvedType::LiteralType(resolved_unary_expression.ty)
            }
            ResolvedExpression::Literal(resolved_literal) => {
                ResolvedType::LiteralType(resolved_literal.into())
            }
            ResolvedExpression::Cast(resolved_cast) => {
                ResolvedType::LiteralType(resolved_cast.new_ty)
            }
        }
    }
}

impl fmt::Display for ResolvedType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResolvedType::LiteralType(ty) => write!(f, "{ty}"),
            ResolvedType::TableRef => write!(f, "TableRef"),
        }
    }
}

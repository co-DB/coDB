use std::fmt::{self, Display};

use metadata::types::Type;
use time::{Date, PrimitiveDateTime};

use crate::{
    ast::OrderDirection,
    operators::{BinaryOperator, LogicalOperator, UnaryOperator},
};

/// [`ResolvedTree`] is a semantically analyzed version of [`Ast`].
#[derive(Default, Debug)]
pub struct ResolvedTree {
    pub(crate) nodes: Vec<ResolvedExpression>,
    pub(crate) statements: Vec<ResolvedStatement>,
}

impl ResolvedTree {
    /// Adds node to [`ResolvedTree`] and returns its [`ResolvedNodeId`].
    pub(crate) fn add_node(&mut self, node: ResolvedExpression) -> ResolvedNodeId {
        self.nodes.push(node);
        ResolvedNodeId::new(self.nodes.len() - 1)
    }

    /// Adds statement to [`ResolvedTree`] and returns its [`ResolvedNodeId`].
    pub(crate) fn add_statement(&mut self, statement: ResolvedStatement) -> ResolvedNodeId {
        self.statements.push(statement);
        ResolvedNodeId::new(self.statements.len() - 1)
    }

    /// Returns node with `node_id`.
    pub fn node(&self, id: ResolvedNodeId) -> &ResolvedExpression {
        &self.nodes[id.0]
    }

    /// Returns statement with `node_id`.
    pub(crate) fn statement(&self, id: ResolvedNodeId) -> &ResolvedStatement {
        &self.statements[id.0]
    }

    /// Returns all statements.
    pub(crate) fn statements(&self) -> &[ResolvedStatement] {
        &self.statements
    }
}

/// [`ResolvedNodeId`] is used for indexing nodes and statements inside [`ResolvedTree`].
///
/// Works similarly to [`NodeId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ResolvedNodeId(usize);

impl ResolvedNodeId {
    fn new(id: usize) -> Self {
        ResolvedNodeId(id)
    }
}

#[derive(Debug)]
pub(crate) enum ResolvedStatement {
    Select(ResolvedSelectStatement),
    Insert(ResolvedInsertStatement),
    Update(ResolvedUpdateStatement),
    Delete(ResolvedDeleteStatement),
    Create(ResolvedCreateStatement),
    AlterAddColumn(ResolvedAlterAddColumnStatement),
    AlterRenameColumn(ResolvedAlterRenameColumnStatement),
    AlterRenameTable(ResolvedAlterRenameTableStatement),
    AlterDropColumn(ResolvedAlterDropColumnStatement),
    Truncate(ResolvedTruncateStatement),
    Drop(ResolvedDropStatement),
}

#[derive(Debug)]
pub(crate) struct ResolvedSelectStatement {
    pub(crate) table: ResolvedNodeId,
    pub(crate) columns: Vec<ResolvedNodeId>,
    pub(crate) where_clause: Option<ResolvedNodeId>,
    pub(crate) order_by: Option<ResolvedOrderByDetails>,
    pub(crate) limit: Option<u32>,
    pub(crate) offset: Option<u32>,
}

#[derive(Debug)]
pub(crate) struct ResolvedInsertStatement {
    pub(crate) table: ResolvedNodeId,
    pub(crate) columns: Vec<ResolvedNodeId>,
    pub(crate) values: Vec<ResolvedNodeId>,
}

#[derive(Debug)]
pub(crate) struct ResolvedUpdateStatement {
    pub(crate) table: ResolvedNodeId,
    pub(crate) columns: Vec<ResolvedNodeId>,
    pub(crate) values: Vec<ResolvedNodeId>,
    pub(crate) where_clause: Option<ResolvedNodeId>,
}

#[derive(Debug)]
pub(crate) struct ResolvedDeleteStatement {
    pub(crate) table: ResolvedNodeId,
    pub(crate) where_clause: Option<ResolvedNodeId>,
}

#[derive(Debug)]
pub(crate) struct ResolvedCreateStatement {
    pub(crate) table_name: String,
    pub(crate) primary_key_column: ResolvedCreateColumnDescriptor,
    pub(crate) columns: Vec<ResolvedCreateColumnDescriptor>,
}

#[derive(Debug)]
pub(crate) struct ResolvedAlterAddColumnStatement {
    pub(crate) table: ResolvedNodeId,
    pub(crate) column_name: String,
    pub(crate) column_type: Type,
}

#[derive(Debug)]
pub(crate) struct ResolvedAlterRenameColumnStatement {
    pub(crate) table: ResolvedNodeId,
    pub(crate) column: ResolvedNodeId,
    pub(crate) new_name: String,
}

#[derive(Debug)]
pub(crate) struct ResolvedAlterRenameTableStatement {
    pub(crate) table: ResolvedNodeId,
    pub(crate) new_name: String,
}

#[derive(Debug)]
pub(crate) struct ResolvedAlterDropColumnStatement {
    pub(crate) table: ResolvedNodeId,
    pub(crate) column: ResolvedNodeId,
}

#[derive(Debug)]
pub(crate) struct ResolvedTruncateStatement {
    pub(crate) table: ResolvedNodeId,
}

#[derive(Debug)]
pub(crate) struct ResolvedDropStatement {
    pub(crate) table: ResolvedNodeId,
}

#[derive(Debug, Clone)]
pub struct ResolvedCreateColumnDescriptor {
    pub name: String,
    pub ty: Type,
    pub addon: ResolvedCreateColumnAddon,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum ResolvedCreateColumnAddon {
    PrimaryKey,
    None,
}

impl ResolvedCreateColumnAddon {
    pub(crate) fn unique_per_table(&self) -> bool {
        match self {
            ResolvedCreateColumnAddon::PrimaryKey => true,
            ResolvedCreateColumnAddon::None => false,
        }
    }
}

impl Display for ResolvedCreateColumnAddon {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResolvedCreateColumnAddon::PrimaryKey => write!(f, "PRIMARY_KEY"),
            ResolvedCreateColumnAddon::None => write!(f, "<EMPTY_ADDON>"),
        }
    }
}

#[derive(Debug)]
pub struct ResolvedOrderByDetails {
    pub column: ResolvedNodeId,
    pub direction: OrderDirection,
}

#[derive(Debug)]
pub enum ResolvedExpression {
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
pub struct ResolvedTable {
    pub name: String,
    pub primary_key_name: String,
}

#[derive(Debug)]
pub struct ResolvedColumn {
    pub table: ResolvedNodeId,
    pub name: String,
    pub ty: Type,
    pub pos: u16,
}

#[derive(Debug)]
pub struct ResolvedFunction {
    pub name: String,
    pub args: Vec<ResolvedNodeId>,
    pub output_ty: Type,
}

#[derive(Debug)]
pub struct ResolvedLogicalExpression {
    pub left: ResolvedNodeId,
    pub right: ResolvedNodeId,
    pub op: LogicalOperator,
}

#[derive(Debug)]
pub struct ResolvedBinaryExpression {
    pub left: ResolvedNodeId,
    pub right: ResolvedNodeId,
    pub op: BinaryOperator,
    pub ty: Type,
}

#[derive(Debug)]
pub struct ResolvedUnaryExpression {
    pub expression: ResolvedNodeId,
    pub op: UnaryOperator,
    pub ty: Type,
}

#[derive(Debug)]
pub struct ResolvedCast {
    pub child: ResolvedNodeId,
    pub new_ty: Type,
}

#[derive(Debug)]
pub enum ResolvedLiteral {
    String(String),
    Float32(f32),
    Float64(f64),
    Int32(i32),
    Int64(i64),
    Bool(bool),
    Date(Date),
    DateTime(PrimitiveDateTime),
}

/// Used for one-to-one mapping between [`ResolvedLiteral`] and [`Type`].
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
    /// Returns [`ResolvedType`] of given [`ResolvedExpression`].
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

use std::{collections::HashMap, fmt::Display, sync::Arc};

use metadata::{
    catalog::{Catalog, CatalogError, ColumnMetadata, TableMetadata, TableMetadataError},
    types::Type,
};
use parking_lot::RwLock;
use thiserror::Error;

use crate::{
    ast::{
        Ast, AstError, BinaryExpressionNode, Expression, FunctionCallNode, IdentifierNode, Literal,
        LiteralNode, LogicalExpressionNode, NodeId, SelectStatement, Statement,
        UnaryExpressionNode,
    },
    operators::{BinaryOperator, SupportsType},
    resolved_tree::{
        ResolvedBinaryExpression, ResolvedCast, ResolvedColumn, ResolvedExpression,
        ResolvedLiteral, ResolvedLogicalExpression, ResolvedNodeId, ResolvedSelectStatement,
        ResolvedStatement, ResolvedTable, ResolvedTree, ResolvedType, ResolvedUnaryExpression,
    },
};

/// Error for [`Analyzer`] related operations.
#[derive(Debug, Error)]
pub(crate) enum AnalyzerError {
    #[error("table '{table}' was not found in database")]
    TableNotFound { table: String },
    #[error("column '{column}' was not found")]
    ColumnNotFound { column: String },
    #[error("identifier '{identifier}' is unknown and cannot be resolved")]
    UnknownIdentifier { identifier: String },
    #[error("cannot find common type for '{left}' and '{right}'")]
    CommonTypeNotFound { left: String, right: String },
    #[error("type '{ty}' cannot be used with operator '{op}'")]
    TypeNotSupportedByOperator { op: String, ty: String },
    #[error("column '{column}' found in more than one table")]
    AmbigousColumn { column: String },
    #[error("unexpected type: expected {expected}, got {got}")]
    UnexpectedType { expected: String, got: String },
    #[error("unexpected catalog error: {0}")]
    UnexpectedCatalogError(#[from] CatalogError),
    #[error("unexpected table metadata error: {0}")]
    UnexpectedTableMetadataError(#[from] TableMetadataError),
    #[error("unexpected ast error: {0}")]
    UnexpectedAstError(#[from] AstError),
}

/// Used for resolving identifiers when caller knows from context what type of identifier is expected.
enum ResolveIdentifierHint {
    ExpectedColumn,
    ExpectedTable,
}

/// Used as a key type for [`Analyzer::inserted_columns`].
#[derive(Debug, Hash, PartialEq, Eq)]
struct InsertedColumnRef {
    table_name: String,
    column_name: String,
}

#[derive(Default)]
struct StatementContext {
    /// Metadata of tables analyzed in current statement
    tables_metadata: HashMap<String, TableMetadata>,
    /// List of inserted [`TableMetadata`]s ids - used to avoid duplicating nodes.
    /// It should be used only per statement, as [`TableMetadata`] can be changed between statements.
    inserted_tables: HashMap<String, ResolvedNodeId>,
    /// Same as [`Analyzer::inserted_tables`] - used to avoid duplicating nodes.
    /// It should be used only per statement, as [`ColumnMetadata`] can be changed between statements.
    inserted_columns: HashMap<InsertedColumnRef, ResolvedNodeId>,
}

impl StatementContext {
    /// Clears all data of [`StatementContext`]. Should be used when new statement is being analyzed.
    fn clear(&mut self) {
        self.tables_metadata.clear();
        self.inserted_tables.clear();
        self.inserted_columns.clear();
    }

    fn table_metadata(&self, table_name: &str) -> Option<&TableMetadata> {
        self.tables_metadata.get(table_name)
    }

    fn add_table_metadata(&mut self, table_name: impl Into<String>, table_metadata: TableMetadata) {
        self.tables_metadata
            .insert(table_name.into(), table_metadata);
    }

    fn inserted_table(&self, table_name: &str) -> Option<&ResolvedNodeId> {
        self.inserted_tables.get(table_name)
    }

    fn insert_table(&mut self, table_name: impl Into<String>, table_id: ResolvedNodeId) {
        self.inserted_tables.insert(table_name.into(), table_id);
    }

    fn inserted_column(&self, column_ref: &InsertedColumnRef) -> Option<&ResolvedNodeId> {
        self.inserted_columns.get(column_ref)
    }

    fn insert_column(&mut self, column_ref: InsertedColumnRef, column_id: ResolvedNodeId) {
        self.inserted_columns.insert(column_ref, column_id);
    }

    fn add_new_table(
        &mut self,
        table_name: impl Into<String> + AsRef<str>,
        table_metadata: TableMetadata,
        table_id: ResolvedNodeId,
    ) {
        self.insert_table(table_name.as_ref(), table_id);
        self.add_table_metadata(table_name.into(), table_metadata);
    }
}

/// [`Analyzer`] is responsible for performing semantic analysis on [`Ast`]. As the result it produces the [`ResolvedTree`].
pub(crate) struct Analyzer<'a> {
    /// [`Ast`] being analyzed.
    ast: &'a Ast,
    /// Reference to database catalog, which provides table and column metadatas.
    catalog: Arc<RwLock<Catalog>>,
    /// [`ResolvedTree`] being built - ouput of [`Analyzer`] work.
    resolved_tree: ResolvedTree,
    /// Details about currently analyzed statement (e.g. already resolved tables and columns).
    statement_context: StatementContext,
}

impl<'a> Analyzer<'a> {
    /// Creates new [`Analyzer`] for given [`Ast`].
    pub(crate) fn new(ast: &'a Ast, catalog: Arc<RwLock<Catalog>>) -> Self {
        Analyzer {
            ast,
            catalog,
            resolved_tree: ResolvedTree::default(),
            statement_context: StatementContext::default(),
        }
    }

    /// Analyzes [`Ast`] and returns resolved version of it - [`ResolvedTree`].
    pub(crate) fn analyze(mut self) -> Result<ResolvedTree, AnalyzerError> {
        for statement in self.ast.statements() {
            self.statement_context.clear();
            self.analyze_statement(statement)?;
        }
        Ok(self.resolved_tree)
    }

    /// Clears (resets to default) [`Analyzer`] statement-level fields.
    /// Should be called before analyzing statement.

    /// Analyzes statement. If statement was successfully analyzed then its resolved version ([`ResolvedStatement`]) is added to [`Analyzer::resolved_tree`].
    /// When analyzing statement it's important to make sure that tables identifiers are resolved first,
    /// as resolving column identifiers require information from their tables.
    fn analyze_statement(&mut self, statement: &Statement) -> Result<(), AnalyzerError> {
        match statement {
            Statement::Select(select) => self.analyze_select_statement(select),
            Statement::Insert(insert) => todo!(),
            Statement::Update(update) => todo!(),
            Statement::Delete(delete) => todo!(),
            Statement::Create(create) => todo!(),
            Statement::Alter(alter) => todo!(),
            Statement::Truncate(truncate) => todo!(),
            Statement::Drop(drop) => todo!(),
        }
    }

    /// Analyzes select statement.
    /// If successful [`ResolvedSelectStatement`] is added to [`Analyzer::resolved_tree`].
    fn analyze_select_statement(&mut self, select: &SelectStatement) -> Result<(), AnalyzerError> {
        let resolved_table = self.resolve_table(select.table_name)?;
        let resolved_columns = match &select.columns {
            Some(columns) => self.resolve_columns(columns)?,
            None => self.resolve_all_columns_from_table(resolved_table)?,
        };
        let resolved_where_clause = select
            .where_clause
            .map(|node_id| self.resolve_expression(node_id))
            .transpose()?;
        let select_statement = ResolvedSelectStatement {
            table: resolved_table,
            columns: resolved_columns,
            where_clause: resolved_where_clause,
        };
        self.resolved_tree
            .add_statement(ResolvedStatement::Select(select_statement));
        Ok(())
    }

    /// Resolves table with identifier id `table_name`.
    /// If successful updates [`Analyzer::tables_context`] and [`Analyzer::inserted_tables`].
    fn resolve_table(&mut self, table_name: NodeId) -> Result<ResolvedNodeId, AnalyzerError> {
        let table_identifier = self.ast.identifier(table_name)?;
        self.resolve_identifier(table_identifier, Some(ResolveIdentifierHint::ExpectedTable))
    }

    /// Resolves column with identifier id `column_name`.
    /// If successful updates [`Analyzer::inserted_columns`].
    fn resolve_column(&mut self, column_name: NodeId) -> Result<ResolvedNodeId, AnalyzerError> {
        let column_identifier = self.ast.identifier(column_name)?;
        self.resolve_identifier(
            column_identifier,
            Some(ResolveIdentifierHint::ExpectedColumn),
        )
    }

    /// Resolves list of collumns.
    /// If successful updates [`Analyzer::inserted_columns`].
    fn resolve_columns(
        &mut self,
        columns: &[NodeId],
    ) -> Result<Vec<ResolvedNodeId>, AnalyzerError> {
        let mut resolved_columns = Vec::with_capacity(columns.len());
        for column in columns {
            let resolved_column = self.resolve_column(*column)?;
            resolved_columns.push(resolved_column);
        }
        Ok(resolved_columns)
    }

    /// Resolves all columns from table with `table_id`.
    /// If successful updates [`Analyzer::inserted_columns`].
    fn resolve_all_columns_from_table(
        &mut self,
        table_id: ResolvedNodeId,
    ) -> Result<Vec<ResolvedNodeId>, AnalyzerError> {
        let table_node = self.resolved_tree.node(table_id);
        let table_name = match table_node {
            ResolvedExpression::TableRef(table) => table.name.clone(),
            _ => {
                return Err(AnalyzerError::UnexpectedType {
                    expected: "TableRef".into(),
                    got: table_node.resolved_type().to_string(),
                });
            }
        };
        let tm = self
            .statement_context
            .table_metadata(&table_name)
            .expect("table inserted to 'resolved_tree' but not to `inserted_tables`");
        let columns: Vec<_> = tm.columns().collect();
        let mut resolved_columns = Vec::with_capacity(columns.len());
        for column in columns {
            let key = InsertedColumnRef {
                column_name: column.name().into(),
                table_name: table_name.clone(),
            };
            if let Some(already_inserted) = self.statement_context.inserted_column(&key) {
                resolved_columns.push(*already_inserted);
                continue;
            }
            let resolved_column = ResolvedColumn {
                table: table_id,
                name: column.name().into(),
                ty: column.ty(),
                pos: column.pos(),
            };
            let resolved_column_id = self
                .resolved_tree
                .add_node(ResolvedExpression::ColumnRef(resolved_column));
            self.statement_context
                .insert_column(key, resolved_column_id);
            resolved_columns.push(resolved_column_id);
        }
        Ok(resolved_columns)
    }

    /// Resolves ast [`Expression`] and returns [`ResolvedNodeId`] of its analyzed version ([`ResolvedExpression`]).
    fn resolve_expression(
        &mut self,
        expression_id: NodeId,
    ) -> Result<ResolvedNodeId, AnalyzerError> {
        let expression = self.ast.node(expression_id);
        match expression {
            Expression::Logical(logical_expression_node) => {
                self.resolve_logical_expression(logical_expression_node)
            }
            Expression::Binary(binary_expression_node) => {
                self.resolve_binary_expression(binary_expression_node)
            }
            Expression::Unary(unary_expression_node) => {
                self.resolve_unary_expression(unary_expression_node)
            }
            Expression::FunctionCall(function_call_node) => {
                self.resolve_function_call(function_call_node)
            }
            Expression::Literal(literal_node) => Ok(self.resolve_literal(literal_node)),
            Expression::Identifier(identifier_node) => {
                self.resolve_identifier(identifier_node, None)
            }
            Expression::TableIdentifier(table_identifier_node) => todo!(),
            Expression::ColumnIdentifier(column_identifier_node) => todo!(),
        }
    }

    fn resolve_logical_expression(
        &mut self,
        logical_expression: &LogicalExpressionNode,
    ) -> Result<ResolvedNodeId, AnalyzerError> {
        let left_resolved = self.resolve_expression(logical_expression.left_id)?;
        self.assert_resolved_type(left_resolved, ResolvedType::LiteralType(Type::Bool))?;
        let right_resolved = self.resolve_expression(logical_expression.right_id)?;
        self.assert_resolved_type(right_resolved, ResolvedType::LiteralType(Type::Bool))?;
        let resolved = ResolvedLogicalExpression {
            left: left_resolved,
            right: right_resolved,
            op: logical_expression.op,
        };
        Ok(self
            .resolved_tree
            .add_node(ResolvedExpression::Logical(resolved)))
    }

    fn resolve_binary_expression(
        &mut self,
        binary_expression: &BinaryExpressionNode,
    ) -> Result<ResolvedNodeId, AnalyzerError> {
        let left_resolved = self.resolve_expression(binary_expression.left_id)?;
        let right_resolved = self.resolve_expression(binary_expression.right_id)?;
        let common_type = self.get_common_type(left_resolved, right_resolved)?;
        let left_resolved = self.resolve_cast(left_resolved, common_type)?;
        let right_resolved = self.resolve_cast(right_resolved, common_type)?;
        self.assert_type_and_operator_compatible(&common_type, &binary_expression.op)?;
        let final_type = self.binary_expression_type(&binary_expression.op, &common_type);
        let resolved = ResolvedBinaryExpression {
            left: left_resolved,
            right: right_resolved,
            op: binary_expression.op,
            ty: final_type,
        };
        Ok(self
            .resolved_tree
            .add_node(ResolvedExpression::Binary(resolved)))
    }

    /// Returns type of binary expression based on its operator.
    fn binary_expression_type(&self, op: &BinaryOperator, args_type: &Type) -> Type {
        match op {
            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Star
            | BinaryOperator::Slash
            | BinaryOperator::Modulo => *args_type,
            _ => Type::Bool,
        }
    }

    fn resolve_unary_expression(
        &mut self,
        unary_expression: &UnaryExpressionNode,
    ) -> Result<ResolvedNodeId, AnalyzerError> {
        let child_resolved = self.resolve_expression(unary_expression.expression_id)?;
        let ty = self.assert_not_table_ref(child_resolved)?;
        self.assert_type_and_operator_compatible(&ty, &unary_expression.op)?;
        let resolved = ResolvedUnaryExpression {
            expression: child_resolved,
            op: unary_expression.op,
            ty,
        };
        Ok(self
            .resolved_tree
            .add_node(ResolvedExpression::Unary(resolved)))
    }

    // TODO: should be implemented once functions are added to coDB
    fn resolve_function_call(
        &mut self,
        function_call: &FunctionCallNode,
    ) -> Result<ResolvedNodeId, AnalyzerError> {
        todo!()
    }

    fn resolve_literal(&mut self, literal: &LiteralNode) -> ResolvedNodeId {
        match &literal.value {
            Literal::String(value) => self.resolve_string_literal(value.clone()),
            Literal::Float(value) => self.resolve_float_literal(*value),
            Literal::Int(value) => self.resolve_int_literal(*value),
            Literal::Bool(value) => self.resolve_bool_literal(*value),
        }
    }

    fn resolve_string_literal(&mut self, value: String) -> ResolvedNodeId {
        let resolved = ResolvedLiteral::String(value);
        self.resolved_tree
            .add_node(ResolvedExpression::Literal(resolved))
    }

    fn resolve_float_literal(&mut self, value: f64) -> ResolvedNodeId {
        let can_fit_in_f32 = value > f32::MIN as f64 && value < f32::MAX as f64;
        let resolved = if can_fit_in_f32 {
            ResolvedLiteral::Float32(value as f32)
        } else {
            ResolvedLiteral::Float64(value)
        };
        self.resolved_tree
            .add_node(ResolvedExpression::Literal(resolved))
    }

    fn resolve_int_literal(&mut self, value: i64) -> ResolvedNodeId {
        let can_fit_in_i32 = value > i32::MIN as i64 && value < i32::MAX as i64;
        let resolved = if can_fit_in_i32 {
            ResolvedLiteral::Int32(value as i32)
        } else {
            ResolvedLiteral::Int64(value)
        };
        self.resolved_tree
            .add_node(ResolvedExpression::Literal(resolved))
    }

    fn resolve_bool_literal(&mut self, value: bool) -> ResolvedNodeId {
        let resolved = ResolvedLiteral::Bool(value);
        self.resolved_tree
            .add_node(ResolvedExpression::Literal(resolved))
    }

    fn resolve_identifier(
        &mut self,
        identifier: &IdentifierNode,
        hint: Option<ResolveIdentifierHint>,
    ) -> Result<ResolvedNodeId, AnalyzerError> {
        if let Some(hint) = hint {
            match hint {
                ResolveIdentifierHint::ExpectedColumn => {
                    return self.resolve_column_identifier(identifier);
                }
                ResolveIdentifierHint::ExpectedTable => {
                    return self.resolve_table_identifier(identifier);
                }
            }
        }
        let resolved_column = self.resolve_column_identifier(identifier);
        if resolved_column.is_ok() {
            return resolved_column;
        }
        let resolved_table = self.resolve_table_identifier(identifier);
        if resolved_table.is_ok() {
            return resolved_table;
        }
        Err(AnalyzerError::UnknownIdentifier {
            identifier: identifier.value.clone(),
        })
    }

    /// Resolves column pointed by `identifier`.
    /// If more than one column matches `identifier` (meaning there are two tables with same column name
    /// and alises weren't used) then error is returned.
    fn resolve_column_identifier(
        &mut self,
        identifier: &IdentifierNode,
    ) -> Result<ResolvedNodeId, AnalyzerError> {
        for (table_name, tm) in &self.statement_context.tables_metadata {
            let key = InsertedColumnRef {
                column_name: identifier.value.clone(),
                table_name: table_name.clone(),
            };

            // [`Analyzer::tables_context`] was out of sync with [`Analyzer::inserted_tables`].
            // It should never happen, it can only happen in case of programmer mistake and
            // there is nothing else to do here than just panic.
            let resolved_table = self
                .statement_context
                .inserted_table(table_name)
                .expect("table inserted to 'tables_context' but not to `inserted_tables`");

            if let Some(already_inserted) = self.statement_context.inserted_column(&key) {
                self.assert_column_not_ambigous(&identifier.value, *resolved_table)?;

                return Ok(*already_inserted);
            }

            let column_metadata = self.get_column_metadata(tm, &identifier.value);
            if let Ok(cm) = column_metadata {
                self.assert_column_not_ambigous(&identifier.value, *resolved_table)?;
                let resolved_column = ResolvedColumn {
                    table: *resolved_table,
                    name: cm.name().into(),
                    ty: cm.ty(),
                    pos: cm.pos(),
                };
                let resolved_column_id = self
                    .resolved_tree
                    .add_node(ResolvedExpression::ColumnRef(resolved_column));
                self.statement_context
                    .insert_column(key, resolved_column_id);
                return Ok(resolved_column_id);
            }
        }
        return Err(AnalyzerError::ColumnNotFound {
            column: identifier.value.clone(),
        });
    }

    /// Resolves identifier pointing to table and updates [`Analyzer::tables_context`] and [`Analyzer::inserted_tables`].
    fn resolve_table_identifier(
        &mut self,
        identifier: &IdentifierNode,
    ) -> Result<ResolvedNodeId, AnalyzerError> {
        if let Some(alread_inserted) = self.statement_context.inserted_table(&identifier.value) {
            return Ok(*alread_inserted);
        }
        let table_name = identifier.value.clone();
        let table_metadata = self.get_table_metadata(&table_name)?;
        let resolved = ResolvedTable {
            name: table_name.clone(),
            primary_key_name: table_metadata.primary_key_column_name().into(),
        };
        let resolved_node_id = self
            .resolved_tree
            .add_node(ResolvedExpression::TableRef(resolved));
        self.statement_context
            .add_new_table(table_name, table_metadata, resolved_node_id);
        Ok(resolved_node_id)
    }

    fn resolve_cast(
        &mut self,
        child: ResolvedNodeId,
        new_ty: Type,
    ) -> Result<ResolvedNodeId, AnalyzerError> {
        let ty = self.assert_not_table_ref(child)?;
        if ty == new_ty {
            return Ok(child);
        }
        let resolved = ResolvedCast { child, new_ty };
        Ok(self
            .resolved_tree
            .add_node(ResolvedExpression::Cast(resolved)))
    }

    fn get_table_metadata(&self, table_name: &str) -> Result<TableMetadata, AnalyzerError> {
        match self.catalog.read().table(table_name) {
            Ok(tm) => Ok(tm),
            Err(CatalogError::TableNotFound(_)) => Err(AnalyzerError::TableNotFound {
                table: table_name.into(),
            }),
            Err(e) => Err(AnalyzerError::UnexpectedCatalogError(e)),
        }
    }

    fn get_column_metadata(
        &self,
        table_metadata: &TableMetadata,
        column_name: &str,
    ) -> Result<ColumnMetadata, AnalyzerError> {
        match table_metadata.column(column_name) {
            Ok(cm) => Ok(cm),
            Err(TableMetadataError::ColumnNotFound(_)) => Err(AnalyzerError::ColumnNotFound {
                column: column_name.into(),
            }),
            Err(e) => Err(AnalyzerError::UnexpectedTableMetadataError(e)),
        }
    }

    /// Tries to find a common type for `left` and `right`.
    /// If such type does not exist error is returned.
    fn get_common_type(
        &self,
        left: ResolvedNodeId,
        right: ResolvedNodeId,
    ) -> Result<Type, AnalyzerError> {
        let left_ty = self.assert_not_table_ref(left)?;
        let right_ty = self.assert_not_table_ref(right)?;
        Type::coercion(&left_ty, &right_ty).ok_or(AnalyzerError::CommonTypeNotFound {
            left: left_ty.to_string(),
            right: right_ty.to_string(),
        })
    }

    /// Checks if node pointed by `id` has the same [`ResolvedType`] as `expected_type`.
    fn assert_resolved_type(
        &self,
        id: ResolvedNodeId,
        expected_type: ResolvedType,
    ) -> Result<(), AnalyzerError> {
        let resolved_type = self.resolved_tree.node(id).resolved_type();
        let err = AnalyzerError::UnexpectedType {
            expected: expected_type.to_string(),
            got: resolved_type.to_string(),
        };
        match expected_type {
            ResolvedType::LiteralType(expected_ty) => {
                let ResolvedType::LiteralType(ty) = resolved_type else {
                    return Err(err);
                };
                if expected_ty != ty {
                    return Err(err);
                }
            }
            ResolvedType::TableRef => {
                if resolved_type != ResolvedType::TableRef {
                    return Err(err);
                }
            }
        }
        Ok(())
    }

    /// Checks if node pointed by `id` has [`ResolvedType`] that is not [`ResolvedType::TableRef`].
    /// In such case underlying [`ResolvedType::LiteralType`] is returned.
    fn assert_not_table_ref(&self, id: ResolvedNodeId) -> Result<Type, AnalyzerError> {
        let resolved_type = self.resolved_tree.node(id).resolved_type();
        match resolved_type {
            ResolvedType::LiteralType(ty) => Ok(ty),
            ResolvedType::TableRef => Err(AnalyzerError::UnexpectedType {
                expected: "any literal or column".into(),
                got: resolved_type.to_string(),
            }),
        }
    }

    /// Checks if provided `ty` and `op` are compatible.
    fn assert_type_and_operator_compatible(
        &self,
        ty: &Type,
        op: &(impl SupportsType + Display),
    ) -> Result<(), AnalyzerError> {
        if !op.supports_type(ty) {
            return Err(AnalyzerError::TypeNotSupportedByOperator {
                op: op.to_string(),
                ty: ty.to_string(),
            });
        }
        Ok(())
    }

    // TODO: test if it works as expected once JOINs are implemented
    fn assert_column_not_ambigous(
        &self,
        column_identifier: &str,
        expected_table: ResolvedNodeId,
    ) -> Result<(), AnalyzerError> {
        for (table_name, tm) in &self.statement_context.tables_metadata {
            if tm.column(column_identifier).is_ok() {
                let table_id = self
                    .statement_context
                    .inserted_table(table_name)
                    .expect("table inserted to 'tables_context' but not to `inserted_tables`");
                if *table_id != expected_table {
                    return Err(AnalyzerError::AmbigousColumn {
                        column: column_identifier.into(),
                    });
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Ast, Expression, IdentifierNode, SelectStatement, Statement};
    use crate::operators::{BinaryOperator, LogicalOperator, UnaryOperator};
    use metadata::catalog::Catalog;
    use metadata::types::Type;
    use parking_lot::RwLock;
    use std::sync::Arc;
    use tempfile::TempDir;

    // Helper to create a catalog file with a users table
    fn catalog_with_users() -> Arc<RwLock<Catalog>> {
        let tmp = TempDir::new().unwrap();
        let db_file = tmp.path().join("testdb");

        let json = r#"
        {
            "tables": [
                {
                    "name": "users",
                    "columns": [
                        { "name": "id", "ty": "I32", "pos": 0, "base_offset": 0, "base_offset_pos": 0 },
                        { "name": "name", "ty": "String", "pos": 1, "base_offset": 4, "base_offset_pos": 1 }
                    ],
                    "primary_key_column_name": "id"
                }
            ]
        }
        "#;
        std::fs::write(&db_file, json).unwrap();

        let catalog = Catalog::new(tmp.path(), "testdb").unwrap();
        Arc::new(RwLock::new(catalog))
    }

    // Helper to create an ast "SELECT id, name FROM users"
    fn build_select_ast() -> Ast {
        let mut ast = Ast::default();
        let table_name = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));
        let col_id = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "id".into(),
        }));
        let col_name = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "name".into(),
        }));
        let select = SelectStatement {
            table_name,
            columns: Some(vec![col_id, col_name]),
            where_clause: None,
        };
        ast.add_statement(Statement::Select(select));
        ast
    }

    // Helper to assert column is as expected
    fn assert_column(
        rt: &ResolvedTree,
        id: ResolvedNodeId,
        expected_name: &str,
        expected_ty: Type,
        expected_table: ResolvedNodeId,
        expected_pos: u16,
    ) {
        match rt.node(id) {
            ResolvedExpression::ColumnRef(col) => {
                assert_eq!(col.name, expected_name);
                assert_eq!(col.ty, expected_ty);
                assert_eq!(col.table, expected_table);
                assert_eq!(col.pos, expected_pos);
            }
            _ => panic!("Expected ColumnRef"),
        }
    }

    // Expressions

    #[test]
    fn resolve_binary_add_int_and_float_coercion() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        // 1 + 1.5
        let int_node = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Int(1),
        }));
        let float_node = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Float(1.5),
        }));
        let bin_node = ast.add_node(Expression::Binary(BinaryExpressionNode {
            left_id: int_node,
            right_id: float_node,
            op: BinaryOperator::Plus,
        }));

        let mut analyzer = Analyzer::new(&ast, catalog);
        let resolved_id = analyzer
            .resolve_expression(bin_node)
            .expect("binary expression should resolve");

        match analyzer.resolved_tree.node(resolved_id) {
            ResolvedExpression::Binary(b) => {
                assert_eq!(b.ty, Type::F32);

                match analyzer.resolved_tree.node(b.left) {
                    ResolvedExpression::Cast(c) => {
                        assert_eq!(c.new_ty, Type::F32);

                        match analyzer.resolved_tree.node(c.child) {
                            ResolvedExpression::Literal(ResolvedLiteral::Int32(value)) => {
                                assert_eq!(*value, 1);
                            }
                            other => panic!("expected child to be Int32 literal, got: {:?}", other),
                        }
                    }
                    other => panic!("expected left to be Cast, got: {:?}", other),
                }

                match analyzer.resolved_tree.node(b.right) {
                    ResolvedExpression::Literal(ResolvedLiteral::Float32(value)) => {
                        assert_eq!(*value, 1.5);
                    }
                    other => panic!("expected right to be Float32 literal, got: {:?}", other),
                }
            }
            other => panic!("expected Binary resolved expression, got: {:?}", other),
        }
    }

    #[test]
    fn resolve_binary_minus_string_not_supported() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        let s1 = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::String("hello".into()),
        }));
        let s2 = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::String("world".into()),
        }));
        let bin = ast.add_node(Expression::Binary(BinaryExpressionNode {
            left_id: s1,
            right_id: s2,
            op: BinaryOperator::Minus,
        }));

        let mut analyzer = Analyzer::new(&ast, catalog);
        let err = analyzer.resolve_expression(bin).unwrap_err();
        match err {
            AnalyzerError::TypeNotSupportedByOperator { op, ty } => {
                assert_eq!(op, "BinaryMinus");
                assert_eq!(ty, "String");
            }
            other => panic!("expected TypeNotSupportedByOperator, got: {:?}", other),
        }
    }

    #[test]
    fn resolve_binary_invalid_type_combination_errors() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        // "hello" + 1
        // String + Int should not have a common coercion type
        let str_node = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::String("hello".into()),
        }));
        let int_node = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Int(1),
        }));
        let bin_node = ast.add_node(Expression::Binary(BinaryExpressionNode {
            left_id: str_node,
            right_id: int_node,
            op: BinaryOperator::Plus,
        }));

        let mut analyzer = Analyzer::new(&ast, catalog);
        let err = analyzer.resolve_expression(bin_node).unwrap_err();
        match err {
            AnalyzerError::CommonTypeNotFound { left, right } => {
                assert_eq!(left, "String");
                assert_eq!(right, "Int32");
            }
            other => panic!("expected CommonTypeNotFound, got: {:?}", other),
        }
    }

    #[test]
    fn resolve_logical_expression() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        // true AND false
        let t = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Bool(true),
        }));
        let f = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Bool(false),
        }));
        let logical_node = ast.add_node(Expression::Logical(LogicalExpressionNode {
            left_id: t,
            right_id: f,
            op: LogicalOperator::And,
        }));

        let mut analyzer = Analyzer::new(&ast, catalog);
        let resolved_id = analyzer
            .resolve_expression(logical_node)
            .expect("logical expression should resolve");

        match analyzer.resolved_tree.node(resolved_id) {
            ResolvedExpression::Logical(l) => {
                match analyzer.resolved_tree.node(l.left) {
                    ResolvedExpression::Literal(ResolvedLiteral::Bool(true)) => {}
                    other => panic!("expected left bool literal true, got: {:?}", other),
                }
                match analyzer.resolved_tree.node(l.right) {
                    ResolvedExpression::Literal(ResolvedLiteral::Bool(false)) => {}
                    other => panic!("expected right bool literal false, got: {:?}", other),
                }
                assert!(matches!(l.op, LogicalOperator::And));
            }
            other => panic!("expected Logical resolved expression, got: {:?}", other),
        }
    }

    #[test]
    fn resolve_logical_expression_arg_not_bool() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();
        let one = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Int(1),
        }));
        let tr = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Bool(true),
        }));
        let invalid_logical = ast.add_node(Expression::Logical(LogicalExpressionNode {
            left_id: one,
            right_id: tr,
            op: LogicalOperator::And,
        }));

        let mut analyzer = Analyzer::new(&ast, catalog);
        let err = analyzer.resolve_expression(invalid_logical).unwrap_err();
        match err {
            AnalyzerError::UnexpectedType { expected, got } => {
                assert_eq!(expected, "Bool");
                assert_ne!(got, "Bool");
            }
            other => panic!("expected UnexpectedType, got: {:?}", other),
        }
    }

    #[test]
    fn literal_expression_int_and_float_bounds() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        // int boundary: i32::MAX should become Int64
        let int_max = i32::MAX as i64;
        let int_node = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Int(int_max),
        }));

        // float boundary: f32::MAX should become Float64
        let float_edge = std::f32::MAX as f64;
        let float_node = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Float(float_edge),
        }));

        let mut analyzer = Analyzer::new(&ast, catalog);

        let int_res = analyzer
            .resolve_expression(int_node)
            .expect("int literal resolve");
        match analyzer.resolved_tree.node(int_res) {
            ResolvedExpression::Literal(ResolvedLiteral::Int64(v)) => {
                assert_eq!(*v, int_max);
            }
            other => panic!("expected Int64 literal, got: {:?}", other),
        }

        let float_res = analyzer
            .resolve_expression(float_node)
            .expect("float literal resolve");
        match analyzer.resolved_tree.node(float_res) {
            ResolvedExpression::Literal(ResolvedLiteral::Float64(v)) => {
                assert_eq!(*v, float_edge);
            }
            other => panic!("expected Float64 literal, got: {:?}", other),
        }
    }

    #[test]
    fn resolve_unary_minus_numeric() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        // -5
        let five = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Int(5),
        }));
        let neg_five = ast.add_node(Expression::Unary(UnaryExpressionNode {
            expression_id: five,
            op: UnaryOperator::Minus,
        }));

        // -1.5
        let one_point_five = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Float(1.5),
        }));
        let neg_float = ast.add_node(Expression::Unary(UnaryExpressionNode {
            expression_id: one_point_five,
            op: UnaryOperator::Minus,
        }));

        let mut analyzer = Analyzer::new(&ast, catalog);

        let neg_five_res = analyzer
            .resolve_expression(neg_five)
            .expect("unary int resolve");
        match analyzer.resolved_tree.node(neg_five_res) {
            ResolvedExpression::Unary(u) => {
                assert_eq!(u.ty, Type::I32);
                assert!(matches!(u.op, UnaryOperator::Minus));
                match analyzer.resolved_tree.node(u.expression) {
                    ResolvedExpression::Literal(ResolvedLiteral::Int32(v)) => assert_eq!(*v, 5),
                    other => panic!("expected Int32 literal child, got: {:?}", other),
                }
            }
            other => panic!("expected Unary resolved expression, got: {:?}", other),
        }

        let neg_float_res = analyzer
            .resolve_expression(neg_float)
            .expect("unary float resolve");
        match analyzer.resolved_tree.node(neg_float_res) {
            ResolvedExpression::Unary(u) => {
                assert_eq!(u.ty, Type::F32);
                assert!(matches!(u.op, UnaryOperator::Minus));
                match analyzer.resolved_tree.node(u.expression) {
                    ResolvedExpression::Literal(ResolvedLiteral::Float32(v)) => {
                        assert_eq!(*v, 1.5f32)
                    }
                    other => panic!("expected Float32 literal child, got: {:?}", other),
                }
            }
            other => panic!("expected Unary resolved expression, got: {:?}", other),
        }
    }

    #[test]
    fn resolve_unary_bang_bool() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        let t = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Bool(true),
        }));
        let not_t = ast.add_node(Expression::Unary(UnaryExpressionNode {
            expression_id: t,
            op: UnaryOperator::Bang,
        }));

        let mut analyzer = Analyzer::new(&ast, catalog);
        let res = analyzer
            .resolve_expression(not_t)
            .expect("unary bang resolve");
        match analyzer.resolved_tree.node(res) {
            ResolvedExpression::Unary(u) => {
                assert_eq!(u.ty, Type::Bool);
                assert!(matches!(u.op, UnaryOperator::Bang));
                match analyzer.resolved_tree.node(u.expression) {
                    ResolvedExpression::Literal(ResolvedLiteral::Bool(v)) => assert_eq!(*v, true),
                    other => panic!("expected Bool literal child, got: {:?}", other),
                }
            }
            other => panic!("expected Unary resolved expression, got: {:?}", other),
        }
    }

    #[test]
    fn resolve_unary_minus_bool_not_supported() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        let b = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Bool(true),
        }));
        let invalid = ast.add_node(Expression::Unary(UnaryExpressionNode {
            expression_id: b,
            op: UnaryOperator::Minus,
        }));

        let mut analyzer = Analyzer::new(&ast, catalog);
        let err = analyzer.resolve_expression(invalid).unwrap_err();
        match err {
            AnalyzerError::TypeNotSupportedByOperator { op, ty } => {
                assert_eq!(op, "UnaryMinus");
                assert_eq!(ty, "Bool");
            }
            other => panic!("expected TypeNotSupportedByOperator, got: {:?}", other),
        }
    }

    // Statements

    #[test]
    fn analyze_simple_select() {
        let catalog = catalog_with_users();
        let ast = build_select_ast();
        let analyzer = Analyzer::new(&ast, catalog);
        let resolved_tree = analyzer.analyze().expect("analyze should succeed");

        assert_eq!(resolved_tree.statements.len(), 1);

        match &resolved_tree.statements[0] {
            ResolvedStatement::Select(select) => {
                // Table node
                let table_node = resolved_tree.node(select.table);
                match table_node {
                    ResolvedExpression::TableRef(tbl) => {
                        assert_eq!(tbl.name, "users");
                        assert_eq!(tbl.primary_key_name, "id");
                    }
                    _ => panic!("Expected TableRef"),
                }

                assert_eq!(select.columns.len(), 2);
                assert_column(
                    &resolved_tree,
                    select.columns[0],
                    "id",
                    Type::I32,
                    select.table,
                    0,
                );
                assert_column(
                    &resolved_tree,
                    select.columns[1],
                    "name",
                    Type::String,
                    select.table,
                    1,
                );
            }
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn analyze_select_star() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();
        let table_name = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));

        // SELECT * FROM users;
        let select = SelectStatement {
            table_name,
            columns: None,
            where_clause: None,
        };
        ast.add_statement(Statement::Select(select));

        let analyzer = Analyzer::new(&ast, catalog);
        let resolved_tree = analyzer.analyze().expect("analyze should succeed");

        assert_eq!(resolved_tree.statements.len(), 1);

        match &resolved_tree.statements[0] {
            ResolvedStatement::Select(select) => {
                assert_eq!(select.columns.len(), 2);
                assert_column(
                    &resolved_tree,
                    select.columns[0],
                    "id",
                    Type::I32,
                    select.table,
                    0,
                );
                assert_column(
                    &resolved_tree,
                    select.columns[1],
                    "name",
                    Type::String,
                    select.table,
                    1,
                );
            }
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn analyze_select_table_not_found() {
        let tmp = TempDir::new().unwrap();
        let db_file = tmp.path().join("testdb");
        std::fs::write(&db_file, r#"{ "tables": [] }"#).unwrap();
        let catalog = Catalog::new(tmp.path(), "testdb").unwrap();
        let catalog = Arc::new(RwLock::new(catalog));

        let mut ast = Ast::default();
        let table_name = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "nonexistent".into(),
        }));
        let select = SelectStatement {
            table_name,
            columns: None,
            where_clause: None,
        };
        ast.add_statement(Statement::Select(select));

        let analyzer = Analyzer::new(&ast, catalog);
        let err = analyzer.analyze().unwrap_err();
        match err {
            AnalyzerError::TableNotFound { table } => assert_eq!(table, "nonexistent"),
            _ => panic!("Expected TableNotFound"),
        }
    }

    #[test]
    fn analyze_select_column_not_found() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();
        let table_name = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));
        let col_id = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "id".into(),
        }));
        let col_fake = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "doesnotexist".into(),
        }));
        let select = SelectStatement {
            table_name,
            columns: Some(vec![col_id, col_fake]),
            where_clause: None,
        };
        ast.add_statement(Statement::Select(select));

        let analyzer = Analyzer::new(&ast, catalog);
        let err = analyzer.analyze().unwrap_err();
        match err {
            AnalyzerError::ColumnNotFound { column } => {
                assert_eq!(column, "doesnotexist");
            }
            _ => panic!("Expected ColumnNotFound"),
        }
    }

    #[test]
    fn analyze_select_where() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        // identifiers
        let table_name = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));
        let id_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "id".into(),
        }));
        let name_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "name".into(),
        }));

        // literals
        let lit_0 = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Int(0),
        }));
        let lit_1000 = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Int(1000),
        }));
        let lit_300 = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Int(300),
        }));
        let lit_codb = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::String("coDB".into()),
        }));

        // (id > 0)
        let id_gt_0 = ast.add_node(Expression::Binary(BinaryExpressionNode {
            left_id: id_ident,
            right_id: lit_0,
            op: BinaryOperator::Greater,
        }));
        // (name == "coDB")
        let name_eq_codb = ast.add_node(Expression::Binary(BinaryExpressionNode {
            left_id: name_ident,
            right_id: lit_codb,
            op: BinaryOperator::Equal,
        }));
        // left conjunct: (id > 0 AND name == "coDB")
        let left_and = ast.add_node(Expression::Logical(LogicalExpressionNode {
            left_id: id_gt_0,
            right_id: name_eq_codb,
            op: LogicalOperator::And,
        }));

        // (id < 1000)
        let id_lt_1000 = ast.add_node(Expression::Binary(BinaryExpressionNode {
            left_id: id_ident,
            right_id: lit_1000,
            op: BinaryOperator::Less,
        }));
        // (id > 300)
        let id_gt_300 = ast.add_node(Expression::Binary(BinaryExpressionNode {
            left_id: id_ident,
            right_id: lit_300,
            op: BinaryOperator::Greater,
        }));
        // right conjunct: (id < 1000 AND id > 300)
        let right_and = ast.add_node(Expression::Logical(LogicalExpressionNode {
            left_id: id_lt_1000,
            right_id: id_gt_300,
            op: LogicalOperator::And,
        }));

        // full where: (left_and) OR (right_and)
        let where_node = ast.add_node(Expression::Logical(LogicalExpressionNode {
            left_id: left_and,
            right_id: right_and,
            op: LogicalOperator::Or,
        }));

        // SELECT id FROM users WHERE (id > 0 AND name == "coDB") OR (id < 1000 AND id > 300)
        let select = SelectStatement {
            table_name,
            columns: Some(vec![id_ident]),
            where_clause: Some(where_node),
        };
        ast.add_statement(Statement::Select(select));

        let analyzer = Analyzer::new(&ast, catalog);
        let resolved_tree = analyzer.analyze().expect("analyze should succeed");

        match &resolved_tree.statements[0] {
            ResolvedStatement::Select(select) => {
                let where_id = select.where_clause.expect("where clause resolved");
                match resolved_tree.node(where_id) {
                    ResolvedExpression::Logical(top_or) => {
                        assert!(matches!(top_or.op, LogicalOperator::Or));

                        // left side: (id > 0 AND name == "coDB")
                        match resolved_tree.node(top_or.left) {
                            ResolvedExpression::Logical(left_and_res) => {
                                assert!(matches!(left_and_res.op, LogicalOperator::And));

                                // id > 0
                                match resolved_tree.node(left_and_res.left) {
                                    ResolvedExpression::Binary(b) => {
                                        assert!(matches!(b.op, BinaryOperator::Greater));
                                        match resolved_tree.node(b.left) {
                                            ResolvedExpression::ColumnRef(c) => {
                                                assert_eq!(c.name, "id");
                                            }
                                            other => panic!(
                                                "expected ColumnRef for id, got: {:?}",
                                                other
                                            ),
                                        }
                                        match resolved_tree.node(b.right) {
                                            ResolvedExpression::Literal(
                                                ResolvedLiteral::Int32(v),
                                            ) => {
                                                assert_eq!(*v, 0);
                                            }
                                            other => {
                                                panic!("expected Int32 literal 0, got: {:?}", other)
                                            }
                                        }
                                    }
                                    other => panic!("expected Binary for id > 0, got: {:?}", other),
                                }

                                // name == "coDB"
                                match resolved_tree.node(left_and_res.right) {
                                    ResolvedExpression::Binary(b) => {
                                        assert!(matches!(b.op, BinaryOperator::Equal));
                                        match resolved_tree.node(b.left) {
                                            ResolvedExpression::ColumnRef(c) => {
                                                assert_eq!(c.name, "name");
                                            }
                                            other => panic!(
                                                "expected ColumnRef for name, got: {:?}",
                                                other
                                            ),
                                        }
                                        match resolved_tree.node(b.right) {
                                            ResolvedExpression::Literal(
                                                ResolvedLiteral::String(s),
                                            ) => {
                                                assert_eq!(s, "coDB");
                                            }
                                            other => panic!(
                                                "expected String literal \"coDB\", got: {:?}",
                                                other
                                            ),
                                        }
                                    }
                                    other => panic!(
                                        "expected Binary for name == \"coDB\", got: {:?}",
                                        other
                                    ),
                                }
                            }
                            other => panic!("expected Logical (AND) on left, got: {:?}", other),
                        }

                        // right side: (id < 1000 AND id > 300)
                        match resolved_tree.node(top_or.right) {
                            ResolvedExpression::Logical(right_and_res) => {
                                assert!(matches!(right_and_res.op, LogicalOperator::And));

                                // id < 1000
                                match resolved_tree.node(right_and_res.left) {
                                    ResolvedExpression::Binary(b) => {
                                        assert!(matches!(b.op, BinaryOperator::Less));
                                        match resolved_tree.node(b.right) {
                                            ResolvedExpression::Literal(
                                                ResolvedLiteral::Int32(v),
                                            ) => {
                                                assert_eq!(*v, 1000);
                                            }
                                            other => panic!(
                                                "expected Int32 literal 1000, got: {:?}",
                                                other
                                            ),
                                        }
                                    }
                                    other => {
                                        panic!("expected Binary for id < 1000, got: {:?}", other)
                                    }
                                }

                                // id > 300
                                match resolved_tree.node(right_and_res.right) {
                                    ResolvedExpression::Binary(b) => {
                                        assert!(matches!(b.op, BinaryOperator::Greater));
                                        match resolved_tree.node(b.right) {
                                            ResolvedExpression::Literal(
                                                ResolvedLiteral::Int32(v),
                                            ) => {
                                                assert_eq!(*v, 300);
                                            }
                                            other => panic!(
                                                "expected Int32 literal 300, got: {:?}",
                                                other
                                            ),
                                        }
                                    }
                                    other => {
                                        panic!("expected Binary for id > 300, got: {:?}", other)
                                    }
                                }
                            }
                            other => panic!("expected Logical (AND) on right, got: {:?}", other),
                        }
                    }
                    other => panic!(
                        "expected top-level Logical (OR) in where clause, got: {:?}",
                        other
                    ),
                }
            }
            _ => panic!("expected select"),
        }
    }

    #[test]
    fn analyze_select_where_unknown_column() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        let table_name = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));

        let unknown_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "unknown_field".into(),
        }));

        let lit_5 = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Int(5),
        }));

        let cond = ast.add_node(Expression::Binary(BinaryExpressionNode {
            left_id: unknown_ident,
            right_id: lit_5,
            op: BinaryOperator::Equal,
        }));

        // SELECT * FROM users WHERE unknown_field == 5
        let select = SelectStatement {
            table_name,
            columns: None,
            where_clause: Some(cond),
        };
        ast.add_statement(Statement::Select(select));

        let analyzer = Analyzer::new(&ast, catalog);
        let result = analyzer.analyze();

        match result {
            Err(AnalyzerError::UnknownIdentifier { identifier }) => {
                assert_eq!(identifier, "unknown_field");
            }
            Err(e) => panic!("expected ColumnNotFound, got: {:?}", e),
            Ok(_) => panic!("expected analysis to fail for unknown column"),
        }
    }
}

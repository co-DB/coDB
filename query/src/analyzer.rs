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
    operators::SupportsType,
    resolved_tree::{
        ResolvedBinaryExpression, ResolvedCast, ResolvedColumn, ResolvedExpression,
        ResolvedLiteral, ResolvedLogicalExpression, ResolvedNodeId, ResolvedSelectStatement,
        ResolvedStatement, ResolvedTable, ResolvedTree, ResolvedType, ResolvedUnaryExpression,
    },
};

/// Error for [`Analyzer`] related operations.
#[derive(Debug, Error)]
pub(crate) enum AnalyzerError {
    #[error("table '{0}' was not found in database")]
    TableNotFound(String),
    #[error("column '{column}' was not found")]
    ColumnNotFound { column: String },
    #[error("cannot find common type for '{left}' and '{right}'")]
    CommonTypeNotFound { left: String, right: String },
    #[error("type '{ty}' cannot be use with operator '{op}'")]
    TypeNotSupportedByOperator { op: String, ty: String },
    #[error("unexpected type: expected {expected}, got {got}")]
    UnexpectedType { expected: String, got: String },
    #[error("unexpected catalog error: {0}")]
    UnexpectedCatalogError(#[from] CatalogError),
    #[error("unexpected table metadata error: {0}")]
    UnexpectedTableMetadataError(#[from] TableMetadataError),
    #[error("unexpected ast error: {0}")]
    UnexpectedAstError(#[from] AstError),
}

/// Used for resolving identifiers when caller know from context what type of identifier is expected.
enum ResolveIdentifierHint {
    ExpectedColumn,
    ExpectedTable,
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct InsertedColumnRef {
    table_name: String,
    column_name: String,
}

/// [`Analyzer`] is responsible for performing semantic analysis on [`Ast`]. As the result it produces the [`ResolvedTree`].
pub(crate) struct Analyzer<'a> {
    /// [`Ast`] being analyzed.
    ast: &'a Ast,
    /// Reference to database catalog, which provides table and column metadatas.
    catalog: Arc<RwLock<Catalog>>,
    /// [`ResolvedTree`] being built - ouput of [`Analyzer`] work.
    resolved_tree: ResolvedTree,
    /// Metadata of tables analyzed in current statement
    tables_context: HashMap<String, TableMetadata>,
    /// List of inserted [`TableMetadata`]s ids - used so that same node is not duplicated.
    /// It should be used only per statement, as [`TableMetadata`] can be changed between statements.
    inserted_tables: HashMap<String, ResolvedNodeId>,
    /// Same as [`Analyzer::inserted_tables`] - used so that same node is not inserted multiple times.
    /// It should be used only per statement, as [`ColumnMetadata`] can be changed between statements.
    inserted_columns: HashMap<InsertedColumnRef, ResolvedNodeId>,
}

impl<'a> Analyzer<'a> {
    /// Creates new [`Analyzer`] for given [`Ast`].
    pub(crate) fn new(ast: &'a Ast, catalog: Arc<RwLock<Catalog>>) -> Self {
        Analyzer {
            ast,
            catalog,
            resolved_tree: ResolvedTree::default(),
            tables_context: HashMap::new(),
            inserted_tables: HashMap::new(),
            inserted_columns: HashMap::new(),
        }
    }

    /// Analyzes [`Ast`] and returns resolved version of it - [`ResolvedTree`].
    pub(crate) fn analyze(mut self) -> Result<ResolvedTree, AnalyzerError> {
        for statement in self.ast.statements() {
            self.clear_statement_context();
            self.analyze_statement(statement)?;
        }
        Ok(self.resolved_tree)
    }

    /// Clears (resets to default) [`Analyzer`] statement-level fields.
    /// Should be called before analyzing statement.
    fn clear_statement_context(&mut self) {
        self.tables_context.clear();
        self.inserted_tables.clear();
        self.inserted_columns.clear();
    }

    /// Analyzes statement. If statement was successfully analyzed then its resolved version ([`ResolvedStatement`]) is added to [`Analyzer::resolved_tree`].
    /// When analyzing statement it's important to make sure that tables identifiers are resolved first,
    /// as resolving columnd identifiers require information from their tables.
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

    fn analyze_select_statement(&mut self, select: &SelectStatement) -> Result<(), AnalyzerError> {
        let resolved_table = self.resolve_table(select.table_name)?;

        let resolved_columns = match &select.columns {
            Some(columns) => self.resolve_columns(columns)?,
            None => todo!(),
        };

        let select_statement = ResolvedSelectStatement {
            table: resolved_table,
            columns: resolved_columns,
            // TODO: parse this
            where_clause: None,
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
            Expression::Identifier(identifier_node) => todo!(),
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
        let resolved = ResolvedBinaryExpression {
            left: left_resolved,
            right: right_resolved,
            op: binary_expression.op,
            ty: common_type,
        };
        Ok(self
            .resolved_tree
            .add_node(ResolvedExpression::Binary(resolved)))
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
            Literal::Float(value) => self.resolve_float_literat(*value),
            Literal::Int(value) => self.resolve_int_literal(*value),
            Literal::Bool(value) => self.resolve_bool_literal(*value),
        }
    }

    fn resolve_string_literal(&mut self, value: String) -> ResolvedNodeId {
        let resolved = ResolvedLiteral::String(value);
        self.resolved_tree
            .add_node(ResolvedExpression::Literal(resolved))
    }

    fn resolve_float_literat(&mut self, value: f64) -> ResolvedNodeId {
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
        // Hint was not provided, first we try column and then we try table.
        todo!()
    }

    fn resolve_column_identifier(
        &mut self,
        identifier: &IdentifierNode,
    ) -> Result<ResolvedNodeId, AnalyzerError> {
        for (table_name, tm) in &self.tables_context {
            let key = InsertedColumnRef {
                column_name: identifier.value.clone(),
                table_name: table_name.clone(),
            };
            if let Some(already_inserted) = self.inserted_columns.get(&key) {
                return Ok(*already_inserted);
            }
            let column_metadata = self.get_column_metadata(tm, &identifier.value);
            if let Ok(cm) = column_metadata {
                // [`Analyzer::tables_context`] was out of sync with [`Analyzer::inserted_tables`].
                // It should never happen, it can only happen in case of programmer mistake and
                // there is nothing else to do here than just panic.
                let resolved_table = self
                    .inserted_tables
                    .get(table_name)
                    .expect("table inserted to 'tables_context' but not to `inserted_tables`");
                let resolved_column = ResolvedColumn {
                    table: *resolved_table,
                    name: cm.name().into(),
                    ty: cm.ty(),
                    pos: cm.pos(),
                };
                let resolved_column_id = self
                    .resolved_tree
                    .add_node(ResolvedExpression::ColumnRef(resolved_column));
                self.add_to_inserted_columns(key, resolved_column_id);
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
        if let Some(alread_inserted) = self.inserted_tables.get(&identifier.value) {
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
        self.add_to_inserted_tables(&table_name, resolved_node_id);
        self.add_to_tables_context(table_metadata, table_name);
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

    fn add_to_tables_context(&mut self, table_metadata: TableMetadata, table_name: String) {
        self.tables_context.insert(table_name, table_metadata);
    }

    fn add_to_inserted_tables(&mut self, table_name: impl Into<String>, id: ResolvedNodeId) {
        self.inserted_tables.insert(table_name.into(), id);
    }

    fn add_to_inserted_columns(&mut self, key: InsertedColumnRef, id: ResolvedNodeId) {
        self.inserted_columns.insert(key, id);
    }

    fn get_table_metadata(&self, table_name: &str) -> Result<TableMetadata, AnalyzerError> {
        match self.catalog.read().table(table_name) {
            Ok(tm) => Ok(tm),
            Err(CatalogError::TableNotFound(_)) => {
                Err(AnalyzerError::TableNotFound(table_name.into()))
            }
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

    // TODO: update to reflect changes made to [`ResolvedColumn`]
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

                // Columns: collect names and types
                let cols: Vec<(String, Type)> = select
                    .columns
                    .iter()
                    .map(|&id| match resolved_tree.node(id) {
                        ResolvedExpression::ColumnRef(col) => (col.name.clone(), col.ty.clone()),
                        _ => panic!("Expected ColumnRef"),
                    })
                    .collect();

                let col_names: Vec<_> = cols.iter().map(|(n, _)| n.clone()).collect();
                let col_types: Vec<_> = cols.iter().map(|(_, t)| t.clone()).collect();

                assert_eq!(col_names, vec!["id", "name"]);
                assert_eq!(col_types, vec![Type::I32, Type::String]);
            }
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
            AnalyzerError::TableNotFound(name) => assert_eq!(name, "nonexistent"),
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
}

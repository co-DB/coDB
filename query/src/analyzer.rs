use std::{fmt::Display, sync::Arc};

use metadata::{
    catalog::{Catalog, CatalogError, ColumnMetadata, TableMetadata, TableMetadataError},
    types::Type,
};
use parking_lot::RwLock;
use thiserror::Error;

use crate::{
    ast::{
        Ast, AstError, BinaryExpressionNode, Expression, Literal, LiteralNode,
        LogicalExpressionNode, NodeId, SelectStatement, Statement,
    },
    operators::SupportsType,
    resolved_tree::{
        ResolvedBinaryExpression, ResolvedCast, ResolvedColumn, ResolvedExpression,
        ResolvedLiteral, ResolvedLogicalExpression, ResolvedNodeId, ResolvedSelectStatement,
        ResolvedStatement, ResolvedTable, ResolvedTree, ResolvedType,
    },
};

/// Error for [`Analyzer`] related operations.
#[derive(Debug, Error)]
pub(crate) enum AnalyzerError {
    #[error("table '{0}' was not found in database")]
    TableNotFound(String),
    #[error("column '{column}' was not found in table '{table}'")]
    ColumnNotFound { table: String, column: String },
    #[error("cannot find common type for '{left}' and '{right}'")]
    CommonTypeNotFound { left: String, right: String },
    #[error("type '{ty}' cannot be use with operator '{op}'")]
    TypeUnsupportedByOperator { op: String, ty: String },
    #[error("unexpected type: expected {expected}, got {got}")]
    UnexpectedType { expected: String, got: String },
    #[error("unexpected catalog error: {0}")]
    UnexpectedCatalogError(#[from] CatalogError),
    #[error("unexpected table metadata error: {0}")]
    UnexpectedTableMetadataError(#[from] TableMetadataError),
    #[error("unexpected ast error: {0}")]
    UnexpectedAstError(#[from] AstError),
}

/// [`Analyzer`] is responsible for performing semantic analysis on [`Ast`]. As the result it produces the [`ResolvedTree`].
pub(crate) struct Analyzer<'a> {
    ast: &'a Ast,
    catalog: Arc<RwLock<Catalog>>,
    resolved_tree: ResolvedTree,
}

impl<'a> Analyzer<'a> {
    pub(crate) fn new(ast: &'a Ast, catalog: Arc<RwLock<Catalog>>) -> Self {
        Analyzer {
            ast,
            catalog,
            resolved_tree: ResolvedTree::default(),
        }
    }

    pub(crate) fn analyze(mut self) -> Result<ResolvedTree, AnalyzerError> {
        for statement in self.ast.statements() {
            self.analyze_statement(statement)?;
        }
        Ok(self.resolved_tree)
    }

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
        let table_name = self.ast.identifier(select.table_name)?.to_string();
        let table_metadata = self.get_table_metadata(&table_name)?;
        let column_metadatas = match &select.columns {
            Some(cols) => self.get_columns_metadata(&table_metadata, &table_name, cols)?,
            None => table_metadata.columns().collect(),
        };
        let resolved_table = self.resolve_table(table_name, &table_metadata);
        let resolved_columns: Vec<_> = self.resolve_columns(&column_metadatas).collect();
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

    fn resolve_table(
        &mut self,
        table_name: String,
        table_metadata: &TableMetadata,
    ) -> ResolvedNodeId {
        let resolved_table = ResolvedTable {
            name: table_name,
            primary_key_name: table_metadata.primary_key_column_name().into(),
        };
        self.resolved_tree
            .add_node(ResolvedExpression::TableRef(resolved_table))
    }

    fn resolve_columns(
        &mut self,
        column_metadatas: &[ColumnMetadata],
    ) -> impl Iterator<Item = ResolvedNodeId> {
        column_metadatas.iter().map(|cm| self.resolve_column(cm))
    }

    fn resolve_column(&mut self, column_metadata: &ColumnMetadata) -> ResolvedNodeId {
        let resolved_column = ResolvedColumn {
            name: column_metadata.name().into(),
            ty: column_metadata.ty(),
        };
        self.resolved_tree
            .add_node(ResolvedExpression::ColumnRef(resolved_column))
    }

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
            Expression::Unary(unary_expression_node) => todo!(),
            Expression::FunctionCall(function_call_node) => todo!(),
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
            Err(CatalogError::TableNotFound(_)) => {
                Err(AnalyzerError::TableNotFound(table_name.into()))
            }
            Err(e) => Err(AnalyzerError::UnexpectedCatalogError(e)),
        }
    }

    fn get_columns_metadata(
        &self,
        table_metadata: &TableMetadata,
        table_name: &str,
        col_ids: &[NodeId],
    ) -> Result<Vec<ColumnMetadata>, AnalyzerError> {
        col_ids
            .iter()
            .map(|&node_id| {
                let column_name = self.ast.identifier(node_id)?;
                let column_metadata =
                    self.get_column_metadata(&table_metadata, column_name, &table_name)?;
                Ok(column_metadata)
            })
            .collect::<Result<Vec<_>, AnalyzerError>>()
    }

    fn get_column_metadata(
        &self,
        table_metadata: &TableMetadata,
        column_name: &str,
        table_name: &str,
    ) -> Result<ColumnMetadata, AnalyzerError> {
        match table_metadata.column(column_name) {
            Ok(cm) => Ok(cm),
            Err(TableMetadataError::ColumnNotFound(_)) => Err(AnalyzerError::ColumnNotFound {
                table: table_name.into(),
                column: column_name.into(),
            }),
            Err(e) => Err(AnalyzerError::UnexpectedTableMetadataError(e)),
        }
    }

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

    fn assert_type_and_operator_compatible(
        &self,
        ty: &Type,
        op: &(impl SupportsType + Display),
    ) -> Result<(), AnalyzerError> {
        if !op.supports_type(ty) {
            return Err(AnalyzerError::TypeUnsupportedByOperator {
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
    use crate::operators::{BinaryOperator, LogicalOperator};
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
            AnalyzerError::TypeUnsupportedByOperator { op, ty } => {
                assert_eq!(op, "BinaryMinus");
                assert_eq!(ty, "String");
            }
            other => panic!("expected TypeUnsupportedByOperator, got: {:?}", other),
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
            AnalyzerError::ColumnNotFound { table, column } => {
                assert_eq!(table, "users");
                assert_eq!(column, "doesnotexist");
            }
            _ => panic!("Expected ColumnNotFound"),
        }
    }
}

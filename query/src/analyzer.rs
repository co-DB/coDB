use std::sync::Arc;

use metadata::catalog::{Catalog, CatalogError, ColumnMetadata, TableMetadata, TableMetadataError};
use parking_lot::RwLock;
use thiserror::Error;

use crate::{
    ast::{Ast, AstError, NodeId, SelectStatement, Statement},
    resolved_tree::{
        ResolvedColumn, ResolvedExpression, ResolvedNodeId, ResolvedSelectStatement,
        ResolvedStatement, ResolvedTable, ResolvedTree,
    },
};

/// Error for [`Analyzer`] related operations.
#[derive(Debug, Error)]
pub(crate) enum AnalyzerError {
    #[error("table '{0}' was not found in database")]
    TableNotFound(String),
    #[error("column '{column}' was not found in table '{table}'")]
    ColumnNotFound { table: String, column: String },
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Ast, Expression, IdentifierNode, SelectStatement, Statement};
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

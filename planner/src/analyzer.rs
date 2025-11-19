use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    sync::Arc,
};

use metadata::{
    catalog::{Catalog, CatalogError, ColumnMetadata, TableMetadata, TableMetadataError},
    types::Type,
};
use parking_lot::RwLock;
use thiserror::Error;

use crate::{
    ast::{
        AddAlterAction, AlterAction, AlterStatement, Ast, AstError, BinaryExpressionNode,
        ColumnIdentifierNode, CreateColumnAddon, CreateStatement, DeleteStatement, DropAlterAction,
        DropStatement, Expression, FunctionCallNode, InsertStatement, Literal, LiteralNode,
        LogicalExpressionNode, NodeId, RenameColumnAlterAction, RenameTableAlterAction,
        SelectStatement, Statement, TableIdentifierNode, TruncateStatement, UnaryExpressionNode,
        UpdateStatement,
    },
    operators::{BinaryOperator, SupportsType},
    resolved_tree::{
        ResolvedAlterAddColumnStatement, ResolvedAlterDropColumnStatement,
        ResolvedAlterRenameColumnStatement, ResolvedAlterRenameTableStatement,
        ResolvedBinaryExpression, ResolvedCast, ResolvedColumn, ResolvedCreateColumnAddon,
        ResolvedCreateColumnDescriptor, ResolvedCreateStatement, ResolvedDeleteStatement,
        ResolvedDropStatement, ResolvedExpression, ResolvedInsertStatement, ResolvedLiteral,
        ResolvedLogicalExpression, ResolvedNodeId, ResolvedSelectStatement, ResolvedStatement,
        ResolvedTable, ResolvedTree, ResolvedTruncateStatement, ResolvedType,
        ResolvedUnaryExpression, ResolvedUpdateStatement,
    },
};

/// Error for [`Analyzer`] related operations.
#[derive(Debug, Error)]
pub(crate) enum AnalyzerError {
    #[error("table '{table}' was not found in database")]
    TableNotFound { table: String },
    #[error("column '{column}' was not found")]
    ColumnNotFound { column: String },
    #[error("cannot find common type for '{left}' and '{right}'")]
    CommonTypeNotFound { left: String, right: String },
    #[error("type '{ty}' cannot be used with operator '{op}'")]
    TypeNotSupportedByOperator { op: String, ty: String },
    #[error("column '{column}' found in more than one table")]
    AmbiguousColumn { column: String },
    #[error("table alias '{table_alias}' used for more than one table")]
    AmbiguousTableAlias { table_alias: String },
    #[error("no table with alias '{table_alias}' was found")]
    TableWithAliasNotFound { table_alias: String },
    #[error("number of columns ('{columns}') and number of values ('{values}') are not equal")]
    InsertColumnsAndValuesLenNotEqual { columns: usize, values: usize },
    #[error("cannot use type '{value_type}' for column '{column_type}'")]
    ColumnAndValueTypeDontMatch {
        column_type: String,
        value_type: String,
    },
    #[error("create column addon '{addon}' can only be used once in create table statement")]
    UniqueAddonUsedMoreThanOnce { addon: String },
    #[error("table '{table}' already exists")]
    TableAlreadyExists { table: String },
    #[error("column '{column}' already exists")]
    ColumnAlreadyExists { column: String },
    #[error("column '{column}' cannot be dropped")]
    ColumnCannotBeDropped { column: String },
    #[error("primary key missing in create table statement")]
    PrimaryKeyMissing,
    #[error("unexpected type: expected {expected}, got {got}")]
    UnexpectedType { expected: String, got: String },
    #[error("unexpected catalog error: {0}")]
    UnexpectedCatalogError(#[from] CatalogError),
    #[error("unexpected table metadata error: {0}")]
    UnexpectedTableMetadataError(#[from] TableMetadataError),
    #[error("unexpected ast error: {0}")]
    UnexpectedAstError(#[from] AstError),
}

/// Used as a key type for [`StatementContext::inserted_columns`].
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
    /// Maps table alias to its name.
    alias_to_table: HashMap<String, String>,
    /// Same as [`StatementContext::inserted_tables`] - used to avoid duplicating nodes.
    /// It should be used only per statement, as [`ColumnMetadata`] can be changed between statements.
    inserted_columns: HashMap<InsertedColumnRef, ResolvedNodeId>,
}

impl StatementContext {
    /// Clears all data of [`StatementContext`]. Should be used when a new statement is being analyzed.
    fn clear(&mut self) {
        self.tables_metadata.clear();
        self.inserted_tables.clear();
        self.alias_to_table.clear();
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

    fn alias_to_table(&self, alias: &str) -> Option<&String> {
        self.alias_to_table.get(alias)
    }

    fn inserted_column(&self, column_ref: &InsertedColumnRef) -> Option<&ResolvedNodeId> {
        self.inserted_columns.get(column_ref)
    }

    fn insert_column(&mut self, column_ref: InsertedColumnRef, column_id: ResolvedNodeId) {
        self.inserted_columns.insert(column_ref, column_id);
    }

    /// Adds new table to current context.
    /// It should be preferred way of adding new table to context, as it ensures sync between [`StatementContext`] fields.
    fn add_new_table(
        &mut self,
        table_name: &str,
        table_metadata: TableMetadata,
        table_id: ResolvedNodeId,
        table_alias: Option<String>,
    ) -> Result<(), AnalyzerError> {
        self.insert_table(table_name, table_id);
        self.add_table_metadata(table_name, table_metadata);
        if let Some(ta) = table_alias {
            let is_duplicate = self
                .alias_to_table
                .insert(ta.clone(), table_name.into())
                .is_some();
            if is_duplicate {
                return Err(AnalyzerError::AmbiguousTableAlias { table_alias: ta });
            }
        }
        Ok(())
    }
}

/// [`Analyzer`] is responsible for performing semantic analysis on [`Ast`]. As the result it produces the [`ResolvedTree`].
pub(crate) struct Analyzer<'a> {
    /// [`Ast`] being analyzed.
    ast: &'a Ast,
    /// Reference to the database catalog, which provides table and column metadatas.
    catalog: Arc<RwLock<Catalog>>,
    /// [`ResolvedTree`] being built - output of the [`Analyzer`]'s work.
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
    pub(crate) fn analyze(mut self) -> Result<ResolvedTree, Vec<AnalyzerError>> {
        let mut errors = vec![];
        for statement in self.ast.statements() {
            self.statement_context.clear();
            if let Err(e) = self.analyze_statement(statement) {
                errors.push(e);
            }
        }
        match errors.is_empty() {
            true => Ok(self.resolved_tree),
            false => Err(errors),
        }
    }

    /// Analyzes a statement. If statement is successfully analyzed then its resolved version ([`ResolvedStatement`]) is added to [`Analyzer::resolved_tree`].
    /// When analyzing a statement, it's important to make sure that table identifiers are resolved first,
    /// as resolving column identifiers requires information from their tables.
    fn analyze_statement(&mut self, statement: &Statement) -> Result<(), AnalyzerError> {
        match statement {
            Statement::Select(select) => self.analyze_select_statement(select),
            Statement::Insert(insert) => self.analyze_insert_statement(insert),
            Statement::Update(update) => self.analyze_update_statement(update),
            Statement::Delete(delete) => self.analyze_delete_statement(delete),
            Statement::Create(create) => self.analyze_create_statement(create),
            Statement::Alter(alter) => self.analyze_alter_statement(alter),
            Statement::Truncate(truncate) => self.analyze_truncate_statement(truncate),
            Statement::Drop(drop) => self.analyze_drop_statement(drop),
        }
    }

    /// Analyzes select statement.
    /// If successful [`ResolvedSelectStatement`] is added to [`Analyzer::resolved_tree`].
    fn analyze_select_statement(&mut self, select: &SelectStatement) -> Result<(), AnalyzerError> {
        let resolved_table = self.resolve_expression(select.table_name)?;
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

    /// Analyzes insert statement.
    /// If successful [`ResolvedInsertStatement`] is added to [`Analyzer::resolved_tree`].
    fn analyze_insert_statement(&mut self, insert: &InsertStatement) -> Result<(), AnalyzerError> {
        let resolved_table = self.resolve_expression(insert.table_name)?;
        let resolved_columns = match &insert.columns {
            Some(columns) => self.resolve_columns(columns)?,
            None => self.resolve_all_columns_from_table(resolved_table)?,
        };
        let resolved_values = self.resolve_expressions(&insert.values)?;
        if resolved_columns.len() != resolved_values.len() {
            return Err(AnalyzerError::InsertColumnsAndValuesLenNotEqual {
                columns: resolved_columns.len(),
                values: resolved_values.len(),
            });
        }
        let resolved_values = resolved_columns
            .iter()
            .zip(resolved_values.iter())
            .map(|(&column, &value)| self.cast_value_to_column(column, value))
            .collect::<Result<Vec<_>, _>>()?;
        let insert_statement = ResolvedInsertStatement {
            table: resolved_table,
            columns: resolved_columns,
            values: resolved_values,
        };
        self.resolved_tree
            .add_statement(ResolvedStatement::Insert(insert_statement));
        Ok(())
    }

    /// Analyzes update statement.
    /// If successful [`ResolvedUpdateStatement`] is added to [`Analyzer::resolved_tree`].
    fn analyze_update_statement(&mut self, update: &UpdateStatement) -> Result<(), AnalyzerError> {
        let resolved_table = self.resolve_expression(update.table_name)?;
        let resolved_column_setters = update
            .column_setters
            .iter()
            .map(|&(column, value)| {
                let resolved_column = self.resolve_expression(column)?;
                let resolved_value = self.resolve_expression(value)?;
                let casted_value = self.cast_value_to_column(resolved_column, resolved_value)?;
                Ok((resolved_column, casted_value))
            })
            .collect::<Result<Vec<_>, AnalyzerError>>()?;
        let (resolved_columns, resolved_values): (Vec<_>, Vec<_>) =
            resolved_column_setters.into_iter().unzip();
        let resolved_where_clause = update
            .where_clause
            .map(|node_id| self.resolve_expression(node_id))
            .transpose()?;
        let update_statement = ResolvedUpdateStatement {
            table: resolved_table,
            columns: resolved_columns,
            values: resolved_values,
            where_clause: resolved_where_clause,
        };
        self.resolved_tree
            .add_statement(ResolvedStatement::Update(update_statement));
        Ok(())
    }

    /// Analyzes delete statement.
    /// If successful [`ResolvedDeleteStatement`] is added to [`Analyzer::resolved_tree`].
    fn analyze_delete_statement(&mut self, delete: &DeleteStatement) -> Result<(), AnalyzerError> {
        let resolved_table = self.resolve_expression(delete.table_name)?;
        let resolved_where_clause = delete
            .where_clause
            .map(|node_id| self.resolve_expression(node_id))
            .transpose()?;
        let delete_statement = ResolvedDeleteStatement {
            table: resolved_table,
            where_clause: resolved_where_clause,
        };
        self.resolved_tree
            .add_statement(ResolvedStatement::Delete(delete_statement));
        Ok(())
    }

    /// Analyzes create statement.
    /// If successful [`ResolvedCreateStatement`] is added to [`Analyzer::resolved_tree`].
    fn analyze_create_statement(&mut self, create: &CreateStatement) -> Result<(), AnalyzerError> {
        let table_name = self.get_identifier_value(create.table_name)?;

        let table_exists = self.get_table_metadata(&table_name).is_ok();
        if table_exists {
            return Err(AnalyzerError::TableAlreadyExists { table: table_name });
        }

        let mut already_used_unique_addons = HashSet::new();

        let mut resolved_columns = Vec::with_capacity(create.columns.len() - 1);
        let mut primary_key_column = None;

        for col in &create.columns {
            let addon = self.transform_addon(&col.addon);
            if addon.unique_per_table() {
                if already_used_unique_addons.contains(&addon) {
                    return Err(AnalyzerError::UniqueAddonUsedMoreThanOnce {
                        addon: addon.to_string(),
                    });
                }
                already_used_unique_addons.insert(addon);
            }

            let resolved = ResolvedCreateColumnDescriptor {
                name: self.get_identifier_value(col.name)?,
                ty: col.ty,
                addon: addon,
            };

            if addon == ResolvedCreateColumnAddon::PrimaryKey {
                primary_key_column = Some(resolved)
            } else {
                resolved_columns.push(resolved);
            }
        }

        let primary_key_resolved = match primary_key_column {
            Some(col) => col,
            None => return Err(AnalyzerError::PrimaryKeyMissing),
        };

        let create_statement = ResolvedCreateStatement {
            table_name,
            primary_key_column: primary_key_resolved,
            columns: resolved_columns,
        };
        self.resolved_tree
            .add_statement(ResolvedStatement::Create(create_statement));
        Ok(())
    }

    /// Analyzes alter statement.
    /// If successful proper variant of resolved alter statement is added to [`Analyzer::resolved_tree`].
    fn analyze_alter_statement(&mut self, alter: &AlterStatement) -> Result<(), AnalyzerError> {
        let resolved_table = self.resolve_expression(alter.table_name)?;
        match &alter.action {
            AlterAction::Add(add_alter_action) => {
                self.analyze_alter_add_column_statement(resolved_table, add_alter_action)
            }
            AlterAction::RenameColumn(rename_column_alter_action) => self
                .analyze_alter_rename_column_statement(resolved_table, rename_column_alter_action),
            AlterAction::RenameTable(rename_table_alter_action) => self
                .analyzer_alter_rename_table_statement(resolved_table, rename_table_alter_action),
            AlterAction::Drop(drop_alter_action) => {
                self.analyzer_alter_drop_column_statement(resolved_table, drop_alter_action)
            }
        }
    }

    fn analyze_alter_add_column_statement(
        &mut self,
        resolved_table: ResolvedNodeId,
        add: &AddAlterAction,
    ) -> Result<(), AnalyzerError> {
        let column_name = self.get_identifier_value(add.column_name)?;

        let column_exists = self.column_exists_in_table(resolved_table, &column_name)?;
        if column_exists {
            return Err(AnalyzerError::ColumnAlreadyExists {
                column: column_name,
            });
        }

        let resolved_add = ResolvedAlterAddColumnStatement {
            table: resolved_table,
            column_name,
            column_type: add.column_type,
        };
        self.resolved_tree
            .add_statement(ResolvedStatement::AlterAddColumn(resolved_add));
        Ok(())
    }

    fn analyze_alter_rename_column_statement(
        &mut self,
        resolved_table: ResolvedNodeId,
        rename: &RenameColumnAlterAction,
    ) -> Result<(), AnalyzerError> {
        let column = self.resolve_expression(rename.previous_name)?;
        let new_name = self.get_identifier_value(rename.new_name)?;
        let column_exists = self.column_exists_in_table(resolved_table, &new_name)?;
        if column_exists {
            return Err(AnalyzerError::ColumnAlreadyExists { column: new_name });
        }

        let resolved_rename = ResolvedAlterRenameColumnStatement {
            table: resolved_table,
            column,
            new_name,
        };
        self.resolved_tree
            .add_statement(ResolvedStatement::AlterRenameColumn(resolved_rename));
        Ok(())
    }

    fn analyzer_alter_rename_table_statement(
        &mut self,
        resolved_table: ResolvedNodeId,
        rename: &RenameTableAlterAction,
    ) -> Result<(), AnalyzerError> {
        let new_name = self.get_identifier_value(rename.new_name)?;

        let table_exists = self.get_table_metadata(&new_name).is_ok();
        if table_exists {
            return Err(AnalyzerError::TableAlreadyExists { table: new_name });
        }

        let resolved_rename = ResolvedAlterRenameTableStatement {
            table: resolved_table,
            new_name,
        };
        self.resolved_tree
            .add_statement(ResolvedStatement::AlterRenameTable(resolved_rename));
        Ok(())
    }

    fn analyzer_alter_drop_column_statement(
        &mut self,
        resolved_table: ResolvedNodeId,
        drop: &DropAlterAction,
    ) -> Result<(), AnalyzerError> {
        let column = self.resolve_expression(drop.column_name)?;

        let can_be_dropped = self.column_can_be_dropped_from_table(column, resolved_table)?;
        if !can_be_dropped {
            return Err(AnalyzerError::ColumnCannotBeDropped {
                column: self.get_column_name(column)?,
            });
        }

        let resolved_drop = ResolvedAlterDropColumnStatement {
            table: resolved_table,
            column,
        };
        self.resolved_tree
            .add_statement(ResolvedStatement::AlterDropColumn(resolved_drop));
        Ok(())
    }

    /// Analyzes truncate statement.
    /// If successful [`ResolvedTruncateStatement`] is added to [`Analyzer::resolved_tree`].
    fn analyze_truncate_statement(
        &mut self,
        truncate: &TruncateStatement,
    ) -> Result<(), AnalyzerError> {
        let resolved_table = self.resolve_expression(truncate.table_name)?;
        let truncate_statement = ResolvedTruncateStatement {
            table: resolved_table,
        };
        self.resolved_tree
            .add_statement(ResolvedStatement::Truncate(truncate_statement));
        Ok(())
    }

    /// Analyzes drop statement.
    /// If successful [`ResolvedDropStatement`] is added to [`Analyzer::resolved_tree`].
    fn analyze_drop_statement(&mut self, drop: &DropStatement) -> Result<(), AnalyzerError> {
        let resolved_table = self.resolve_expression(drop.table_name)?;
        let drop_statement = ResolvedDropStatement {
            table: resolved_table,
        };
        self.resolved_tree
            .add_statement(ResolvedStatement::Drop(drop_statement));
        Ok(())
    }

    /// Resolves list of columns.
    fn resolve_columns(
        &mut self,
        columns: &[NodeId],
    ) -> Result<Vec<ResolvedNodeId>, AnalyzerError> {
        let mut resolved_columns = Vec::with_capacity(columns.len());
        for column in columns {
            let resolved_column = self.resolve_expression(*column)?;
            resolved_columns.push(resolved_column);
        }
        Ok(resolved_columns)
    }

    /// Resolves all columns from table with `table_id`.
    /// If successful updates [`StatementContext::inserted_columns`].
    fn resolve_all_columns_from_table(
        &mut self,
        table_id: ResolvedNodeId,
    ) -> Result<Vec<ResolvedNodeId>, AnalyzerError> {
        let table_name = self.get_table_name(table_id)?;
        let tm = self
            .statement_context
            .table_metadata(&table_name)
            .expect("'inserted_tables' out of sync");

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
            let id = self.add_resolved_column(key, resolved_column);
            resolved_columns.push(id);
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
            Expression::Identifier(_) => {
                unreachable!("IdentifierNode should never be reached from 'resolve_expression'")
            }
            Expression::TableIdentifier(table_identifier_node) => {
                self.resolve_table_identifier(table_identifier_node)
            }
            Expression::ColumnIdentifier(column_identifier_node) => {
                self.resolve_column_identifier(column_identifier_node)
            }
        }
    }

    /// Resolves list of [`Expression`]s.
    /// Returns list of [`ResolvedNodeId`]s of matching [`ResolvedExpression`]s or first error that was returned.
    fn resolve_expressions(
        &mut self,
        expressions: &[NodeId],
    ) -> Result<Vec<ResolvedNodeId>, AnalyzerError> {
        expressions
            .iter()
            .map(|&value| self.resolve_expression(value))
            .collect::<Result<Vec<_>, _>>()
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
        let can_fit_in_f32 = value >= f32::MIN as f64 && value <= f32::MAX as f64;
        let resolved = if can_fit_in_f32 {
            ResolvedLiteral::Float32(value as f32)
        } else {
            ResolvedLiteral::Float64(value)
        };
        self.resolved_tree
            .add_node(ResolvedExpression::Literal(resolved))
    }

    fn resolve_int_literal(&mut self, value: i64) -> ResolvedNodeId {
        let can_fit_in_i32 = value >= i32::MIN as i64 && value <= i32::MAX as i64;
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

    fn resolve_column_identifier(
        &mut self,
        column_identifier: &ColumnIdentifierNode,
    ) -> Result<ResolvedNodeId, AnalyzerError> {
        let column_name = self.get_identifier_value(column_identifier.identifier)?;
        if let Some(table_alias_id) = column_identifier.table_alias {
            let ta = self.get_identifier_value(table_alias_id)?;
            let table_name = self
                .statement_context
                .alias_to_table(&ta)
                .ok_or(AnalyzerError::TableWithAliasNotFound { table_alias: ta })?;
            // This clone is needed, because `Analyzer::resolve_column_from_table`
            // takes `&mut self` and `table_name` is borrowed from `self`.
            return self.resolve_column_from_table(&table_name.clone(), &column_name);
        }
        self.resolve_column_identifier_without_alias(&column_name)
    }

    /// Resolves column by searching for it in every table used in current statement.
    /// It will fail if such column is found in more than one table.
    fn resolve_column_identifier_without_alias(
        &mut self,
        column_name: &str,
    ) -> Result<ResolvedNodeId, AnalyzerError> {
        let table_name = self.find_table_for_column(&column_name)?;
        self.resolve_column_from_table(&table_name, &column_name)
    }

    /// Resolves column once its table is known.
    /// Other functions that resolve columns should fall back to this once they know column's table.
    fn resolve_column_from_table(
        &mut self,
        table_name: &str,
        column_name: &str,
    ) -> Result<ResolvedNodeId, AnalyzerError> {
        let key = InsertedColumnRef {
            column_name: column_name.to_string(),
            table_name: table_name.to_string(),
        };

        if let Some(already_inserted) = self.statement_context.inserted_column(&key) {
            return Ok(*already_inserted);
        }

        let tm = self
            .statement_context
            .table_metadata(table_name)
            .expect("'table_metadata' out of sync");
        let cm = self.get_column_metadata(tm, column_name)?;

        let resolved_table_id = self
            .statement_context
            .inserted_table(table_name)
            .expect("'inserted_table' out of sync");
        let resolved_column = ResolvedColumn {
            table: *resolved_table_id,
            name: cm.name().into(),
            ty: cm.ty(),
            pos: cm.pos(),
        };
        let resolved_column_id = self.add_resolved_column(key, resolved_column);
        Ok(resolved_column_id)
    }

    fn resolve_table_identifier(
        &mut self,
        table_identifier: &TableIdentifierNode,
    ) -> Result<ResolvedNodeId, AnalyzerError> {
        let table_name = self.get_identifier_value(table_identifier.identifier)?;
        if let Some(already_inserted) = self.statement_context.inserted_table(&table_name) {
            return Ok(*already_inserted);
        }
        let table_metadata = self.get_table_metadata(&table_name)?;
        let resolved = ResolvedTable {
            name: table_name.clone(),
            primary_key_name: table_metadata.primary_key_column_name().into(),
        };
        let table_alias = table_identifier
            .alias
            .map(|id| self.get_identifier_value(id))
            .transpose()?;
        self.add_resolved_table(table_alias, &table_name, table_metadata, resolved)
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

    /// Adds `resolved_table` to both [`Analyzer::resolved_tree`] and [`Analyzer::statement_context`].
    /// Tables should always be added via this function to enforce sync between analyzer fields.
    fn add_resolved_table(
        &mut self,
        table_alias: Option<String>,
        table_name: &str,
        table_metadata: TableMetadata,
        resolved_table: ResolvedTable,
    ) -> Result<ResolvedNodeId, AnalyzerError> {
        let resolved_node_id = self
            .resolved_tree
            .add_node(ResolvedExpression::TableRef(resolved_table));
        // If this fails, we may end up with a dangling pointer in the AST, but we don't care
        // because we won't use the AST in case of an error.
        self.statement_context.add_new_table(
            table_name,
            table_metadata,
            resolved_node_id,
            table_alias,
        )?;
        Ok(resolved_node_id)
    }

    /// Adds `resolved_column` to both [`Analyzer::resolved_tree`] and [`Analyzer::statement_context`].
    /// Columns should always be added via this function to enforce sync between analyzer fields.
    fn add_resolved_column(
        &mut self,
        key: InsertedColumnRef,
        resolved_column: ResolvedColumn,
    ) -> ResolvedNodeId {
        let resolved_column_id = self
            .resolved_tree
            .add_node(ResolvedExpression::ColumnRef(resolved_column));
        self.statement_context
            .insert_column(key, resolved_column_id);
        resolved_column_id
    }

    /// Gets [`TableMetadata`] from [`Analyzer::catalog`] and wraps it in an appropriate error if needed.
    fn get_table_metadata(&self, table_name: &str) -> Result<TableMetadata, AnalyzerError> {
        match self.catalog.read().table(table_name) {
            Ok(tm) => Ok(tm),
            Err(CatalogError::TableNotFound(_)) => Err(AnalyzerError::TableNotFound {
                table: table_name.into(),
            }),
            Err(e) => Err(AnalyzerError::UnexpectedCatalogError(e)),
        }
    }

    /// Gets [`ColumnMetadata`] from provided [`TableMetadata`] and wraps it in an appropriate error if needed.
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

    /// Returns the identifier as a string.
    /// Returns an error when the node with `identifier_id` is not an [`IdentifierNode`].
    fn get_identifier_value(&self, identifier_id: NodeId) -> Result<String, AnalyzerError> {
        let node = self.ast.identifier(identifier_id)?;
        Ok(node.value.clone())
    }

    /// Returns name of already resolved table.
    fn get_table_name(&self, resolved_table: ResolvedNodeId) -> Result<String, AnalyzerError> {
        let table_node = self.resolved_tree.node(resolved_table);
        match table_node {
            ResolvedExpression::TableRef(table) => Ok(table.name.clone()),
            _ => Err(AnalyzerError::UnexpectedType {
                expected: "TableRef".into(),
                got: table_node.resolved_type().to_string(),
            }),
        }
    }

    /// Returns name of already resolved column.
    fn get_column_name(&self, resolved_column: ResolvedNodeId) -> Result<String, AnalyzerError> {
        let column_node = self.resolved_tree.node(resolved_column);
        match column_node {
            ResolvedExpression::ColumnRef(column) => Ok(column.name.clone()),
            _ => Err(AnalyzerError::UnexpectedType {
                expected: "ColumnRef".into(),
                got: column_node.resolved_type().to_string(),
            }),
        }
    }

    /// Finds the table that contains the column named `column_name` in the current [`Analyzer::statement_context`].
    /// Returns an error when no column is found or when more than one column matches the name.
    ///
    /// It should be used only when the column does not use a table alias.
    fn find_table_for_column(&self, column_name: &str) -> Result<String, AnalyzerError> {
        let mut found: Option<String> = None;
        for (table_name, tm) in &self.statement_context.tables_metadata {
            if tm.column(column_name).is_ok() {
                if found.is_some() {
                    return Err(AnalyzerError::AmbiguousColumn {
                        column: column_name.into(),
                    });
                }
                found = Some(table_name.clone());
            }
        }
        found.ok_or(AnalyzerError::ColumnNotFound {
            column: column_name.into(),
        })
    }

    /// Checks if the node pointed to by `id` has the same [`ResolvedType`] as `expected_type`.
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

    /// Checks if the node pointed to by `id` has a [`ResolvedType`] that is not [`ResolvedType::TableRef`].
    /// In such a case, the underlying [`ResolvedType::LiteralType`] is returned.
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

    /// Tries to cast value to column creating new [`ResolvedCast`] if needed.
    fn cast_value_to_column(
        &mut self,
        column: ResolvedNodeId,
        value: ResolvedNodeId,
    ) -> Result<ResolvedNodeId, AnalyzerError> {
        let column_type = self.assert_not_table_ref(column)?;
        let value_type = self.assert_not_table_ref(value)?;

        // No cast is needed
        if column_type == value_type {
            return Ok(value);
        }

        let common_type = Type::coercion(&column_type, &value_type).ok_or(
            AnalyzerError::ColumnAndValueTypeDontMatch {
                column_type: column_type.to_string(),
                value_type: value_type.to_string(),
            },
        )?;

        // It means that column_type can be casted to value_type, but in this case
        // we only allow value_type to be casted.
        if column_type != common_type {
            return Err(AnalyzerError::ColumnAndValueTypeDontMatch {
                column_type: column_type.to_string(),
                value_type: value_type.to_string(),
            });
        }

        // We need to cast value to new type
        let resolved_cast = ResolvedCast {
            child: value,
            new_ty: common_type,
        };
        Ok(self
            .resolved_tree
            .add_node(ResolvedExpression::Cast(resolved_cast)))
    }

    /// Helper for transforming create column addon from [`Ast`] version into [`ResolvedTree`] version.
    fn transform_addon(&self, from_ast: &CreateColumnAddon) -> ResolvedCreateColumnAddon {
        match from_ast {
            CreateColumnAddon::PrimaryKey => ResolvedCreateColumnAddon::PrimaryKey,
            CreateColumnAddon::None => ResolvedCreateColumnAddon::None,
        }
    }

    /// Returns true if column with name `column_name` exists in resolved table with `ResolvedNodeId`.
    fn column_exists_in_table(
        &self,
        resolved_table: ResolvedNodeId,
        column_name: &str,
    ) -> Result<bool, AnalyzerError> {
        let table_name = self.get_table_name(resolved_table)?;
        let tm = self.get_table_metadata(&table_name)?;
        let column_exists = self.get_column_metadata(&tm, &column_name).is_ok();
        Ok(column_exists)
    }

    /// Returns true if column can be dropped from table
    fn column_can_be_dropped_from_table(
        &self,
        column: ResolvedNodeId,
        table: ResolvedNodeId,
    ) -> Result<bool, AnalyzerError> {
        let column_name = self.get_column_name(column)?;
        let table_name = self.get_table_name(table)?;
        let tm = self.get_table_metadata(&table_name)?;
        let pk = tm.primary_key_column_name();
        if column_name == pk {
            return Ok(false);
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{
        Ast, CreateColumnDescriptor, Expression, IdentifierNode, SelectStatement, Statement,
    };
    use crate::operators::{BinaryOperator, LogicalOperator, UnaryOperator};
    use metadata::catalog::Catalog;
    use metadata::types::Type;
    use parking_lot::RwLock;
    use std::fs;
    use std::sync::Arc;
    use tempfile::TempDir;

    const METADATA_FILE_NAME: &str = "metadata.coDB";

    // Helper to create a catalog file with a users table
    fn catalog_with_users() -> Arc<RwLock<Catalog>> {
        let tmp_dir = TempDir::new().unwrap();
        let db_dir = tmp_dir.path().join("db");
        fs::create_dir(&db_dir).unwrap();
        let db_path = db_dir.join(METADATA_FILE_NAME);

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

        fs::write(&db_path, json).unwrap();

        let catalog = Catalog::new(tmp_dir.path(), "db").unwrap();
        Arc::new(RwLock::new(catalog))
    }

    // Helper to create an ast "SELECT id, name FROM users"
    fn build_select_ast() -> Ast {
        let mut ast = Ast::default();

        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));
        let table_name = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: table_ident,
            alias: None,
        }));

        let id_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "id".into(),
        }));
        let col_id = ast.add_node(Expression::ColumnIdentifier(ColumnIdentifierNode {
            identifier: id_ident,
            table_alias: None,
        }));

        let name_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "name".into(),
        }));
        let col_name = ast.add_node(Expression::ColumnIdentifier(ColumnIdentifierNode {
            identifier: name_ident,
            table_alias: None,
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

    // Helpers for asserting node type.

    fn expect_table(rt: &ResolvedTree, id: ResolvedNodeId) -> &ResolvedTable {
        match rt.node(id) {
            ResolvedExpression::TableRef(t) => t,
            other => panic!("expected TableRef, got: {:?}", other),
        }
    }

    fn expect_column(rt: &ResolvedTree, id: ResolvedNodeId) -> &ResolvedColumn {
        match rt.node(id) {
            ResolvedExpression::ColumnRef(c) => c,
            other => panic!("expected ColumnRef, got: {:?}", other),
        }
    }

    fn expect_literal_f32(rt: &ResolvedTree, id: ResolvedNodeId) -> f32 {
        match rt.node(id) {
            ResolvedExpression::Literal(ResolvedLiteral::Float32(v)) => *v,
            other => panic!("expected Float32 literal, got: {:?}", other),
        }
    }

    fn expect_literal_f64(rt: &ResolvedTree, id: ResolvedNodeId) -> f64 {
        match rt.node(id) {
            ResolvedExpression::Literal(ResolvedLiteral::Float64(v)) => *v,
            other => panic!("expected Float64 literal, got: {:?}", other),
        }
    }

    fn expect_literal_i32(rt: &ResolvedTree, id: ResolvedNodeId) -> i32 {
        match rt.node(id) {
            ResolvedExpression::Literal(ResolvedLiteral::Int32(v)) => *v,
            other => panic!("expected Int32 literal, got: {:?}", other),
        }
    }

    fn expect_literal_bool(rt: &ResolvedTree, id: ResolvedNodeId) -> bool {
        match rt.node(id) {
            ResolvedExpression::Literal(ResolvedLiteral::Bool(v)) => *v,
            other => panic!("expected Bool literal, got: {:?}", other),
        }
    }

    fn expect_literal_string(rt: &ResolvedTree, id: ResolvedNodeId) -> String {
        match rt.node(id) {
            ResolvedExpression::Literal(ResolvedLiteral::String(s)) => s.clone(),
            other => panic!("expected String literal, got: {:?}", other),
        }
    }

    fn expect_binary(rt: &ResolvedTree, id: ResolvedNodeId) -> &ResolvedBinaryExpression {
        match rt.node(id) {
            ResolvedExpression::Binary(b) => b,
            other => panic!("expected Binary, got: {:?}", other),
        }
    }

    fn expect_logical(rt: &ResolvedTree, id: ResolvedNodeId) -> &ResolvedLogicalExpression {
        match rt.node(id) {
            ResolvedExpression::Logical(l) => l,
            other => panic!("expected Logical, got: {:?}", other),
        }
    }

    fn expect_unary(rt: &ResolvedTree, id: ResolvedNodeId) -> &ResolvedUnaryExpression {
        match rt.node(id) {
            ResolvedExpression::Unary(u) => u,
            other => panic!("expected Unary, got: {:?}", other),
        }
    }

    fn expect_cast(rt: &ResolvedTree, id: ResolvedNodeId) -> &ResolvedCast {
        match rt.node(id) {
            ResolvedExpression::Cast(c) => c,
            other => panic!("expected Cast, got: {:?}", other),
        }
    }

    fn expect_select(rt: &ResolvedTree, idx: usize) -> &ResolvedSelectStatement {
        match &rt.statements[idx] {
            ResolvedStatement::Select(s) => s,
            other => panic!("expected Select statement, got: {:?}", other),
        }
    }

    fn expect_insert(rt: &ResolvedTree, idx: usize) -> &ResolvedInsertStatement {
        match &rt.statements[idx] {
            ResolvedStatement::Insert(i) => i,
            other => panic!("expected Insert statement, got: {:?}", other),
        }
    }

    fn expect_update(rt: &ResolvedTree, idx: usize) -> &ResolvedUpdateStatement {
        match &rt.statements[idx] {
            ResolvedStatement::Update(u) => u,
            other => panic!("expected Update statement, got: {:?}", other),
        }
    }

    fn expect_delete(rt: &ResolvedTree, idx: usize) -> &ResolvedDeleteStatement {
        match &rt.statements[idx] {
            ResolvedStatement::Delete(d) => d,
            other => panic!("expected Delete statement, got: {:?}", other),
        }
    }

    fn expect_create(rt: &ResolvedTree, idx: usize) -> &ResolvedCreateStatement {
        match &rt.statements[idx] {
            ResolvedStatement::Create(c) => c,
            other => panic!("expected Create statement, got: {:?}", other),
        }
    }

    fn expect_alter_add_column(rt: &ResolvedTree, idx: usize) -> &ResolvedAlterAddColumnStatement {
        match &rt.statements[idx] {
            ResolvedStatement::AlterAddColumn(a) => a,
            other => panic!("expected AlterAddColumn statement, got: {:?}", other),
        }
    }

    fn expect_alter_rename_column(
        rt: &ResolvedTree,
        idx: usize,
    ) -> &ResolvedAlterRenameColumnStatement {
        match &rt.statements[idx] {
            ResolvedStatement::AlterRenameColumn(a) => a,
            other => panic!("expected AlterRenameColumn statement, got: {:?}", other),
        }
    }

    fn expect_alter_rename_table(
        rt: &ResolvedTree,
        idx: usize,
    ) -> &ResolvedAlterRenameTableStatement {
        match &rt.statements[idx] {
            ResolvedStatement::AlterRenameTable(a) => a,
            other => panic!("expected AlterRenameTable statement, got: {:?}", other),
        }
    }

    fn expect_alter_drop_column(
        rt: &ResolvedTree,
        idx: usize,
    ) -> &ResolvedAlterDropColumnStatement {
        match &rt.statements[idx] {
            ResolvedStatement::AlterDropColumn(a) => a,
            other => panic!("expected AlterDropColumn statement, got: {:?}", other),
        }
    }

    fn expect_truncate(rt: &ResolvedTree, idx: usize) -> &ResolvedTruncateStatement {
        match &rt.statements[idx] {
            ResolvedStatement::Truncate(t) => t,
            other => panic!("expected Truncate statement, got: {:?}", other),
        }
    }

    fn expect_drop_table(rt: &ResolvedTree, idx: usize) -> &ResolvedDropStatement {
        match &rt.statements[idx] {
            ResolvedStatement::Drop(d) => d,
            other => panic!("expected Drop statement, got: {:?}", other),
        }
    }

    // Expressions

    #[test]
    fn resolve_binary_add_f32_and_f64_coercion() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        // 1.5 + (1.5 * f32::MAX)
        let int_node = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Float(1.5),
        }));
        let f32_times_1_5 = f32::MAX as f64 * 1.5;
        let float_node = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Float(f32_times_1_5),
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

        let b = expect_binary(&analyzer.resolved_tree, resolved_id);
        assert_eq!(b.ty, Type::F64);

        let c = expect_cast(&analyzer.resolved_tree, b.left);
        assert_eq!(c.new_ty, Type::F64);

        let left_value = expect_literal_f32(&analyzer.resolved_tree, c.child);
        assert_eq!(left_value, 1.5f32);

        let right_value = expect_literal_f64(&analyzer.resolved_tree, b.right);
        assert_eq!(right_value, f32_times_1_5);
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

        let l = expect_logical(&analyzer.resolved_tree, resolved_id);
        let left_bool = expect_literal_bool(&analyzer.resolved_tree, l.left);
        let right_bool = expect_literal_bool(&analyzer.resolved_tree, l.right);
        assert!(matches!(l.op, LogicalOperator::And));
        assert_eq!(left_bool, true);
        assert_eq!(right_bool, false);
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

        // int boundary: i32::MAX should become Int32
        let int_max = i32::MAX as i64;
        let int_max_node = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Int(int_max),
        }));

        // int boundary: i32::MIN should become Int32
        let int_min = i32::MIN as i64;
        let int_min_node = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Int(int_min),
        }));

        // float boundary: f32::MAX should become Float32
        let float_max = f32::MAX as f64;
        let float_max_node = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Float(float_max),
        }));

        // float boundary: f32::MIN should become Float32
        let float_min = f32::MIN as f64;
        let float_min_node = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Float(float_min),
        }));

        let mut analyzer = Analyzer::new(&ast, catalog);

        let int_max_res = analyzer
            .resolve_expression(int_max_node)
            .expect("int32 max literal resolve");
        let int_v = expect_literal_i32(&analyzer.resolved_tree, int_max_res);
        assert_eq!(int_v, int_max as i32);

        let int_min_res = analyzer
            .resolve_expression(int_min_node)
            .expect("int32 min literal resolve");
        let int_v = expect_literal_i32(&analyzer.resolved_tree, int_min_res);
        assert_eq!(int_v, int_min as i32);

        let float_max_res = analyzer
            .resolve_expression(float_max_node)
            .expect("float32 max literal resolve");
        let float_v = expect_literal_f32(&analyzer.resolved_tree, float_max_res);
        assert_eq!(float_v, float_max as f32);

        let float_min_res = analyzer
            .resolve_expression(float_min_node)
            .expect("float32 min literal resolve");
        let float_v = expect_literal_f32(&analyzer.resolved_tree, float_min_res);
        assert_eq!(float_v, float_min as f32);
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
        let u = expect_unary(&analyzer.resolved_tree, neg_five_res);
        assert_eq!(u.ty, Type::I32);
        assert!(matches!(u.op, UnaryOperator::Minus));
        let child_i32 = expect_literal_i32(&analyzer.resolved_tree, u.expression);
        assert_eq!(child_i32, 5);

        let neg_float_res = analyzer
            .resolve_expression(neg_float)
            .expect("unary float resolve");
        let uf = expect_unary(&analyzer.resolved_tree, neg_float_res);
        assert_eq!(uf.ty, Type::F32);
        assert!(matches!(uf.op, UnaryOperator::Minus));
        let child_f32 = expect_literal_f32(&analyzer.resolved_tree, uf.expression);
        assert_eq!(child_f32, 1.5f32);
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
        let u = expect_unary(&analyzer.resolved_tree, res);
        assert_eq!(u.ty, Type::Bool);
        assert!(matches!(u.op, UnaryOperator::Bang));
        let v = expect_literal_bool(&analyzer.resolved_tree, u.expression);
        assert_eq!(v, true);
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

        let select = expect_select(&resolved_tree, 0);

        // Table node
        let tbl = expect_table(&resolved_tree, select.table);
        assert_eq!(tbl.name, "users");
        assert_eq!(tbl.primary_key_name, "id");

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

    #[test]
    fn analyze_select_star() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();
        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));
        let table_name = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: table_ident,
            alias: None,
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

        let select = expect_select(&resolved_tree, 0);

        let tbl = expect_table(&resolved_tree, select.table);
        assert_eq!(tbl.name, "users");
        assert_eq!(tbl.primary_key_name, "id");

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

    #[test]
    fn analyze_select_table_not_found() {
        let tmp_dir = TempDir::new().unwrap();
        let db_dir = tmp_dir.path().join("db");
        fs::create_dir(&db_dir).unwrap();
        let db_path = db_dir.join(METADATA_FILE_NAME);
        fs::write(&db_path, r#"{ "tables": [] }"#).unwrap();
        let catalog = Catalog::new(tmp_dir.path(), "db").unwrap();

        let catalog = Arc::new(RwLock::new(catalog));

        let mut ast = Ast::default();
        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "nonexistent".into(),
        }));
        let table_name = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: table_ident,
            alias: None,
        }));
        let select = SelectStatement {
            table_name,
            columns: None,
            where_clause: None,
        };
        ast.add_statement(Statement::Select(select));

        let analyzer = Analyzer::new(&ast, catalog);
        let errs = analyzer.analyze().unwrap_err();
        assert_eq!(errs.len(), 1);
        let err = &errs[0];
        match err {
            AnalyzerError::TableNotFound { table } => assert_eq!(table, "nonexistent"),
            _ => panic!("Expected TableNotFound"),
        }
    }

    #[test]
    fn analyze_select_column_not_found() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();
        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));
        let table_name = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: table_ident,
            alias: None,
        }));

        let id_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "id".into(),
        }));
        let col_id = ast.add_node(Expression::ColumnIdentifier(ColumnIdentifierNode {
            identifier: id_ident,
            table_alias: None,
        }));

        let fake_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "doesnotexist".into(),
        }));
        let col_fake = ast.add_node(Expression::ColumnIdentifier(ColumnIdentifierNode {
            identifier: fake_ident,
            table_alias: None,
        }));

        let select = SelectStatement {
            table_name,
            columns: Some(vec![col_id, col_fake]),
            where_clause: None,
        };
        ast.add_statement(Statement::Select(select));

        let analyzer = Analyzer::new(&ast, catalog);
        let errs = analyzer.analyze().unwrap_err();
        assert_eq!(errs.len(), 1);
        let err = &errs[0];
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

        // table identifier
        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));
        let table_name = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: table_ident,
            alias: None,
        }));

        // column identifiers
        let id_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "id".into(),
        }));
        let id_col = ast.add_node(Expression::ColumnIdentifier(ColumnIdentifierNode {
            identifier: id_ident,
            table_alias: None,
        }));

        let name_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "name".into(),
        }));
        let name_col = ast.add_node(Expression::ColumnIdentifier(ColumnIdentifierNode {
            identifier: name_ident,
            table_alias: None,
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
            left_id: id_col,
            right_id: lit_0,
            op: BinaryOperator::Greater,
        }));
        // (name == "coDB")
        let name_eq_codb = ast.add_node(Expression::Binary(BinaryExpressionNode {
            left_id: name_col,
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
            left_id: id_col,
            right_id: lit_1000,
            op: BinaryOperator::Less,
        }));
        // (id > 300)
        let id_gt_300 = ast.add_node(Expression::Binary(BinaryExpressionNode {
            left_id: id_col,
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
            columns: Some(vec![id_col]),
            where_clause: Some(where_node),
        };
        ast.add_statement(Statement::Select(select));

        let analyzer = Analyzer::new(&ast, catalog);
        let resolved_tree = analyzer.analyze().expect("analyze should succeed");

        assert_eq!(resolved_tree.statements.len(), 1);

        let select = expect_select(&resolved_tree, 0);

        let where_id = select.where_clause.expect("where clause resolved");

        // top-level OR
        let top_or = expect_logical(&resolved_tree, where_id);
        assert!(matches!(top_or.op, LogicalOperator::Or));

        // left side: (id > 0 AND name == "coDB")
        let left_and_res = expect_logical(&resolved_tree, top_or.left);
        assert!(matches!(left_and_res.op, LogicalOperator::And));

        // id > 0
        let b1 = expect_binary(&resolved_tree, left_and_res.left);
        assert!(matches!(b1.op, BinaryOperator::Greater));
        let c1 = expect_column(&resolved_tree, b1.left);
        assert_eq!(c1.name, "id");
        let v0 = expect_literal_i32(&resolved_tree, b1.right);
        assert_eq!(v0, 0);

        // name == "coDB"
        let b2 = expect_binary(&resolved_tree, left_and_res.right);
        assert!(matches!(b2.op, BinaryOperator::Equal));
        let c2 = expect_column(&resolved_tree, b2.left);
        assert_eq!(c2.name, "name");
        let s = expect_literal_string(&resolved_tree, b2.right);
        assert_eq!(s, "coDB");

        // right side: (id < 1000 AND id > 300)
        let right_and_res = expect_logical(&resolved_tree, top_or.right);
        assert!(matches!(right_and_res.op, LogicalOperator::And));

        // id < 1000
        let b3 = expect_binary(&resolved_tree, right_and_res.left);
        assert!(matches!(b3.op, BinaryOperator::Less));
        let v1000 = expect_literal_i32(&resolved_tree, b3.right);
        assert_eq!(v1000, 1000);

        // id > 300
        let b4 = expect_binary(&resolved_tree, right_and_res.right);
        assert!(matches!(b4.op, BinaryOperator::Greater));
        let v300 = expect_literal_i32(&resolved_tree, b4.right);
        assert_eq!(v300, 300);
    }

    #[test]
    fn analyze_select_with_table_alias() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));
        let table_alias_ident =
            ast.add_node(Expression::Identifier(IdentifierNode { value: "u".into() }));
        let table_name = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: table_ident,
            alias: Some(table_alias_ident),
        }));

        let id_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "id".into(),
        }));
        let col_u_id = ast.add_node(Expression::ColumnIdentifier(ColumnIdentifierNode {
            identifier: id_ident,
            table_alias: Some(table_alias_ident),
        }));

        let name_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "name".into(),
        }));
        let col_name = ast.add_node(Expression::ColumnIdentifier(ColumnIdentifierNode {
            identifier: name_ident,
            table_alias: None,
        }));

        let select = SelectStatement {
            table_name,
            columns: Some(vec![col_u_id, col_name]),
            where_clause: None,
        };

        // SELECT u.id, name FROM users AS u;
        ast.add_statement(Statement::Select(select));

        let analyzer = Analyzer::new(&ast, catalog);
        let resolved_tree = analyzer.analyze().expect("analyze should succeed");

        let select = expect_select(&resolved_tree, 0);

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

    #[test]
    fn analyze_select_table_alias_mismatch_errors() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));
        let table_alias_ident =
            ast.add_node(Expression::Identifier(IdentifierNode { value: "u".into() }));
        let table_name = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: table_ident,
            alias: Some(table_alias_ident),
        }));

        let id_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "id".into(),
        }));
        let k_ident = ast.add_node(Expression::Identifier(IdentifierNode { value: "k".into() }));
        let col_k_id = ast.add_node(Expression::ColumnIdentifier(ColumnIdentifierNode {
            identifier: id_ident,
            table_alias: Some(k_ident),
        }));

        let select = SelectStatement {
            table_name,
            columns: Some(vec![col_k_id]),
            where_clause: None,
        };

        // SELECT k.id FROM users AS u;
        ast.add_statement(Statement::Select(select));

        let analyzer = Analyzer::new(&ast, catalog);
        let errs = analyzer.analyze().unwrap_err();
        assert_eq!(errs.len(), 1);
        let err = &errs[0];
        match err {
            AnalyzerError::TableWithAliasNotFound { table_alias } => {
                assert_eq!(table_alias, "k");
            }
            other => panic!("expected TableWithAliasNotFound, got: {:?}", other),
        }
    }

    #[test]
    fn analyze_simple_insert_with_columns() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        // table identifier
        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));
        let table_name = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: table_ident,
            alias: None,
        }));

        // columns
        let id_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "id".into(),
        }));
        let col_id = ast.add_node(Expression::ColumnIdentifier(ColumnIdentifierNode {
            identifier: id_ident,
            table_alias: None,
        }));

        let name_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "name".into(),
        }));
        let col_name = ast.add_node(Expression::ColumnIdentifier(ColumnIdentifierNode {
            identifier: name_ident,
            table_alias: None,
        }));

        // values
        let val_id = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Int(1),
        }));
        let val_name = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::String("bob ferguson".into()),
        }));

        let insert = InsertStatement {
            table_name,
            columns: Some(vec![col_id, col_name]),
            values: vec![val_id, val_name],
        };
        ast.add_statement(Statement::Insert(insert));

        let analyzer = Analyzer::new(&ast, catalog);
        let rt = analyzer.analyze().expect("analyze should succeed");

        assert_eq!(rt.statements.len(), 1);
        let insert = expect_insert(&rt, 0);

        // table
        let tbl = expect_table(&rt, insert.table);
        assert_eq!(tbl.name, "users");
        assert_eq!(tbl.primary_key_name, "id");

        // columns
        assert_eq!(insert.columns.len(), 2);
        assert_column(&rt, insert.columns[0], "id", Type::I32, insert.table, 0);
        assert_column(
            &rt,
            insert.columns[1],
            "name",
            Type::String,
            insert.table,
            1,
        );

        // values
        let v0 = expect_literal_i32(&rt, insert.values[0]);
        assert_eq!(v0, 1);
        let v1 = expect_literal_string(&rt, insert.values[1]);
        assert_eq!(v1, "bob ferguson");
    }

    #[test]
    fn analyze_insert_with_column_but_without_primary_key_provided() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        // table identifier
        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));
        let table_name = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: table_ident,
            alias: None,
        }));

        // columns (only name)
        let name_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "name".into(),
        }));
        let col_name = ast.add_node(Expression::ColumnIdentifier(ColumnIdentifierNode {
            identifier: name_ident,
            table_alias: None,
        }));

        // values
        let val_name = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::String("bob".into()),
        }));

        let insert = InsertStatement {
            table_name,
            columns: Some(vec![col_name]),
            values: vec![val_name],
        };
        ast.add_statement(Statement::Insert(insert));

        let analyzer = Analyzer::new(&ast, catalog);
        let rt = analyzer.analyze().expect("analyze should succeed");

        assert_eq!(rt.statements.len(), 1);

        let insert = expect_insert(&rt, 0);

        // table
        let tbl = expect_table(&rt, insert.table);
        assert_eq!(tbl.name, "users");

        // columns
        assert_eq!(insert.columns.len(), 1);
        assert_column(
            &rt,
            insert.columns[0],
            "name",
            Type::String,
            insert.table,
            1,
        );

        // values
        let v0 = expect_literal_string(&rt, insert.values[0]);
        assert_eq!(v0, "bob");
    }

    #[test]
    fn analyze_insert_values_only_auto_analyzed_columns() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        // table identifier
        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));
        let table_name = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: table_ident,
            alias: None,
        }));

        // values
        // must be in the exact order as in metadata (user should assert it)
        let val_id = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Int(2),
        }));
        let val_name = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::String("ferguson".into()),
        }));

        let insert = InsertStatement {
            table_name,
            columns: None,
            values: vec![val_id, val_name],
        };
        ast.add_statement(Statement::Insert(insert));

        let analyzer = Analyzer::new(&ast, catalog);
        let rt = analyzer.analyze().expect("analyze should succeed");

        assert_eq!(rt.statements.len(), 1);

        let insert = expect_insert(&rt, 0);

        // table
        let tbl = expect_table(&rt, insert.table);
        assert_eq!(tbl.name, "users");

        // columns
        assert_eq!(insert.columns.len(), 2);
        assert_column(&rt, insert.columns[0], "id", Type::I32, insert.table, 0);
        assert_column(
            &rt,
            insert.columns[1],
            "name",
            Type::String,
            insert.table,
            1,
        );

        // values
        let v0 = expect_literal_i32(&rt, insert.values[0]);
        assert_eq!(v0, 2);
        let v1 = expect_literal_string(&rt, insert.values[1]);
        assert_eq!(v1, "ferguson");
    }

    #[test]
    fn analyze_insert_cast_value_to_column_type() {
        let tmp_dir = TempDir::new().unwrap();
        let db_dir = tmp_dir.path().join("db");
        fs::create_dir(&db_dir).unwrap();
        let db_path = db_dir.join(METADATA_FILE_NAME);
        let json = r#"
        {
            "tables": [
                {
                    "name": "numbers",
                    "columns": [
                        { "name": "id", "ty": "I64", "pos": 0, "base_offset": 0, "base_offset_pos": 0 }
                    ],
                    "primary_key_column_name": "id"
                }
            ]
        }
        "#;
        fs::write(&db_path, json).unwrap();
        let catalog = Catalog::new(tmp_dir.path(), "db").unwrap();
        let catalog = Arc::new(RwLock::new(catalog));

        let mut ast = Ast::default();

        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "numbers".into(),
        }));
        let table_name = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: table_ident,
            alias: None,
        }));

        let id_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "id".into(),
        }));
        let col_id = ast.add_node(Expression::ColumnIdentifier(ColumnIdentifierNode {
            identifier: id_ident,
            table_alias: None,
        }));

        let val_small = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Int(1),
        }));

        let insert = InsertStatement {
            table_name,
            columns: Some(vec![col_id]),
            values: vec![val_small],
        };
        ast.add_statement(Statement::Insert(insert));

        let analyzer = Analyzer::new(&ast, catalog);
        let rt = analyzer.analyze().expect("analyze should succeed");

        let insert = expect_insert(&rt, 0);

        let c = expect_cast(&rt, insert.values[0]);
        assert_eq!(c.new_ty, Type::I64);
        let child = expect_literal_i32(&rt, c.child);
        assert_eq!(child, 1);
    }

    #[test]
    fn analyze_insert_value_cast_disallowed_f64_to_f32() {
        let tmp_dir = TempDir::new().unwrap();
        let db_dir = tmp_dir.path().join("db");
        fs::create_dir(&db_dir).unwrap();
        let db_path = db_dir.join(METADATA_FILE_NAME);
        let json = r#"
        {
            "tables": [
                {
                    "name": "floats",
                    "columns": [
                        { "name": "v", "ty": "F32", "pos": 0, "base_offset": 0, "base_offset_pos": 0 }
                    ],
                    "primary_key_column_name": "v"
                }
            ]
        }
        "#;
        fs::write(&db_path, json).unwrap();
        let catalog = Catalog::new(tmp_dir.path(), "db").unwrap();
        let catalog = Arc::new(RwLock::new(catalog));

        let mut ast = Ast::default();

        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "floats".into(),
        }));
        let table_name = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: table_ident,
            alias: None,
        }));

        let v_ident = ast.add_node(Expression::Identifier(IdentifierNode { value: "v".into() }));
        let col_v = ast.add_node(Expression::ColumnIdentifier(ColumnIdentifierNode {
            identifier: v_ident,
            table_alias: None,
        }));

        let big_double = f32::MAX as f64 * 2.0;
        let val_big = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Float(big_double),
        }));

        let insert = InsertStatement {
            table_name,
            columns: Some(vec![col_v]),
            values: vec![val_big],
        };
        ast.add_statement(Statement::Insert(insert));

        let analyzer = Analyzer::new(&ast, catalog);
        let errs = analyzer.analyze().unwrap_err();
        assert_eq!(errs.len(), 1);
        let err = &errs[0];
        match err {
            AnalyzerError::ColumnAndValueTypeDontMatch {
                column_type,
                value_type,
            } => {
                assert_eq!(column_type, "Float32");
                assert_eq!(value_type, "Float64");
            }
            other => panic!("expected ColumnAndValueTypeDontMatch, got: {:?}", other),
        }
    }

    #[test]
    fn analyze_insert_more_values_than_columns() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        // table
        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));
        let table_name = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: table_ident,
            alias: None,
        }));

        // one column
        let id_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "id".into(),
        }));
        let col_id = ast.add_node(Expression::ColumnIdentifier(ColumnIdentifierNode {
            identifier: id_ident,
            table_alias: None,
        }));

        // two values
        let val_id = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Int(1),
        }));
        let val_name = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::String("extra".into()),
        }));

        let insert = InsertStatement {
            table_name,
            columns: Some(vec![col_id]),
            values: vec![val_id, val_name],
        };
        ast.add_statement(Statement::Insert(insert));

        let analyzer = Analyzer::new(&ast, catalog);
        let errs = analyzer.analyze().unwrap_err();
        assert_eq!(errs.len(), 1);
        let err = &errs[0];
        match err {
            AnalyzerError::InsertColumnsAndValuesLenNotEqual { columns, values } => {
                assert_eq!(*columns, 1);
                assert_eq!(*values, 2);
            }
            other => panic!(
                "expected InsertColumnsAndValuesLenNotEqual, got: {:?}",
                other
            ),
        }
    }

    #[test]
    fn analyze_update_successful() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        // table identifier
        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));
        let table_name = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: table_ident,
            alias: None,
        }));

        // column
        let name_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "name".into(),
        }));
        let col_name = ast.add_node(Expression::ColumnIdentifier(ColumnIdentifierNode {
            identifier: name_ident,
            table_alias: None,
        }));

        // value
        let val_updated = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::String("updated".into()),
        }));

        let update = UpdateStatement {
            table_name,
            column_setters: vec![(col_name, val_updated)],
            where_clause: None,
        };
        ast.add_statement(Statement::Update(update));

        let analyzer = Analyzer::new(&ast, catalog);
        let rt = analyzer.analyze().expect("analyze should succeed");

        assert_eq!(rt.statements.len(), 1);

        let update = expect_update(&rt, 0);

        // table
        let tbl = expect_table(&rt, update.table);
        assert_eq!(tbl.name, "users");

        // columns
        assert_eq!(update.columns.len(), 1);
        assert_column(
            &rt,
            update.columns[0],
            "name",
            Type::String,
            update.table,
            1,
        );

        // value
        let v0 = expect_literal_string(&rt, update.values[0]);
        assert_eq!(v0, "updated");
    }

    #[test]
    fn analyze_update_type_mismatch_errors() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        // table identifier
        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));
        let table_name = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: table_ident,
            alias: None,
        }));

        // column (i32)
        let id_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "id".into(),
        }));
        let col_id = ast.add_node(Expression::ColumnIdentifier(ColumnIdentifierNode {
            identifier: id_ident,
            table_alias: None,
        }));

        // value (f64)
        let big_double = f32::MAX as f64 * 2.0;
        let val_big = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Float(big_double),
        }));

        let update = UpdateStatement {
            table_name,
            column_setters: vec![(col_id, val_big)],
            where_clause: None,
        };
        ast.add_statement(Statement::Update(update));

        let analyzer = Analyzer::new(&ast, catalog);
        let errs = analyzer.analyze().unwrap_err();
        assert_eq!(errs.len(), 1);
        let err = &errs[0];
        match err {
            AnalyzerError::ColumnAndValueTypeDontMatch {
                column_type,
                value_type,
            } => {
                assert_eq!(column_type, "Int32");
                assert_eq!(value_type, "Float64");
            }
            other => panic!("expected ColumnAndValueTypeDontMatch, got: {:?}", other),
        }
    }

    #[test]
    fn analyze_delete_with_where_clause() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        // table identifier
        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));
        let table_name = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: table_ident,
            alias: None,
        }));

        // where id == 1
        let id_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "id".into(),
        }));
        let id_col = ast.add_node(Expression::ColumnIdentifier(ColumnIdentifierNode {
            identifier: id_ident,
            table_alias: None,
        }));
        let lit_1 = ast.add_node(Expression::Literal(LiteralNode {
            value: Literal::Int(1),
        }));
        let cond = ast.add_node(Expression::Binary(BinaryExpressionNode {
            left_id: id_col,
            right_id: lit_1,
            op: BinaryOperator::Equal,
        }));

        let delete = DeleteStatement {
            table_name,
            where_clause: Some(cond),
        };
        ast.add_statement(Statement::Delete(delete));

        let analyzer = Analyzer::new(&ast, catalog);
        let rt = analyzer.analyze().expect("analyze should succeed");

        assert_eq!(rt.statements.len(), 1);

        let delete = expect_delete(&rt, 0);

        let tbl = expect_table(&rt, delete.table);
        assert_eq!(tbl.name, "users");

        let where_id = delete.where_clause.expect("where clause resolved");
        let b = expect_binary(&rt, where_id);
        assert!(matches!(b.op, BinaryOperator::Equal));
        let c = expect_column(&rt, b.left);
        assert_eq!(c.name, "id");
        let v = expect_literal_i32(&rt, b.right);
        assert_eq!(v, 1);
    }

    #[test]
    fn analyze_delete_without_where_clause() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));
        let table_name = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: table_ident,
            alias: None,
        }));

        let delete = DeleteStatement {
            table_name,
            where_clause: None,
        };
        ast.add_statement(Statement::Delete(delete));

        let analyzer = Analyzer::new(&ast, catalog);
        let rt = analyzer.analyze().expect("analyze should succeed");

        assert_eq!(rt.statements.len(), 1);

        let delete = expect_delete(&rt, 0);

        let tbl = expect_table(&rt, delete.table);
        assert_eq!(tbl.name, "users");
        assert!(delete.where_clause.is_none());
    }

    #[test]
    fn analyze_truncate_table() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));
        let table_name = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: table_ident,
            alias: None,
        }));

        let truncate = TruncateStatement { table_name };
        ast.add_statement(Statement::Truncate(truncate));

        let analyzer = Analyzer::new(&ast, catalog);
        let rt = analyzer.analyze().expect("analyze should succeed");

        assert_eq!(rt.statements.len(), 1);

        let truncate = expect_truncate(&rt, 0);
        let tbl = expect_table(&rt, truncate.table);
        assert_eq!(tbl.name, "users");
        assert_eq!(tbl.primary_key_name, "id");
    }

    #[test]
    fn analyze_drop_table() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));
        let table_name = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: table_ident,
            alias: None,
        }));

        let drop = DropStatement { table_name };
        ast.add_statement(Statement::Drop(drop));

        let analyzer = Analyzer::new(&ast, catalog);
        let rt = analyzer.analyze().expect("analyze should succeed");

        assert_eq!(rt.statements.len(), 1);

        let drop = expect_drop_table(&rt, 0);

        let tbl = expect_table(&rt, drop.table);
        assert_eq!(tbl.name, "users");
        assert_eq!(tbl.primary_key_name, "id");
    }

    #[test]
    fn analyze_create_statement_success() {
        let tmp_dir = TempDir::new().unwrap();
        let db_dir = tmp_dir.path().join("db");
        fs::create_dir(&db_dir).unwrap();
        let db_path = db_dir.join(METADATA_FILE_NAME);
        // no tables
        fs::write(&db_path, r#"{ "tables": [] }"#).unwrap();
        let catalog = Catalog::new(tmp_dir.path(), "db").unwrap();
        let catalog = Arc::new(RwLock::new(catalog));

        let mut ast = Ast::default();

        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "my_table".into(),
        }));

        let col_a_ident =
            ast.add_node(Expression::Identifier(IdentifierNode { value: "a".into() }));
        let col_b_ident =
            ast.add_node(Expression::Identifier(IdentifierNode { value: "b".into() }));

        let col_a = CreateColumnDescriptor {
            name: col_a_ident,
            ty: Type::I32,
            addon: CreateColumnAddon::PrimaryKey,
        };
        let col_b = CreateColumnDescriptor {
            name: col_b_ident,
            ty: Type::String,
            addon: CreateColumnAddon::None,
        };

        let create = CreateStatement {
            table_name: table_ident,
            columns: vec![col_a, col_b],
        };
        ast.add_statement(Statement::Create(create));

        let analyzer = Analyzer::new(&ast, catalog);
        let rt = analyzer.analyze().expect("create analyze should succeed");

        assert_eq!(rt.statements.len(), 1);

        let create = expect_create(&rt, 0);
        assert_eq!(create.table_name, "my_table");
        assert_eq!(create.columns.len(), 1);
        assert_eq!(create.primary_key_column.name, "a");
        assert_eq!(create.primary_key_column.ty, Type::I32);
        assert_eq!(
            create.primary_key_column.addon,
            ResolvedCreateColumnAddon::PrimaryKey
        );
        assert_eq!(create.columns[0].name, "b");
        assert_eq!(create.columns[0].ty, Type::String);
        assert_eq!(create.columns[0].addon, ResolvedCreateColumnAddon::None);
    }

    #[test]
    fn analyze_create_table_already_exists() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));

        let create = CreateStatement {
            table_name: table_ident,
            columns: vec![],
        };
        ast.add_statement(Statement::Create(create));

        let analyzer = Analyzer::new(&ast, catalog);
        let errs = analyzer.analyze().unwrap_err();
        assert_eq!(errs.len(), 1);
        let err = &errs[0];
        match err {
            AnalyzerError::TableAlreadyExists { table } => assert_eq!(table, "users"),
            other => panic!("expected TableAlreadyExists, got: {:?}", other),
        }
    }

    #[test]
    fn analyze_create_statement_missing_primary_key() {
        let tmp = TempDir::new().unwrap();
        let db_dir = tmp.path().join("db");
        fs::create_dir(&db_dir).unwrap();
        let db_path = db_dir.join(METADATA_FILE_NAME);
        fs::write(&db_path, r#"{ "tables": [] }"#).unwrap();
        let catalog = Catalog::new(tmp.path(), "db").unwrap();
        let catalog = Arc::new(RwLock::new(catalog));

        let mut ast = Ast::default();

        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "missing_pk".into(),
        }));

        let col_x_ident =
            ast.add_node(Expression::Identifier(IdentifierNode { value: "x".into() }));

        let col_x = CreateColumnDescriptor {
            name: col_x_ident,
            ty: Type::I32,
            addon: CreateColumnAddon::None,
        };

        let create = CreateStatement {
            table_name: table_ident,
            columns: vec![col_x],
        };
        ast.add_statement(Statement::Create(create));

        let analyzer = Analyzer::new(&ast, catalog);
        let errs = analyzer.analyze().unwrap_err();
        assert_eq!(errs.len(), 1);
        let err = &errs[0];

        match err {
            AnalyzerError::PrimaryKeyMissing => {}
            other => panic!("expected PrimaryKeyMissing, got: {:?}", other),
        }
    }

    #[test]
    fn analyze_create_duplicate_primary_key() {
        let tmp_dir = TempDir::new().unwrap();
        let db_dir = tmp_dir.path().join("db");
        fs::create_dir(&db_dir).unwrap();
        let db_path = db_dir.join(METADATA_FILE_NAME);
        fs::write(&db_path, r#"{ "tables": [] }"#).unwrap();
        let catalog = Catalog::new(tmp_dir.path(), "db").unwrap();

        let catalog = Arc::new(RwLock::new(catalog));

        let mut ast = Ast::default();

        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "dup_pk".into(),
        }));

        let col_x_ident =
            ast.add_node(Expression::Identifier(IdentifierNode { value: "x".into() }));
        let col_y_ident =
            ast.add_node(Expression::Identifier(IdentifierNode { value: "y".into() }));

        let col_x = CreateColumnDescriptor {
            name: col_x_ident,
            ty: Type::I32,
            addon: CreateColumnAddon::PrimaryKey,
        };
        let col_y = CreateColumnDescriptor {
            name: col_y_ident,
            ty: Type::I64,
            addon: CreateColumnAddon::PrimaryKey,
        };

        let create = CreateStatement {
            table_name: table_ident,
            columns: vec![col_x, col_y],
        };
        ast.add_statement(Statement::Create(create));

        let analyzer = Analyzer::new(&ast, catalog);
        let errs = analyzer.analyze().unwrap_err();
        assert_eq!(errs.len(), 1);
        let err = &errs[0];
        match err {
            AnalyzerError::UniqueAddonUsedMoreThanOnce { addon } => {
                assert_eq!(*addon, ResolvedCreateColumnAddon::PrimaryKey.to_string());
            }
            other => panic!("expected UniqueAddonUsedMoreThanOnce, got: {:?}", other),
        }
    }

    #[test]
    fn analyze_alter_add_column_successful() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));
        let table_name = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: table_ident,
            alias: None,
        }));

        let new_col_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "age".into(),
        }));

        let add_action = AddAlterAction {
            column_name: new_col_ident,
            column_type: Type::I32,
        };

        let alter = AlterStatement {
            table_name,
            action: AlterAction::Add(add_action),
        };
        ast.add_statement(Statement::Alter(alter));

        let analyzer = Analyzer::new(&ast, catalog);
        let rt = analyzer.analyze().expect("analyze should succeed");

        assert_eq!(rt.statements.len(), 1);

        let add = expect_alter_add_column(&rt, 0);
        let tbl = expect_table(&rt, add.table);
        assert_eq!(tbl.name, "users");
        assert_eq!(add.column_name, "age");
        assert_eq!(add.column_type, Type::I32);
    }

    #[test]
    fn analyze_alter_add_column_column_exists() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));
        let table_name = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: table_ident,
            alias: None,
        }));

        let existing_col_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "name".into(),
        }));

        let add_action = AddAlterAction {
            column_name: existing_col_ident,
            column_type: Type::String,
        };

        let alter = AlterStatement {
            table_name,
            action: AlterAction::Add(add_action),
        };
        ast.add_statement(Statement::Alter(alter));

        let analyzer = Analyzer::new(&ast, catalog);
        let errs = analyzer.analyze().unwrap_err();
        assert_eq!(errs.len(), 1);
        let err = &errs[0];
        match err {
            AnalyzerError::ColumnAlreadyExists { column } => {
                assert_eq!(column, "name");
            }
            other => panic!("expected ColumnAlreadyExists, got: {:?}", other),
        }
    }

    #[test]
    fn analyze_alter_rename_column_successful() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));
        let table_name = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: table_ident,
            alias: None,
        }));

        let prev_name_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "name".into(),
        }));
        let prev_col = ast.add_node(Expression::ColumnIdentifier(ColumnIdentifierNode {
            identifier: prev_name_ident,
            table_alias: None,
        }));

        let new_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "full_name".into(),
        }));

        let rename_action = RenameColumnAlterAction {
            previous_name: prev_col,
            new_name: new_ident,
        };

        let alter = AlterStatement {
            table_name,
            action: AlterAction::RenameColumn(rename_action),
        };
        ast.add_statement(Statement::Alter(alter));

        let analyzer = Analyzer::new(&ast, catalog);
        let rt = analyzer.analyze().expect("analyze should succeed");

        assert_eq!(rt.statements.len(), 1);

        let rename = expect_alter_rename_column(&rt, 0);

        let tbl = expect_table(&rt, rename.table);

        assert_eq!(tbl.name, "users");

        // previous column resolved correctly
        let col = expect_column(&rt, rename.column);
        assert_eq!(col.name, "name");
        assert_eq!(col.ty, Type::String);
        assert_eq!(col.pos, 1);

        // new name
        assert_eq!(rename.new_name, "full_name");
    }

    #[test]
    fn analyze_alter_rename_column_new_name_already_exists() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));
        let table_name = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: table_ident,
            alias: None,
        }));

        let prev_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "id".into(),
        }));
        let prev_col = ast.add_node(Expression::ColumnIdentifier(ColumnIdentifierNode {
            identifier: prev_ident,
            table_alias: None,
        }));

        // try to rename to existing column "name"
        let new_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "name".into(),
        }));

        let rename_action = RenameColumnAlterAction {
            previous_name: prev_col,
            new_name: new_ident,
        };

        let alter = AlterStatement {
            table_name,
            action: AlterAction::RenameColumn(rename_action),
        };
        ast.add_statement(Statement::Alter(alter));

        let analyzer = Analyzer::new(&ast, catalog);
        let errs = analyzer.analyze().unwrap_err();
        assert_eq!(errs.len(), 1);
        let err = &errs[0];
        match err {
            AnalyzerError::ColumnAlreadyExists { column } => {
                assert_eq!(column, "name");
            }
            other => panic!("expected ColumnAlreadyExists, got: {:?}", other),
        }
    }

    #[test]
    fn analyze_alter_rename_table_successful() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));
        let table_name = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: table_ident,
            alias: None,
        }));

        let new_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "accounts".into(),
        }));

        let rename_action = RenameTableAlterAction {
            new_name: new_ident,
        };

        let alter = AlterStatement {
            table_name,
            action: AlterAction::RenameTable(rename_action),
        };
        ast.add_statement(Statement::Alter(alter));

        let analyzer = Analyzer::new(&ast, catalog);
        let rt = analyzer.analyze().expect("analyze should succeed");

        assert_eq!(rt.statements.len(), 1);

        let rename = expect_alter_rename_table(&rt, 0);
        let tbl = expect_table(&rt, rename.table);
        assert_eq!(tbl.name, "users");
        assert_eq!(rename.new_name, "accounts");
    }

    #[test]
    fn analyze_alter_rename_table_new_name_already_exists() {
        // create a catalog containing both "users" and "accounts"
        let tmp_dir = TempDir::new().unwrap();
        let db_dir = tmp_dir.path().join("db");
        fs::create_dir(&db_dir).unwrap();
        let db_path = db_dir.join(METADATA_FILE_NAME);
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
                },
                {
                    "name": "accounts",
                    "columns": [
                        { "name": "id", "ty": "I32", "pos": 0, "base_offset": 0, "base_offset_pos": 0 },
                        { "name": "owner", "ty": "String", "pos": 1, "base_offset": 4, "base_offset_pos": 1 }
                    ],
                    "primary_key_column_name": "id"
                }
            ]
        }
        "#;
        fs::write(&db_path, json).unwrap();
        let catalog = Catalog::new(tmp_dir.path(), "db").unwrap();
        let catalog = Arc::new(RwLock::new(catalog));

        let mut ast = Ast::default();

        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));
        let table_name = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: table_ident,
            alias: None,
        }));

        let new_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "accounts".into(),
        }));

        let rename_action = RenameTableAlterAction {
            new_name: new_ident,
        };

        let alter = AlterStatement {
            table_name,
            action: AlterAction::RenameTable(rename_action),
        };
        ast.add_statement(Statement::Alter(alter));

        let analyzer = Analyzer::new(&ast, catalog);
        let errs = analyzer.analyze().unwrap_err();
        assert_eq!(errs.len(), 1);
        let err = &errs[0];
        match err {
            AnalyzerError::TableAlreadyExists { table } => {
                assert_eq!(table, "accounts");
            }
            other => panic!("expected TableAlreadyExists, got: {:?}", other),
        }
    }

    #[test]
    fn analyze_alter_drop_column_successful() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));
        let table_name = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: table_ident,
            alias: None,
        }));

        // drop existing non-primary column "name"
        let col_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "name".into(),
        }));
        let col_node = ast.add_node(Expression::ColumnIdentifier(ColumnIdentifierNode {
            identifier: col_ident,
            table_alias: None,
        }));

        let drop_action = DropAlterAction {
            column_name: col_node,
        };

        let alter = AlterStatement {
            table_name,
            action: AlterAction::Drop(drop_action),
        };
        ast.add_statement(Statement::Alter(alter));

        let analyzer = Analyzer::new(&ast, catalog);
        let rt = analyzer.analyze().expect("analyze should succeed");

        assert_eq!(rt.statements.len(), 1);
        let drop = expect_alter_drop_column(&rt, 0);
        let tbl = expect_table(&rt, drop.table);
        assert_eq!(tbl.name, "users");

        let col = expect_column(&rt, drop.column);
        assert_eq!(col.name, "name");
        assert_eq!(col.ty, Type::String);
    }

    #[test]
    fn analyze_alter_drop_column_cannot_drop_primary_key() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        let table_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));
        let table_name = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: table_ident,
            alias: None,
        }));

        // try to drop primary key column "id"
        let col_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "id".into(),
        }));
        let col_node = ast.add_node(Expression::ColumnIdentifier(ColumnIdentifierNode {
            identifier: col_ident,
            table_alias: None,
        }));

        let drop_action = DropAlterAction {
            column_name: col_node,
        };

        let alter = AlterStatement {
            table_name,
            action: AlterAction::Drop(drop_action),
        };
        ast.add_statement(Statement::Alter(alter));

        let analyzer = Analyzer::new(&ast, catalog);
        let errs = analyzer.analyze().unwrap_err();
        assert_eq!(errs.len(), 1);
        let err = &errs[0];
        match err {
            AnalyzerError::ColumnCannotBeDropped { column } => {
                assert_eq!(column, "id");
            }
            other => panic!("expected ColumnCannotBeDropped, got: {:?}", other),
        }
    }

    #[test]
    fn analyze_multiple_statements_error_collection() {
        let catalog = catalog_with_users();
        let mut ast = Ast::default();

        // first statement (table not found)
        let t1_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "nonexistent".into(),
        }));
        let t1 = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: t1_ident,
            alias: None,
        }));
        let s1 = SelectStatement {
            table_name: t1,
            columns: None,
            where_clause: None,
        };
        ast.add_statement(Statement::Select(s1));

        // second statement (without an error)
        let t2_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));
        let t2 = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: t2_ident,
            alias: None,
        }));
        let s2 = SelectStatement {
            table_name: t2,
            columns: None,
            where_clause: None,
        };
        ast.add_statement(Statement::Select(s2));

        // third statement (adding existing column "name")
        let t3_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "users".into(),
        }));
        let t3 = ast.add_node(Expression::TableIdentifier(TableIdentifierNode {
            identifier: t3_ident,
            alias: None,
        }));
        let existing_col_ident = ast.add_node(Expression::Identifier(IdentifierNode {
            value: "name".into(),
        }));
        let add_action = AddAlterAction {
            column_name: existing_col_ident,
            column_type: Type::String,
        };
        let alter = AlterStatement {
            table_name: t3,
            action: AlterAction::Add(add_action),
        };
        ast.add_statement(Statement::Alter(alter));

        let analyzer = Analyzer::new(&ast, catalog);
        let errs = analyzer.analyze().unwrap_err();

        assert_eq!(errs.len(), 2);

        match &errs[0] {
            AnalyzerError::TableNotFound { table } => assert_eq!(table, "nonexistent"),
            other => panic!("expected TableNotFound as first error, got: {:?}", other),
        }

        match &errs[1] {
            AnalyzerError::ColumnAlreadyExists { column } => assert_eq!(column, "name"),
            other => panic!(
                "expected ColumnAlreadyExists as second error, got: {:?}",
                other
            ),
        }
    }
}

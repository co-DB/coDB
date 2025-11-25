use std::{collections::HashMap, iter, mem};

use engine::{
    data_types,
    record::{Field, Record},
};
use itertools::Itertools;
use metadata::catalog::{ColumnMetadata, ColumnMetadataError, TableMetadata};
use planner::{
    query_plan::{CreateTable, Filter, Projection, StatementPlan, StatementPlanItem, TableScan},
    resolved_tree::{
        ResolvedColumn, ResolvedCreateColumnDescriptor, ResolvedExpression, ResolvedNodeId,
        ResolvedTree,
    },
};

use crate::{
    Executor, InternalExecutorError, StatementResult, error_factory,
    expression_executor::ExpressionExecutor,
    response::{ColumnData, StatementType},
};

/// Executes a single statement from a query plan.
///
/// This struct encapsulates all the logic needed to execute one statement, including
/// query operations (SELECT) and mutation operations (CREATE TABLE, INSERT, etc.).
pub(crate) struct StatementExecutor<'e, 'q> {
    executor: &'e Executor,
    statement: &'q StatementPlan,
    ast: &'q ResolvedTree,
}

/// Result of projection operation containing records and their column metadata.
///
/// The records vector contains transformed records with only the projected columns,
/// while the columns vector describes the schema of those projected records.
struct ProjectedRecords {
    records: Vec<Record>,
    columns: Vec<ColumnData>,
}

/// Column metadata used during projection operations.
struct ProjectColumn<'c> {
    rc: &'c ResolvedColumn,
    /// Set to true if this is the last struct that references the [`ResolvedColumn`].
    last: bool,
}

impl<'e, 'q> StatementExecutor<'e, 'q> {
    pub(crate) fn new(
        executor: &'e Executor,
        statement: &'q StatementPlan,
        ast: &'q ResolvedTree,
    ) -> Self {
        StatementExecutor {
            executor,
            statement,
            ast,
        }
    }

    /// Executes [`StatementExecutor::statement`] and returns its result.
    pub(crate) fn execute(&self) -> StatementResult {
        let root = self.statement.root();

        match root.produces_result_set() {
            true => self.execute_query(root),
            false => self.execute_mutation(root),
        }
    }

    /// Handler for all statements that only return data.
    fn execute_query(&self, item: &StatementPlanItem) -> StatementResult {
        match item {
            StatementPlanItem::Projection(projection) => self.projection(projection),
            _ => error_factory::runtime_error(format!(
                "Invalid root operation ({:?}) for query statement",
                item
            )),
        }
    }

    /// Handler for all statements that mutate data.
    fn execute_mutation(&self, item: &StatementPlanItem) -> StatementResult {
        match item {
            StatementPlanItem::CreateTable(create_table) => self.create_table(create_table),
            _ => error_factory::runtime_error(format!(
                "Invalid root operation ({:?}) for mutation statement",
                item
            )),
        }
    }

    /// Handler for all statements that are source of data.
    fn execute_data_source(
        &self,
        data_source: &StatementPlanItem,
    ) -> Result<Vec<Record>, InternalExecutorError> {
        match data_source {
            StatementPlanItem::TableScan(table_scan) => {
                let records = self.table_scan(table_scan)?;
                Ok(records)
            }
            StatementPlanItem::Filter(filter) => self.filter(filter),
            _ => Err(InternalExecutorError::InvalidOperationInDataSource {
                operation: format!("{:?}", data_source),
            }),
        }
    }

    /// Handler for [`TableScan`] statement.
    fn table_scan(&self, table_scan: &TableScan) -> Result<Vec<Record>, InternalExecutorError> {
        let records = self
            .executor
            .with_heap_file(&table_scan.table_name, |hf| hf.all_records())??;
        Ok(records)
    }

    /// Handler for [`Filter`] statement.
    fn filter(&self, filter: &Filter) -> Result<Vec<Record>, InternalExecutorError> {
        let data_source = self.statement.item(filter.data_source);
        let records = self.execute_data_source(data_source)?;
        self.apply_filter(records.into_iter(), filter.predicate)
    }

    /// Applies filter to `records`, returning only those where the `predicate` evaluates to `true`.
    fn apply_filter(
        &self,
        records: impl Iterator<Item = Record>,
        predicate: ResolvedNodeId,
    ) -> Result<Vec<Record>, InternalExecutorError> {
        records
            .filter_map(|record| {
                let e = ExpressionExecutor::new(&record, self.ast);
                match e.execute_expression(predicate) {
                    Ok(result) => match result.as_bool() {
                        Some(true) => Some(Ok(record)),
                        Some(false) => None,
                        None => Some(Err(error_factory::unexpected_type("bool", result.as_ref()))),
                    },
                    Err(e) => Some(Err(e)),
                }
            })
            .collect()
    }

    /// Handler for [`Projection`] statement.
    fn projection(&self, projection: &Projection) -> StatementResult {
        let data_source = self.statement.item(projection.data_source);
        let records = match self.execute_data_source(data_source) {
            Ok(records) => records,
            Err(err) => {
                return StatementResult::from(&err);
            }
        };
        match self.project_records(records.into_iter(), &projection.columns) {
            Ok(projected_records) => StatementResult::SelectSuccessful {
                columns: projected_records.columns,
                rows: projected_records.records,
            },
            Err(err) => StatementResult::from(&err),
        }
    }

    /// Transforms `records` so that they only contain specified `columns`.
    fn project_records(
        &self,
        records: impl Iterator<Item = Record>,
        columns: &[ResolvedNodeId],
    ) -> Result<ProjectedRecords, InternalExecutorError> {
        let project_columns: Vec<_> = self.map_expressions_to_project_columns(columns).collect();
        let columns_data: Vec<_> = project_columns
            .iter()
            .map(|sc| ColumnData::from(sc.rc))
            .collect();

        let projected_records: Vec<_> = records
            .map(|record| {
                let mut source_fields = record.fields;
                let fields: Vec<_> = project_columns
                    .iter()
                    .map(|select_col| {
                        let pos = select_col.rc.pos as usize;
                        match select_col.last {
                            true => mem::replace(&mut source_fields[pos], Field::Bool(false)),
                            false => source_fields[pos].clone(),
                        }
                    })
                    .collect();
                Record::new(fields)
            })
            .collect();

        Ok(ProjectedRecords {
            records: projected_records,
            columns: columns_data,
        })
    }

    /// Handler for [`CreateTable`] table statement.
    fn create_table(&self, create_table: &CreateTable) -> StatementResult {
        let column_metadatas = match self.process_columns(create_table) {
            Ok(cm) => cm,
            Err(err) => {
                return error_factory::runtime_error(format!("Failed to create column: {err}"));
            }
        };

        let table_metadata = match TableMetadata::new(
            &create_table.name,
            &column_metadatas,
            &create_table.primary_key_column.name,
        ) {
            Ok(tm) => tm,
            Err(err) => {
                return error_factory::runtime_error(format!("Failed to create table: {}", err));
            }
        };

        let mut catalog = self.executor.catalog.write();

        if let Err(err) = catalog.add_table(table_metadata) {
            return error_factory::runtime_error(format!("Failed to create table: {}", err));
        }

        match catalog.sync_to_disk() {
            Ok(_) => StatementResult::OperationSuccessful {
                rows_affected: 0,
                ty: StatementType::Create,
            },
            Err(err) => error_factory::runtime_error(format!(
                "Failed to save catalog content to disk: {err}"
            )),
        }
    }

    /// Creates iterator over all columns in [`CreateTable`] (including primary key column)
    /// and sorts them by whether they are fixed-size (fixed-size columns are first).
    fn sort_columns_by_fixed_size<'c>(
        &self,
        create_table: &'c CreateTable,
    ) -> impl Iterator<Item = &'c ResolvedCreateColumnDescriptor> {
        iter::once(&create_table.primary_key_column)
            .chain(create_table.columns.iter())
            .sorted_by(|a, b| {
                let a_fixed = a.ty.is_fixed_size();
                let b_fixed = b.ty.is_fixed_size();
                b_fixed.cmp(&a_fixed)
            })
    }

    /// Returns vector of [`ColumnMetadata`] that maps to columns in [`CreateTable`].
    fn process_columns(
        &self,
        create_table: &CreateTable,
    ) -> Result<Vec<ColumnMetadata>, ColumnMetadataError> {
        let mut column_metadatas = Vec::with_capacity(create_table.columns.len());

        let cols = self.sort_columns_by_fixed_size(create_table);

        let mut pos = 0;
        let mut last_fixed_pos = 0;
        let mut base_offset = 0;

        for col in cols {
            let column_metadata =
                ColumnMetadata::new(col.name.clone(), col.ty, pos, base_offset, last_fixed_pos)?;
            column_metadatas.push(column_metadata);
            pos += 1;
            if let Some(offset) = data_types::type_size_on_disk(&col.ty) {
                last_fixed_pos += 1;
                base_offset += offset;
            }
        }
        Ok(column_metadatas)
    }

    /// Creates iterator that maps `expressions` into [`ProjectColumn`]s.
    /// It assumes that each expression points to [`ResolvedExpression::ColumnRef`].
    fn map_expressions_to_project_columns(
        &self,
        expressions: &'q [ResolvedNodeId],
    ) -> impl Iterator<Item = ProjectColumn<'q>> {
        let mut last_occurrence = HashMap::new();
        for (idx, &expr) in expressions.iter().enumerate() {
            last_occurrence.insert(expr, idx);
        }

        expressions.iter().enumerate().map(move |(idx, &expr)| {
            let last = last_occurrence.get(&expr) == Some(&idx);
            match self.ast.node(expr) {
                ResolvedExpression::ColumnRef(rc) => ProjectColumn { rc, last },
                _ => unreachable!(),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::tests::{create_single_statement, create_test_executor};

    use super::*;

    #[test]
    fn test_process_columns_calculates_correct_offsets() {
        let (executor, _temp_dir) = create_test_executor();
        let (plan, ast) = create_single_statement(
            "CREATE TABLE test (name STRING, score FLOAT64, surname STRING, active BOOL, id INT32 PRIMARY_KEY);",
            &executor,
        );

        let se = StatementExecutor::new(&executor, &plan, &ast);

        let create_table = match plan.root() {
            StatementPlanItem::CreateTable(ct) => ct,
            _ => panic!("invalid item"),
        };
        let columns = se.process_columns(create_table).unwrap();

        let id_col = columns.iter().find(|c| c.name() == "id").unwrap();
        assert_eq!(id_col.base_offset(), 0);
        assert_eq!(id_col.base_offset_pos(), 0);
        assert_eq!(id_col.pos(), 0);

        let score_col = columns.iter().find(|c| c.name() == "score").unwrap();
        assert_eq!(score_col.base_offset(), 4);
        assert_eq!(score_col.base_offset_pos(), 1);
        assert_eq!(score_col.pos(), 1);

        let active_col = columns.iter().find(|c| c.name() == "active").unwrap();
        assert_eq!(active_col.base_offset(), 12);
        assert_eq!(active_col.base_offset_pos(), 2);
        assert_eq!(active_col.pos(), 2);

        let name_col = columns.iter().find(|c| c.name() == "name").unwrap();
        assert_eq!(name_col.base_offset(), 13);
        assert_eq!(name_col.base_offset_pos(), 3);
        assert_eq!(name_col.pos(), 3);

        let surname_col = columns.iter().find(|c| c.name() == "surname").unwrap();
        assert_eq!(surname_col.base_offset(), 13);
        assert_eq!(surname_col.base_offset_pos(), 3);
        assert_eq!(surname_col.pos(), 4);
    }
}

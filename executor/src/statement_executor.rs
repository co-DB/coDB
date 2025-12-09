use std::{cmp::Ordering, collections::HashMap, iter, mem, ops::Deref};

use engine::{
    heap_file::{HeapFileError, RecordPtr},
    record::{Field, Record},
};
use itertools::Itertools;
use metadata::catalog::{ColumnMetadata, NewColumnRequest, TableMetadataFactory};
use planner::{
    query_plan::{
        AddColumn, CreateTable, Filter, Insert, Limit, Projection, RemoveColumn, Skip, Sort,
        SortOrder, StatementPlan, StatementPlanItem, TableScan,
    },
    resolved_tree::{
        ResolvedColumn, ResolvedCreateColumnDescriptor, ResolvedExpression, ResolvedNodeId,
        ResolvedTree,
    },
};
use types::data::Value;

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
            StatementPlanItem::Insert(insert) => self.insert(insert),
            StatementPlanItem::CreateTable(create_table) => self.create_table(create_table),
            StatementPlanItem::AddColumn(add_column) => self.add_column(add_column),
            StatementPlanItem::RemoveColumn(remove_column) => self.remove_column(remove_column),
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
            StatementPlanItem::Sort(sort) => self.sort(sort),
            StatementPlanItem::Skip(skip) => self.skip(skip),
            StatementPlanItem::Limit(limit) => self.limit(limit),
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
                let e = ExpressionExecutor::with_single_record(&record, self.ast);
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

    /// Handler for [`Sort`] statement.
    fn sort(&self, sort: &Sort) -> Result<Vec<Record>, InternalExecutorError> {
        let data_source = self.statement.item(sort.data_source);
        let records = self.execute_data_source(data_source)?;
        let column_pos = self.get_column_position(sort.column);
        self.apply_sorting(records, column_pos, sort.order)
    }

    /// Sorts `records` by `column_pos`-th column in `order`.
    /// If any comparison fails the error is returned.
    fn apply_sorting(
        &self,
        mut records: Vec<Record>,
        column_pos: usize,
        order: SortOrder,
    ) -> Result<Vec<Record>, InternalExecutorError> {
        let mut e = None;
        records.sort_by(|lhs, rhs| {
            let lhs_value = lhs.fields[column_pos].deref();
            let rhs_value = rhs.fields[column_pos].deref();
            let cmp_res = match order {
                SortOrder::Ascending => self.cmp_fields(lhs_value, rhs_value),
                SortOrder::Descending => self.cmp_fields(rhs_value, lhs_value),
            };
            match cmp_res {
                Ok(cmp) => cmp,
                Err(err) => {
                    e = Some(err);
                    Ordering::Equal
                }
            }
        });
        match e {
            Some(e) => Err(e),
            None => Ok(records),
        }
    }

    /// Handler for [`Skip`] statement.
    fn skip(&self, skip: &Skip) -> Result<Vec<Record>, InternalExecutorError> {
        let data_source = self.statement.item(skip.data_source);
        let records = self.execute_data_source(data_source)?;
        Ok(records.into_iter().skip(skip.count as _).collect())
    }

    /// Handler for [`Limit`] statement.
    fn limit(&self, limit: &Limit) -> Result<Vec<Record>, InternalExecutorError> {
        let data_source = self.statement.item(limit.data_source);
        let records = self.execute_data_source(data_source)?;
        Ok(records.into_iter().take(limit.count as _).collect())
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
                            true => {
                                // We replace it with dummy field, in this case just Bool(false), but here could be anything - we won't use it anymore
                                mem::replace(&mut source_fields[pos], Value::Bool(false).into())
                            }
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

    /// Handler for [`Insert`] statement.
    fn insert(&self, insert: &Insert) -> StatementResult {
        if let Err(e) = self.process_insert(insert) {
            return StatementResult::from(&e);
        }
        StatementResult::OperationSuccessful {
            rows_affected: 1,
            ty: StatementType::Insert,
        }
    }

    fn process_insert(&self, insert: &Insert) -> Result<(), InternalExecutorError> {
        let record = self.build_record(&insert.columns, &insert.values)?;

        let _ptr = self.executor.with_heap_file(&insert.table_name, |hf| {
            let ptr = hf.insert(record)?;
            Ok::<RecordPtr, HeapFileError>(ptr)
        })??;
        // TODO: we should insert this `_ptr` into btree

        Ok(())
    }

    /// Sorts `values` by their corresponding `column` position and evaluates them.
    /// Returns [`Record`] containing those fields.
    fn build_record(
        &self,
        columns: &[ResolvedNodeId],
        values: &[ResolvedNodeId],
    ) -> Result<Record, InternalExecutorError> {
        let fields: Result<Vec<_>, _> = columns
            .iter()
            .zip(values.iter())
            .sorted_by(|&(lhs_col, _), &(rhs_col, _)| {
                let left_pos = self.get_column_position(*lhs_col);
                let right_pos = self.get_column_position(*rhs_col);
                left_pos.cmp(&right_pos)
            })
            .map(|(_, &expression)| {
                let e = ExpressionExecutor::empty(self.ast);
                e.execute_expression(expression)
            })
            .collect();
        let fields = fields?
            .into_iter()
            .map(|value| Field::from(value.into_owned()))
            .collect();
        let record = Record::new(fields);
        Ok(record)
    }

    /// Handler for [`CreateTable`] statement.
    fn create_table(&self, create_table: &CreateTable) -> StatementResult {
        let new_columns = self.map_to_new_columns_request(
            iter::once(&create_table.primary_key_column).chain(create_table.columns.iter()),
        );

        let tm_factory = TableMetadataFactory::new(
            &create_table.name,
            new_columns,
            &create_table.primary_key_column.name,
        );
        let table_metadata = match tm_factory.create_table_metadata() {
            Ok(tm) => tm,
            Err(err) => {
                return error_factory::runtime_error(format!("Failed to create table: {}", err));
            }
        };

        let mut catalog = self.executor.catalog.write();

        match catalog.add_table(table_metadata) {
            Ok(_) => StatementResult::OperationSuccessful {
                rows_affected: 0,
                ty: StatementType::Create,
            },
            Err(err) => error_factory::runtime_error(format!("Failed to create table: {}", err)),
        }
    }

    fn map_to_new_columns_request(
        &self,
        columns: impl Iterator<Item = &'q ResolvedCreateColumnDescriptor>,
    ) -> Vec<NewColumnRequest> {
        columns
            .map(|c| NewColumnRequest {
                name: c.name.clone(),
                ty: c.ty,
            })
            .collect()
    }

    /// Handler for [`AddColumn`] statement.
    fn add_column(&self, add_column: &AddColumn) -> StatementResult {
        let column_request = NewColumnRequest {
            name: add_column.column_name.clone(),
            ty: add_column.column_ty,
        };

        // Update columns in catalog first
        let column_added = match self
            .executor
            .catalog
            .write()
            .add_column(&add_column.table_name, column_request)
        {
            Ok(ca) => ca,
            Err(e) => return error_factory::runtime_error(format!("failed to add column: {e}")),
        };

        let revert_changes_in_catalog = || {
            if let Err(_) = self
                .executor
                .catalog
                .write()
                .remove_column(&add_column.table_name, &add_column.column_name)
            {
                // Failed to revert changes - DB state is invalid (TODO: maybe we can do something better here)
                return Some(error_factory::runtime_error(
                    "added column to catalog, but didn't migrate records in heap file - DB content is out of sync",
                ));
            }
            None
        };

        // Load new columns list
        let table = match self.executor.catalog.read().table(&add_column.table_name) {
            Ok(t) => t,
            Err(e) => {
                // Try to revert changes made in catalog
                if let Some(revert_err) = revert_changes_in_catalog() {
                    return revert_err;
                }
                return error_factory::runtime_error(format!("failed to read table: {e}"));
            }
        };
        let new_columns: Vec<_> = table.columns().collect();

        // Try to migrate records in heap file
        if let Err(e) = self.add_new_field_to_records(
            &add_column.table_name,
            column_added.pos,
            column_added.base_offset,
            Value::default_for_ty(&column_added.ty),
            new_columns,
        ) {
            // Try to revert changes made in catalog
            if let Some(revert_err) = revert_changes_in_catalog() {
                return revert_err;
            }

            return error_factory::runtime_error(format!(
                "couldn't migrate records in heap file: {e}"
            ));
        };

        StatementResult::OperationSuccessful {
            rows_affected: 0,
            ty: StatementType::Alter,
        }
    }

    /// Adds new field to each record in heap file
    fn add_new_field_to_records(
        &self,
        table_name: impl AsRef<str>,
        position: u16,
        new_column_min_offset: usize,
        default_value: Value,
        new_columns: Vec<ColumnMetadata>,
    ) -> Result<(), InternalExecutorError> {
        self.executor
            .with_heap_file_mut(table_name.as_ref(), |hf| {
                hf.add_column_migration(position, new_column_min_offset, default_value, new_columns)
            })??;
        Ok(())
    }

    /// Handler for [`RemoveColumn`] statement.
    fn remove_column(&self, remove_column: &RemoveColumn) -> StatementResult {
        // Update columns in catalog first
        let column_removed = match self
            .executor
            .catalog
            .write()
            .remove_column(&remove_column.table_name, &remove_column.column_name)
        {
            Ok(ca) => ca,
            Err(e) => return error_factory::runtime_error(format!("failed to remove column: {e}")),
        };

        let revert_changes_in_catalog = || {
            let column_request = NewColumnRequest {
                name: remove_column.column_name.clone(),
                ty: column_removed.ty,
            };

            if let Err(_) = self
                .executor
                .catalog
                .write()
                .add_column(&remove_column.table_name, column_request)
            {
                // Failed to revert changes - DB state is invalid (TODO: maybe we can do something better here)
                return Some(error_factory::runtime_error(
                    "removed column from catalog, but didn't migrate records in heap file - DB content is out of sync",
                ));
            }
            None
        };

        // Load new columns list
        let table = match self
            .executor
            .catalog
            .read()
            .table(&remove_column.table_name)
        {
            Ok(t) => t,
            Err(e) => {
                // Try to revert changes made in catalog
                if let Some(revert_err) = revert_changes_in_catalog() {
                    return revert_err;
                }
                return error_factory::runtime_error(format!("failed to read table: {e}"));
            }
        };
        let new_columns: Vec<_> = table.columns().collect();

        // Try to migrate records in heap file
        if let Err(e) = self.remove_field_from_records(
            &remove_column.table_name,
            column_removed.pos,
            column_removed.prev_column_base_offset,
            new_columns,
        ) {
            // Try to revert changes made in catalog
            if let Some(revert_err) = revert_changes_in_catalog() {
                return revert_err;
            }

            return error_factory::runtime_error(format!(
                "couldn't migrate records in heap file: {e}"
            ));
        };

        StatementResult::OperationSuccessful {
            rows_affected: 0,
            ty: StatementType::Alter,
        }
    }

    /// Removes field from each record in heap file
    fn remove_field_from_records(
        &self,
        table_name: impl AsRef<str>,
        position: u16,
        prev_column_min_offset: usize,
        new_columns: Vec<ColumnMetadata>,
    ) -> Result<(), InternalExecutorError> {
        self.executor
            .with_heap_file_mut(table_name.as_ref(), |hf| {
                hf.remove_column_migration(position, prev_column_min_offset, new_columns)
            })??;
        Ok(())
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

    /// Returns `column` position in table schema.
    fn get_column_position(&self, column: ResolvedNodeId) -> usize {
        match self.ast.node(column) {
            ResolvedExpression::ColumnRef(cr) => cr.pos as usize,
            _ => unreachable!(),
        }
    }

    /// Compares `lhs` with `rhs`, returns an error if types don't match or values cannot be compared.
    fn cmp_fields(&self, lhs: &Value, rhs: &Value) -> Result<Ordering, InternalExecutorError> {
        match (lhs, rhs) {
            (Value::Int32(lhs), Value::Int32(rhs)) => Ok(lhs.cmp(rhs)),
            (Value::Int64(lhs), Value::Int64(rhs)) => Ok(lhs.cmp(rhs)),
            (Value::Float32(lhs), Value::Float32(rhs)) => lhs.partial_cmp(rhs).ok_or_else(|| {
                error_factory::comparing_nan_values(lhs.to_string(), rhs.to_string())
            }),
            (Value::Float64(lhs), Value::Float64(rhs)) => lhs.partial_cmp(rhs).ok_or_else(|| {
                error_factory::comparing_nan_values(lhs.to_string(), rhs.to_string())
            }),
            (Value::Bool(lhs), Value::Bool(rhs)) => Ok(lhs.cmp(rhs)),
            (Value::String(lhs), Value::String(rhs)) => Ok(lhs.cmp(rhs)),
            (Value::Date(lhs), Value::Date(rhs)) => Ok(lhs.cmp(rhs)),
            (Value::DateTime(lhs), Value::DateTime(rhs)) => Ok(lhs.cmp(rhs)),
            _ => Err(error_factory::incompatible_types(lhs, rhs)),
        }
    }
}

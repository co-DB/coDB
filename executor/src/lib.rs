mod consts;
mod iterators;

use std::{iter::once, sync::Arc};

use dashmap::DashMap;
use engine::{data_types, heap_file::HeapFile, record::Record};
use itertools::Itertools;
use metadata::{
    catalog::{Catalog, ColumnMetadata, ColumnMetadataError, TableMetadata},
    types::Type,
};
use parking_lot::RwLock;
use planner::{
    query_plan::{CreateTable, StatementPlan, StatementPlanItem},
    resolved_tree::{ResolvedCreateColumnDescriptor, ResolvedTree},
};
use storage::cache::Cache;

use crate::{
    consts::HEAP_FILE_BUCKET_SIZE,
    iterators::{ParseErrorIter, QueryResultIter, StatementIter},
};

pub struct Executor {
    heap_files: DashMap<String, HeapFile<HEAP_FILE_BUCKET_SIZE>>,
    cache: Arc<Cache>,
    catalog: Arc<RwLock<Catalog>>,
}

pub struct ColumnData {
    pub name: String,
    pub ty: Type,
}

pub enum StatementType {
    Insert,
    Update,
    Delete,
    Create,
    Alter,
    Truncate,
    Drop,
}

pub enum StatementResult {
    OperationSuccessful {
        rows_affected: usize,
        ty: StatementType,
    },
    SelectSuccessful {
        columns: Vec<ColumnData>,
        rows: Vec<Record>,
    },
    ParseError {
        error: String,
    },
    RuntimeError {
        error: String,
    },
}

impl Executor {
    pub fn new(database_name: &str, catalog: Catalog) -> Self {
        todo!()
    }

    pub fn execute<'e>(&'e self, query: &str) -> QueryResultIter<'e> {
        let parse_output = planner::process_query(query, self.catalog.clone());
        match parse_output {
            Ok(query_plan) => StatementIter::new(query_plan.plans, query_plan.tree, &self).into(),
            Err(errors) => ParseErrorIter::new(errors).into(),
        }
    }

    fn execute_statement(&self, statement: &StatementPlan, ast: &ResolvedTree) -> StatementResult {
        let root = statement.item(statement.root());
        match root {
            StatementPlanItem::TableScan(table_scan) => todo!(),
            StatementPlanItem::IndexScan(index_scan) => todo!(),
            StatementPlanItem::Filter(filter) => todo!(),
            StatementPlanItem::Projection(projection) => todo!(),
            StatementPlanItem::Insert(insert) => todo!(),
            StatementPlanItem::CreateTable(create_table) => {
                self.execute_create_table_statement(create_table)
            }
        }
    }

    fn execute_create_table_statement(&self, create_table: &CreateTable) -> StatementResult {
        let column_metadatas = match self.process_columns(create_table) {
            Ok(cm) => cm,
            Err(err) => {
                return self.runtime_error(format!("Failed to create column: {err}"));
            }
        };

        let table_metadata = match TableMetadata::new(
            &create_table.name,
            &column_metadatas,
            &create_table.primary_key_column.name,
        ) {
            Ok(tm) => tm,
            Err(err) => return self.runtime_error(format!("Failed to create table: {}", err)),
        };

        let mut catalog = self.catalog.write();

        if let Err(err) = catalog.add_table(table_metadata) {
            return self.runtime_error(format!("Failed to create table: {}", err));
        }

        match catalog.sync_to_disk() {
            Ok(_) => StatementResult::OperationSuccessful {
                rows_affected: 0,
                ty: StatementType::Create,
            },
            Err(err) => {
                self.runtime_error(format!("Failed to save catalog content to disk: {err}"))
            }
        }
    }

    fn runtime_error(&self, msg: String) -> StatementResult {
        StatementResult::RuntimeError { error: msg }
    }

    fn sort_columns_by_fixed_size<'c>(
        &self,
        create_table: &'c CreateTable,
    ) -> impl Iterator<Item = &'c ResolvedCreateColumnDescriptor> {
        create_table
            .columns
            .iter()
            .chain(once(&create_table.primary_key_column))
            .sorted_by(|a, b| {
                let a_fixed = a.ty.is_fixed_size();
                let b_fixed = b.ty.is_fixed_size();
                b_fixed.cmp(&a_fixed)
            })
    }

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
}

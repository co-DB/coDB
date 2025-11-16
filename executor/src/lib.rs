mod consts;
mod iterators;

use std::{iter::once, sync::Arc};

use dashmap::DashMap;
use engine::{data_types, heap_file::HeapFile, record::Record};
use itertools::Itertools;
use metadata::{
    catalog::{Catalog, ColumnMetadata, TableMetadata},
    types::Type,
};
use parking_lot::RwLock;
use planner::{
    query_plan::{CreateTable, StatementPlan, StatementPlanItem},
    resolved_tree::ResolvedTree,
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
    pub fn new(database_name: &str) -> Self {
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
        let mut column_metadatas = Vec::with_capacity(create_table.columns.len());

        let cols = create_table
            .columns
            .iter()
            .chain(once(&create_table.primary_key_column))
            .sorted_by(|a, b| {
                let a_fixed = a.ty.is_fixed_size();
                let b_fixed = b.ty.is_fixed_size();
                b_fixed.cmp(&a_fixed)
            });

        let mut pos = 0;
        let mut last_fixed_pos = 0;
        let mut base_offset = 0;
        for col in cols {
            let column_metadata = match ColumnMetadata::new(
                col.name.clone(),
                col.ty,
                pos,
                base_offset,
                last_fixed_pos,
            ) {
                Ok(cm) => cm,
                Err(err) => {
                    return StatementResult::RuntimeError {
                        error: format!("Failed to create column: {}", err),
                    };
                }
            };
            column_metadatas.push(column_metadata);
            pos += 1;
            if col.ty.is_fixed_size() {
                let offset = data_types::type_size_on_disk(&col.ty)
                    .expect("type with fixed size must return value");
                last_fixed_pos += 1;
                base_offset += offset;
            }
        }

        let table_metadata = match TableMetadata::new(
            &create_table.name,
            &column_metadatas,
            &create_table.primary_key_column.name,
        ) {
            Ok(tm) => tm,
            Err(err) => {
                return StatementResult::RuntimeError {
                    error: format!("Failed to create table: {}", err),
                };
            }
        };

        match self.catalog.write().add_table(table_metadata) {
            Ok(_) => StatementResult::OperationSuccessful {
                rows_affected: 0,
                ty: StatementType::Create,
            },
            Err(err) => StatementResult::RuntimeError {
                error: format!("Failed to create table: {}", err),
            },
        }
    }
}

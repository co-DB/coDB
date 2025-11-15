mod consts;
mod iterators;

use std::sync::Arc;

use dashmap::DashMap;
use engine::{heap_file::HeapFile, record::Record};
use metadata::{catalog::Catalog, types::Type};
use parking_lot::RwLock;
use planner::{query_plan::StatementPlan, resolved_tree::ResolvedTree};
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
        todo!()
    }
}

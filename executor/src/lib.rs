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
    iterators::{ExecutionIter, ExecutorIterator, ParsingFailedIter},
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

pub enum ExecutionResult {
    OperationSuccessful {
        rows_affected: usize,
    },
    SelectSuccessful {
        columns: Vec<ColumnData>,
        rows: Vec<Record>,
    },
    ParsingFailed {
        error: String,
    },
    ExecutingFailed {
        error: String,
    },
}

impl Executor {
    pub fn new(database_name: &str) -> Self {
        todo!()
    }

    pub fn execute<'e>(&'e self, query: &str) -> ExecutorIterator<'e> {
        let parse_output = planner::process_query(query, self.catalog.clone());
        match parse_output {
            Ok(query_plan) => ExecutionIter::new(query_plan.plans, query_plan.tree, &self).into(),
            Err(errors) => ParsingFailedIter::new(errors).into(),
        }
    }

    fn execute_statement(&self, statement: &StatementPlan, ast: &ResolvedTree) -> ExecutionResult {
        todo!()
    }
}

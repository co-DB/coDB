use planner::{PlannerError, query_plan::StatementPlan, resolved_tree::ResolvedTree};

use crate::{ExecutionResult, Executor};

pub enum ExecutorIterator<'e> {
    ExecutionIter(ExecutionIter<'e>),
    ParsingFailedIter(ParsingFailedIter),
}

impl<'e> Iterator for ExecutorIterator<'e> {
    type Item = ExecutionResult;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ExecutorIterator::ExecutionIter(execution_iter) => execution_iter.next(),
            ExecutorIterator::ParsingFailedIter(parsing_failed_iter) => parsing_failed_iter.next(),
        }
    }
}

pub struct ExecutionIter<'e> {
    statements: Vec<StatementPlan>,
    ast: ResolvedTree,
    pos: usize,
    executor: &'e Executor,
}

impl<'e> ExecutionIter<'e> {
    pub(crate) fn new(
        statements: Vec<StatementPlan>,
        ast: ResolvedTree,
        executor: &'e Executor,
    ) -> Self {
        ExecutionIter {
            statements,
            ast,
            pos: 0,
            executor,
        }
    }
}

impl<'e> Iterator for ExecutionIter<'e> {
    type Item = ExecutionResult;

    fn next(&mut self) -> Option<Self::Item> {
        match self.statements.get(self.pos) {
            Some(statement) => {
                self.pos += 1;
                Some(self.executor.execute_statement(statement, &self.ast))
            }
            None => None,
        }
    }
}

impl<'e> From<ExecutionIter<'e>> for ExecutorIterator<'e> {
    fn from(value: ExecutionIter<'e>) -> Self {
        ExecutorIterator::ExecutionIter(value)
    }
}

pub struct ParsingFailedIter {
    errors: Vec<PlannerError>,
    pos: usize,
}

impl ParsingFailedIter {
    pub(crate) fn new(errors: Vec<PlannerError>) -> Self {
        ParsingFailedIter { errors, pos: 0 }
    }
}

impl Iterator for ParsingFailedIter {
    type Item = ExecutionResult;

    fn next(&mut self) -> Option<Self::Item> {
        match self.errors.get(self.pos) {
            Some(error) => {
                self.pos += 1;
                Some(ExecutionResult::ParsingFailed {
                    error: error.to_string(),
                })
            }
            None => None,
        }
    }
}

impl From<ParsingFailedIter> for ExecutorIterator<'_> {
    fn from(value: ParsingFailedIter) -> Self {
        ExecutorIterator::ParsingFailedIter(value)
    }
}

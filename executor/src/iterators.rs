use planner::{PlannerError, query_plan::StatementPlan, resolved_tree::ResolvedTree};

use crate::{Executor, StatementResult};

pub enum QueryResultIter<'e> {
    Execution(StatementIter<'e>),
    ParseError(ParseErrorIter),
}

impl<'e> Iterator for QueryResultIter<'e> {
    type Item = StatementResult;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            QueryResultIter::Execution(statement_iter) => statement_iter.next(),
            QueryResultIter::ParseError(parsing_failed_iter) => parsing_failed_iter.next(),
        }
    }
}

pub struct StatementIter<'e> {
    statements: Vec<StatementPlan>,
    ast: ResolvedTree,
    pos: usize,
    executor: &'e Executor,
}

impl<'e> StatementIter<'e> {
    pub(crate) fn new(
        statements: Vec<StatementPlan>,
        ast: ResolvedTree,
        executor: &'e Executor,
    ) -> Self {
        StatementIter {
            statements,
            ast,
            pos: 0,
            executor,
        }
    }
}

impl<'e> Iterator for StatementIter<'e> {
    type Item = StatementResult;

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

impl<'e> From<StatementIter<'e>> for QueryResultIter<'e> {
    fn from(value: StatementIter<'e>) -> Self {
        QueryResultIter::Execution(value)
    }
}

pub struct ParseErrorIter {
    errors: Vec<PlannerError>,
    pos: usize,
}

impl ParseErrorIter {
    pub(crate) fn new(errors: Vec<PlannerError>) -> Self {
        ParseErrorIter { errors, pos: 0 }
    }
}

impl Iterator for ParseErrorIter {
    type Item = StatementResult;

    fn next(&mut self) -> Option<Self::Item> {
        match self.errors.get(self.pos) {
            Some(error) => {
                self.pos += 1;
                Some(StatementResult::ParseError {
                    error: error.to_string(),
                })
            }
            None => None,
        }
    }
}

impl From<ParseErrorIter> for QueryResultIter<'_> {
    fn from(value: ParseErrorIter) -> Self {
        QueryResultIter::ParseError(value)
    }
}

use std::sync::Arc;

use metadata::catalog::Catalog;
use parking_lot::RwLock;
use thiserror::Error;

use crate::{
    analyzer::{Analyzer, AnalyzerError},
    parser::{Parser, ParserError},
    query_plan::QueryPlan,
    query_planner::QueryPlanner,
};

mod analyzer;
mod ast;
mod lexer;
pub mod operators;
mod parser;
pub mod query_plan;
mod query_planner;
pub mod resolved_tree;
mod tokens;

#[derive(Debug, Error)]
pub enum PlannerError {
    #[error("{0}")]
    Parser(#[from] ParserError),
    #[error("{0}")]
    Analyzer(#[from] AnalyzerError),
}

/// Helper trait for parsing `Vec<E> -> Vec<F>`, where `E` and `F` are errors.
trait VecErrorExt<T, E> {
    fn map_errors<F>(self) -> Result<T, Vec<F>>
    where
        E: Into<F>;
}

impl<T, E> VecErrorExt<T, E> for Result<T, Vec<E>> {
    fn map_errors<F>(self) -> Result<T, Vec<F>>
    where
        E: Into<F>,
    {
        self.map_err(|errors| errors.into_iter().map(Into::into).collect())
    }
}

pub fn process_query(
    query_str: &str,
    catalog: Arc<RwLock<Catalog>>,
) -> Result<QueryPlan, Vec<PlannerError>> {
    let parser = Parser::new(&query_str);
    let ast = parser.parse_program().map_errors()?;
    let analyzer = Analyzer::new(&ast, catalog);
    let resolved_ast = analyzer.analyze().map_errors()?;
    let planner = QueryPlanner::new(resolved_ast);
    let query_plan = planner.plan_query();
    Ok(query_plan)
}

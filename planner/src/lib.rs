use std::sync::Arc;

use metadata::catalog::Catalog;
use parking_lot::RwLock;
use thiserror::Error;

use crate::{
    analyzer::Analyzer, parser::Parser, query_plan::QueryPlan, query_planner::QueryPlanner,
};

mod analyzer;
mod ast;
mod lexer;
mod operators;
mod parser;
pub mod query_plan;
mod query_planner;
pub mod resolved_tree;
mod tokens;

#[derive(Debug, Error)]
pub enum PlannerError {
    // TODO: fill this and remove unwraps from `process_query`
}

pub fn process_query(
    query_str: &str,
    catalog: Arc<RwLock<Catalog>>,
) -> Result<QueryPlan, Vec<PlannerError>> {
    let parser = Parser::new(&query_str);
    let ast = parser.parse_program().unwrap();
    let analyzer = Analyzer::new(&ast, catalog);
    let resolved_ast = analyzer.analyze().unwrap();
    let planner = QueryPlanner::new(resolved_ast);
    let query_plan = planner.plan_query();
    Ok(query_plan)
}

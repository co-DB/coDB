use std::borrow::Cow;

use engine::record::{Field, Record};
use planner::{
    operators::LogicalOperator,
    resolved_tree::{
        ResolvedColumn, ResolvedExpression, ResolvedLogicalExpression, ResolvedNodeId, ResolvedTree,
    },
};

use crate::InternalExecutorError;

struct ExpressionExecutor<'r, 'q> {
    record: &'r Record,
    ast: &'q ResolvedTree,
}

impl<'r, 'q> ExpressionExecutor<'r, 'q>
where
    'q: 'r,
{
    fn new(record: &'r Record, ast: &'q ResolvedTree) -> Self {
        Self { record, ast }
    }

    fn execute_expression(
        &self,
        expression: ResolvedNodeId,
    ) -> Result<Cow<'r, Field>, InternalExecutorError> {
        let node = self.ast.node(expression);
        match node {
            ResolvedExpression::ColumnRef(column) => self.execute_column_ref(column),
            ResolvedExpression::Logical(logical) => self.execute_logical(logical),
            ResolvedExpression::Binary(resolved_binary_expression) => todo!(),
            ResolvedExpression::Unary(resolved_unary_expression) => todo!(),
            ResolvedExpression::Cast(resolved_cast) => todo!(),
            ResolvedExpression::Literal(resolved_literal) => todo!(),
            _ => Err(InternalExecutorError::InvalidNodeTypeInExpression {
                node_type: format!("{:?}", node),
            }),
        }
    }

    fn execute_column_ref(
        &self,
        column: &'r ResolvedColumn,
    ) -> Result<Cow<'r, Field>, InternalExecutorError> {
        Ok(Cow::Borrowed(&self.record.fields[column.pos as usize]))
    }

    fn execute_logical(
        &self,
        logical: &ResolvedLogicalExpression,
    ) -> Result<Cow<'r, Field>, InternalExecutorError> {
        let left_expr = self.execute_expression(logical.left)?;
        let left = left_expr
            .as_bool()
            .ok_or(self.unexpected_type("bool", &left_expr))?;

        match logical.op {
            LogicalOperator::And if !left => return Ok(Cow::Owned(Field::Bool(false))),
            LogicalOperator::Or if left => return Ok(Cow::Owned(Field::Bool(true))),
            _ => {}
        }

        let right_expr = self.execute_expression(logical.right)?;
        let right = right_expr
            .as_bool()
            .ok_or(self.unexpected_type("bool", &right_expr))?;
        let result = match logical.op {
            LogicalOperator::And => left && right,
            LogicalOperator::Or => left || right,
        };

        Ok(Cow::Owned(Field::Bool(result)))
    }

    fn unexpected_type(&self, expected: impl Into<String>, got: &Field) -> InternalExecutorError {
        InternalExecutorError::UnexpectedType {
            expected: expected.into(),
            got: format!("{:?}", got),
        }
    }
}

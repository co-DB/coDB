use std::borrow::Cow;

use engine::{
    data_types::{DbDate, DbDateTime},
    record::{Field, Record},
};
use metadata::types::Type;
use planner::{
    operators::{BinaryOperator, LogicalOperator, UnaryOperator},
    resolved_tree::{
        ResolvedBinaryExpression, ResolvedCast, ResolvedColumn, ResolvedExpression,
        ResolvedLiteral, ResolvedLogicalExpression, ResolvedNodeId, ResolvedTree,
        ResolvedUnaryExpression,
    },
};

use crate::{InternalExecutorError, error_factory};

pub(crate) struct ExpressionExecutor<'r, 'q> {
    record: &'r Record,
    ast: &'q ResolvedTree,
}

impl<'r, 'q> ExpressionExecutor<'r, 'q>
where
    'q: 'r,
{
    pub(crate) fn new(record: &'r Record, ast: &'q ResolvedTree) -> Self {
        Self { record, ast }
    }

    pub(crate) fn execute_expression(
        &self,
        expression: ResolvedNodeId,
    ) -> Result<Cow<'r, Field>, InternalExecutorError> {
        let node = self.ast.node(expression);
        match node {
            ResolvedExpression::ColumnRef(column) => self.execute_column_ref(column),
            ResolvedExpression::Logical(logical) => self.execute_logical(logical),
            ResolvedExpression::Binary(binary) => self.execute_binary(binary),
            ResolvedExpression::Unary(unary) => self.execute_unary(unary),
            ResolvedExpression::Cast(cast) => self.execute_cast(cast),
            ResolvedExpression::Literal(literal) => self.execute_literal(literal),
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
            .ok_or(error_factory::unexpected_type("bool", &left_expr))?;

        match logical.op {
            LogicalOperator::And if !left => return Ok(Cow::Owned(Field::Bool(false))),
            LogicalOperator::Or if left => return Ok(Cow::Owned(Field::Bool(true))),
            _ => {}
        }

        let right_expr = self.execute_expression(logical.right)?;
        let right = right_expr
            .as_bool()
            .ok_or(error_factory::unexpected_type("bool", &right_expr))?;
        let result = match logical.op {
            LogicalOperator::And => left && right,
            LogicalOperator::Or => left || right,
        };

        Ok(Cow::Owned(Field::Bool(result)))
    }

    fn execute_binary(
        &self,
        binary: &ResolvedBinaryExpression,
    ) -> Result<Cow<'r, Field>, InternalExecutorError> {
        let left = self.execute_expression(binary.left)?;
        let right = self.execute_expression(binary.right)?;

        let lhs = left.as_ref();
        let rhs = right.as_ref();

        let result = match binary.op {
            BinaryOperator::Plus => self.arithmetic_add(lhs, rhs)?,
            BinaryOperator::Minus => self.arithmetic_sub(lhs, rhs)?,
            BinaryOperator::Star => self.arithmetic_mul(lhs, rhs)?,
            BinaryOperator::Slash => self.arithmetic_div(lhs, rhs)?,
            BinaryOperator::Modulo => self.arithmetic_mod(lhs, rhs)?,
            BinaryOperator::Equal => Field::Bool(lhs == rhs),
            BinaryOperator::NotEqual => Field::Bool(lhs != rhs),
            BinaryOperator::Greater => self.compare_lt(rhs, lhs)?,
            BinaryOperator::GreaterEqual => self.compare_lte(rhs, lhs)?,
            BinaryOperator::Less => self.compare_lt(lhs, rhs)?,
            BinaryOperator::LessEqual => self.compare_lte(lhs, rhs)?,
        };

        Ok(Cow::Owned(result))
    }

    fn execute_unary(
        &self,
        unary: &ResolvedUnaryExpression,
    ) -> Result<Cow<'r, Field>, InternalExecutorError> {
        let expr = self.execute_expression(unary.expression)?;
        let value = expr.as_ref();
        let result = match unary.op {
            UnaryOperator::Plus => self.make_positive(value)?,
            UnaryOperator::Minus => self.make_negative(value)?,
            UnaryOperator::Bang => self.logical_bang(value)?,
        };
        Ok(Cow::Owned(result))
    }

    fn execute_cast(&self, cast: &ResolvedCast) -> Result<Cow<'r, Field>, InternalExecutorError> {
        let child = self.execute_expression(cast.child)?;
        let child = child.as_ref();
        let field = match (child, cast.new_ty) {
            (Field::Int32(previous), Type::I64) => Field::Int64(*previous as i64),
            (Field::Float32(previous), Type::F64) => Field::Float64(*previous as f64),
            _ => return Err(error_factory::invalid_cast(child, &cast.new_ty)),
        };
        Ok(Cow::Owned(field))
    }

    fn execute_literal(
        &self,
        literal: &ResolvedLiteral,
    ) -> Result<Cow<'r, Field>, InternalExecutorError> {
        let field = match literal {
            ResolvedLiteral::String(s) => Field::String(s.clone()),
            ResolvedLiteral::Float32(f32) => Field::Float32(*f32),
            ResolvedLiteral::Float64(f64) => Field::Float64(*f64),
            ResolvedLiteral::Int32(i32) => Field::Int32(*i32),
            ResolvedLiteral::Int64(i64) => Field::Int64(*i64),
            ResolvedLiteral::Bool(b) => Field::Bool(*b),
            ResolvedLiteral::Date(date) => Field::Date(DbDate::from(*date)),
            ResolvedLiteral::DateTime(primitive_date_time) => {
                Field::DateTime(DbDateTime::from(*primitive_date_time))
            }
        };
        Ok(Cow::Owned(field))
    }

    fn arithmetic_add(&self, lhs: &Field, rhs: &Field) -> Result<Field, InternalExecutorError> {
        let field = match (lhs, rhs) {
            (Field::Int32(lhs), Field::Int32(rhs)) => Field::Int32(lhs + rhs),
            (Field::Int64(lhs), Field::Int64(rhs)) => Field::Int64(lhs + rhs),
            (Field::Float32(lhs), Field::Float32(rhs)) => Field::Float32(lhs + rhs),
            (Field::Float64(lhs), Field::Float64(rhs)) => Field::Float64(lhs + rhs),
            (Field::String(lhs), Field::String(rhs)) => Field::String(format!("{lhs}{rhs}")),
            _ => return Err(error_factory::incompatible_types(lhs, rhs)),
        };
        Ok(field)
    }

    fn arithmetic_sub(&self, lhs: &Field, rhs: &Field) -> Result<Field, InternalExecutorError> {
        let field = match (lhs, rhs) {
            (Field::Int32(lhs), Field::Int32(rhs)) => Field::Int32(lhs - rhs),
            (Field::Int64(lhs), Field::Int64(rhs)) => Field::Int64(lhs - rhs),
            (Field::Float32(lhs), Field::Float32(rhs)) => Field::Float32(lhs - rhs),
            (Field::Float64(lhs), Field::Float64(rhs)) => Field::Float64(lhs - rhs),
            _ => return Err(error_factory::incompatible_types(lhs, rhs)),
        };
        Ok(field)
    }

    fn arithmetic_mul(&self, lhs: &Field, rhs: &Field) -> Result<Field, InternalExecutorError> {
        let field = match (lhs, rhs) {
            (Field::Int32(lhs), Field::Int32(rhs)) => Field::Int32(lhs * rhs),
            (Field::Int64(lhs), Field::Int64(rhs)) => Field::Int64(lhs * rhs),
            (Field::Float32(lhs), Field::Float32(rhs)) => Field::Float32(lhs * rhs),
            (Field::Float64(lhs), Field::Float64(rhs)) => Field::Float64(lhs * rhs),
            _ => return Err(error_factory::incompatible_types(lhs, rhs)),
        };
        Ok(field)
    }

    fn arithmetic_div(&self, lhs: &Field, rhs: &Field) -> Result<Field, InternalExecutorError> {
        let field = match (lhs, rhs) {
            (Field::Int32(lhs), Field::Int32(rhs)) => {
                if *rhs == 0 {
                    return Err(error_factory::div_by_zero());
                }
                Field::Int32(lhs / rhs)
            }
            (Field::Int64(lhs), Field::Int64(rhs)) => {
                if *rhs == 0 {
                    return Err(error_factory::div_by_zero());
                }
                Field::Int64(lhs / rhs)
            }
            (Field::Float32(lhs), Field::Float32(rhs)) => {
                if *rhs == 0.0 {
                    return Err(error_factory::div_by_zero());
                }
                Field::Float32(lhs / rhs)
            }
            (Field::Float64(lhs), Field::Float64(rhs)) => {
                if *rhs == 0.0 {
                    return Err(error_factory::div_by_zero());
                }
                Field::Float64(lhs / rhs)
            }
            _ => return Err(error_factory::incompatible_types(lhs, rhs)),
        };
        Ok(field)
    }

    fn arithmetic_mod(&self, lhs: &Field, rhs: &Field) -> Result<Field, InternalExecutorError> {
        let field = match (lhs, rhs) {
            (Field::Int32(lhs), Field::Int32(rhs)) => {
                if *rhs == 0 {
                    return Err(error_factory::mod_by_zero());
                }
                Field::Int32(lhs % rhs)
            }
            (Field::Int64(lhs), Field::Int64(rhs)) => {
                if *rhs == 0 {
                    return Err(error_factory::mod_by_zero());
                }
                Field::Int64(lhs % rhs)
            }
            _ => return Err(error_factory::incompatible_types(lhs, rhs)),
        };
        Ok(field)
    }

    fn compare_lt(&self, lhs: &Field, rhs: &Field) -> Result<Field, InternalExecutorError> {
        let result = match (lhs, rhs) {
            (Field::Int32(lhs), Field::Int32(rhs)) => lhs < rhs,
            (Field::Int64(lhs), Field::Int64(rhs)) => lhs < rhs,
            (Field::Float32(lhs), Field::Float32(rhs)) => lhs < rhs,
            (Field::Float64(lhs), Field::Float64(rhs)) => lhs < rhs,
            (Field::String(lhs), Field::String(rhs)) => lhs < rhs,
            (Field::Date(lhs), Field::Date(rhs)) => lhs < rhs,
            (Field::DateTime(lhs), Field::DateTime(rhs)) => lhs < rhs,
            _ => return Err(error_factory::incompatible_types(lhs, rhs)),
        };
        Ok(Field::Bool(result))
    }

    fn compare_lte(&self, lhs: &Field, rhs: &Field) -> Result<Field, InternalExecutorError> {
        let result = match (lhs, rhs) {
            (Field::Int32(lhs), Field::Int32(rhs)) => lhs <= rhs,
            (Field::Int64(lhs), Field::Int64(rhs)) => lhs <= rhs,
            (Field::Float32(lhs), Field::Float32(rhs)) => lhs <= rhs,
            (Field::Float64(lhs), Field::Float64(rhs)) => lhs <= rhs,
            (Field::String(lhs), Field::String(rhs)) => lhs <= rhs,
            (Field::Date(lhs), Field::Date(rhs)) => lhs <= rhs,
            (Field::DateTime(lhs), Field::DateTime(rhs)) => lhs <= rhs,
            _ => return Err(error_factory::incompatible_types(lhs, rhs)),
        };
        Ok(Field::Bool(result))
    }

    fn logical_bang(&self, value: &Field) -> Result<Field, InternalExecutorError> {
        let value = value
            .as_bool()
            .ok_or(error_factory::unexpected_type("bool", value))?;
        Ok(Field::Bool(!value))
    }

    fn make_positive(&self, value: &Field) -> Result<Field, InternalExecutorError> {
        match value {
            Field::Int32(value) => Ok(Field::Int32(value.abs())),
            Field::Int64(value) => Ok(Field::Int64(value.abs())),
            Field::Float32(value) => Ok(Field::Float32(value.abs())),
            Field::Float64(value) => Ok(Field::Float64(value.abs())),
            _ => Err(error_factory::unexpected_type("any numeric type", value)),
        }
    }

    fn make_negative(&self, value: &Field) -> Result<Field, InternalExecutorError> {
        match value {
            Field::Int32(value) => Ok(Field::Int32(-value)),
            Field::Int64(value) => Ok(Field::Int64(-value)),
            Field::Float32(value) => Ok(Field::Float32(-value)),
            Field::Float64(value) => Ok(Field::Float64(-value)),
            _ => Err(error_factory::unexpected_type("any numeric type", value)),
        }
    }
}
#[cfg(test)]
mod tests {
    use planner::query_plan::StatementPlanItem;

    use crate::tests::{create_single_statement, create_test_executor};

    use super::*;
    fn parse_expression(sql_expr: &str) -> (ResolvedTree, ResolvedNodeId) {
        let (executor, _temp_dir) = create_test_executor();

        let (create_plan, create_ast) = create_single_statement(
            "CREATE TABLE test_table (id INT32 PRIMARY_KEY, age INT32, name STRING, score FLOAT64, active BOOL);",
            &executor,
        );
        executor.execute_statement(&create_plan, &create_ast);

        let full_sql = format!("SELECT id FROM test_table WHERE {};", sql_expr);
        let (statement_plan, tree) = create_single_statement(&full_sql, &executor);
        drop(executor);

        let projection = statement_plan.root();
        let projection = match projection {
            StatementPlanItem::Projection(p) => p,
            _ => panic!("Expected Projection as root"),
        };

        let filter = statement_plan.item(projection.data_source);
        let filter = match filter {
            StatementPlanItem::Filter(f) => f,
            _ => panic!("Expected Filter below Projection"),
        };

        (tree, filter.predicate)
    }

    fn create_test_record() -> Record {
        Record::new(vec![
            Field::Int32(1),
            Field::Int32(25),
            Field::Float64(100.5),
            Field::Bool(true),
            Field::String("Alice".into()),
        ])
    }

    #[test]
    fn test_execute_column_ref() {
        let (tree, expr_id) = parse_expression("age = age");
        let record = create_test_record();

        let executor = ExpressionExecutor::new(&record, &tree);
        let result = executor.execute_expression(expr_id).unwrap();

        assert_eq!(result.as_ref(), &Field::Bool(true));
    }

    #[test]
    fn test_execute_literal_int() {
        let (tree, expr_id) = parse_expression("42 = 42");
        let record = create_test_record();

        let executor = ExpressionExecutor::new(&record, &tree);
        let result = executor.execute_expression(expr_id).unwrap();

        assert_eq!(result.as_ref(), &Field::Bool(true));
    }

    #[test]
    fn test_arithmetic_add() {
        let (tree, expr_id) = parse_expression("5 + 3 = 8");
        let record = create_test_record();

        let executor = ExpressionExecutor::new(&record, &tree);
        let result = executor.execute_expression(expr_id).unwrap();

        assert_eq!(result.as_ref(), &Field::Bool(true));
    }

    #[test]
    fn test_comparison_greater_than() {
        let (tree, expr_id) = parse_expression("10 > 5");
        let record = create_test_record();

        let executor = ExpressionExecutor::new(&record, &tree);
        let result = executor.execute_expression(expr_id).unwrap();

        assert_eq!(result.as_ref(), &Field::Bool(true));
    }

    #[test]
    fn test_string_equality() {
        let (tree, expr_id) = parse_expression("name = 'Alice'");
        let record = create_test_record();

        let executor = ExpressionExecutor::new(&record, &tree);
        let result = executor.execute_expression(expr_id).unwrap();

        assert_eq!(result.as_ref(), &Field::Bool(true));
    }

    #[test]
    fn test_complex_expression() {
        let (tree, expr_id) = parse_expression("age > 20 AND active = true");
        let record = create_test_record();

        let executor = ExpressionExecutor::new(&record, &tree);
        let result = executor.execute_expression(expr_id).unwrap();

        assert_eq!(result.as_ref(), &Field::Bool(true));
    }

    #[test]
    fn test_division_by_zero() {
        let (tree, expr_id) = parse_expression("10 / 0 = 0");
        let record = create_test_record();

        let executor = ExpressionExecutor::new(&record, &tree);
        let result = executor.execute_expression(expr_id);

        assert!(result.is_err());
        match result {
            Err(InternalExecutorError::ArithmeticOperationError { message }) => {
                assert!(message.contains("divide by 0"));
            }
            _ => panic!("Expected division by zero error, got {:?}", result),
        }
    }

    #[test]
    fn test_modulo_by_zero() {
        let (tree, expr_id) = parse_expression("10 % 0 = 0");
        let record = create_test_record();

        let executor = ExpressionExecutor::new(&record, &tree);
        let result = executor.execute_expression(expr_id);

        assert!(result.is_err());
        match result {
            Err(InternalExecutorError::ArithmeticOperationError { message }) => {
                assert!(message.contains("modulo by 0"));
            }
            _ => panic!("Expected modulo by zero error, got {:?}", result),
        }
    }

    #[test]
    fn test_unary_minus() {
        let (tree, expr_id) = parse_expression("-age = -25");
        let record = create_test_record();

        let executor = ExpressionExecutor::new(&record, &tree);
        let result = executor.execute_expression(expr_id).unwrap();

        assert_eq!(result.as_ref(), &Field::Bool(true));
    }

    #[test]
    fn test_logical_bang() {
        let (tree, expr_id) = parse_expression("!active");
        let record = create_test_record();

        let executor = ExpressionExecutor::new(&record, &tree);
        let result = executor.execute_expression(expr_id).unwrap();

        assert_eq!(result.as_ref(), &Field::Bool(false));
    }

    #[test]
    fn test_column_with_arithmetic() {
        let (tree, expr_id) = parse_expression("age + 5 = 30");
        let record = create_test_record();

        let executor = ExpressionExecutor::new(&record, &tree);
        let result = executor.execute_expression(expr_id).unwrap();

        assert_eq!(result.as_ref(), &Field::Bool(true));
    }

    #[test]
    fn test_float_arithmetic() {
        let (tree, expr_id) = parse_expression("score + 0.5 > 100.0");
        let record = create_test_record();

        let executor = ExpressionExecutor::new(&record, &tree);
        let result = executor.execute_expression(expr_id).unwrap();

        assert_eq!(result.as_ref(), &Field::Bool(true));
    }

    #[test]
    fn test_multiple_columns() {
        let (tree, expr_id) = parse_expression("age > 20 AND score > 90.0");
        let record = create_test_record();

        let executor = ExpressionExecutor::new(&record, &tree);
        let result = executor.execute_expression(expr_id).unwrap();

        assert_eq!(result.as_ref(), &Field::Bool(true));
    }

    #[test]
    fn test_logical_or_short_circuit() {
        let (tree, expr_id) = parse_expression("active = true OR age < 0");
        let record = create_test_record();

        let executor = ExpressionExecutor::new(&record, &tree);
        let result = executor.execute_expression(expr_id).unwrap();

        // Should be true due to short-circuit (first condition is true)
        assert_eq!(result.as_ref(), &Field::Bool(true));
    }

    #[test]
    fn test_logical_and_short_circuit() {
        let (tree, expr_id) = parse_expression("!active AND age > 0");
        let record = create_test_record();

        let executor = ExpressionExecutor::new(&record, &tree);
        let result = executor.execute_expression(expr_id).unwrap();

        // Should be false due to short-circuit (first condition is false)
        assert_eq!(result.as_ref(), &Field::Bool(false));
    }

    #[test]
    fn test_nested_expressions() {
        let (tree, expr_id) = parse_expression("(age + 5) * 2 = 60");
        let record = create_test_record();

        let executor = ExpressionExecutor::new(&record, &tree);
        let result = executor.execute_expression(expr_id).unwrap();

        assert_eq!(result.as_ref(), &Field::Bool(true));
    }

    #[test]
    fn test_string_concatenation() {
        let (tree, expr_id) = parse_expression("name + ' Smith' = 'Alice Smith'");
        let record = create_test_record();

        let executor = ExpressionExecutor::new(&record, &tree);
        let result = executor.execute_expression(expr_id).unwrap();

        assert_eq!(result.as_ref(), &Field::Bool(true));
    }
}

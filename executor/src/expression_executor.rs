use std::{borrow::Cow, collections::HashMap};

use engine::record::{Field, Record};
use planner::{
    operators::{BinaryOperator, LogicalOperator, UnaryOperator},
    resolved_tree::{
        ResolvedBinaryExpression, ResolvedCast, ResolvedColumn, ResolvedExpression,
        ResolvedLiteral, ResolvedLogicalExpression, ResolvedNodeId, ResolvedTree,
        ResolvedUnaryExpression,
    },
};
use types::{
    data::{DbDate, DbDateTime, Value},
    schema::Type,
};

use crate::{InternalExecutorError, error_factory};

// TODO: once joins are implemented add tests for cases in which ExpressionContext::Multiple is used

/// Context for expression evaluation, containing the record(s) being evaluated.
pub(crate) enum ExpressionContext<'r, 'q> {
    /// No record context (e.g., evaluating constants)
    Empty,
    /// Single record context (e.g., filter, projection)
    /// Uses direct position-based field access
    Single(&'r Record),
    /// Multiple record context (e.g., joins)
    /// Maps table name to record for table-aware column resolution
    Multiple(HashMap<&'q str, &'r Record>),
}

impl<'r, 'q> ExpressionContext<'r, 'q> {
    /// Get a field from the context.
    fn get_field(&self, table_name: &str, pos: u16) -> Option<&'r Field> {
        match self {
            ExpressionContext::Empty => None,
            ExpressionContext::Single(record) => record.fields.get(pos as usize),
            ExpressionContext::Multiple(tables) => tables
                .get(table_name)
                .and_then(|record| record.fields.get(pos as usize)),
        }
    }
}

/// Evaluates expressions from the resolved query AST against context.
///
/// The executor borrows both the record(s) being evaluated and the AST,
/// allowing it to reference fields without copying when possible.
pub(crate) struct ExpressionExecutor<'r, 'q> {
    context: ExpressionContext<'r, 'q>,
    ast: &'q ResolvedTree,
}

impl<'r, 'q> ExpressionExecutor<'r, 'q>
where
    'q: 'r,
{
    pub(crate) fn new(context: ExpressionContext<'r, 'q>, ast: &'q ResolvedTree) -> Self {
        Self { context, ast }
    }

    pub(crate) fn with_single_record(record: &'r Record, ast: &'q ResolvedTree) -> Self {
        Self::new(ExpressionContext::Single(record), ast)
    }

    pub(crate) fn with_multiple_records(
        tables: HashMap<&'q str, &'r Record>,
        ast: &'q ResolvedTree,
    ) -> Self {
        Self::new(ExpressionContext::Multiple(tables), ast)
    }

    pub(crate) fn empty(ast: &'q ResolvedTree) -> Self {
        Self::new(ExpressionContext::Empty, ast)
    }

    /// Evaluates an expression node from the AST and returns its computed value.
    ///
    /// This is the main entry point for expression evaluation. It recursively walks
    /// the expression tree, dispatching to specialized handlers based on the node type.
    ///
    /// Returns `Cow<'r, Field>` to avoid unnecessary cloning:
    /// - `Cow::Borrowed` is returned for column references, directly referencing
    ///   the field from the record without copying
    /// - `Cow::Owned` is returned for computed values (arithmetic, comparisons, literals)
    ///   that must be newly allocated
    pub(crate) fn execute_expression(
        &self,
        expression: ResolvedNodeId,
    ) -> Result<Cow<'r, Value>, InternalExecutorError> {
        let node = self.ast.node(expression);
        match node {
            ResolvedExpression::ColumnRef(column) => self.execute_column_ref(column),
            ResolvedExpression::Logical(logical) => self.execute_logical(logical),
            ResolvedExpression::Binary(binary) => self.execute_binary(binary),
            ResolvedExpression::Unary(unary) => self.execute_unary(unary),
            ResolvedExpression::Cast(cast) => self.execute_cast(cast),
            ResolvedExpression::Literal(literal) => self.execute_literal(literal),
            _ => Err(error_factory::invalid_node_type(format!("{:?}", node))),
        }
    }

    fn execute_column_ref(
        &self,
        column: &'r ResolvedColumn,
    ) -> Result<Cow<'r, Value>, InternalExecutorError> {
        let table_name = self.table_name(column.table)?;
        self.context
            .get_field(table_name, column.pos)
            .map(|field| Cow::Borrowed(field.value()))
            .ok_or(error_factory::cannot_load_column_from_context(&column.name))
    }

    fn execute_logical(
        &self,
        logical: &ResolvedLogicalExpression,
    ) -> Result<Cow<'r, Value>, InternalExecutorError> {
        let left_expr = self.execute_expression(logical.left)?;
        let left = left_expr
            .as_bool()
            .ok_or(error_factory::unexpected_type("bool", &left_expr))?;

        match logical.op {
            LogicalOperator::And if !left => return Ok(Cow::Owned(Value::Bool(false))),
            LogicalOperator::Or if left => return Ok(Cow::Owned(Value::Bool(true))),
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

        Ok(Cow::Owned(Value::Bool(result)))
    }

    fn execute_binary(
        &self,
        binary: &ResolvedBinaryExpression,
    ) -> Result<Cow<'r, Value>, InternalExecutorError> {
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
            BinaryOperator::Equal => Value::Bool(lhs == rhs),
            BinaryOperator::NotEqual => Value::Bool(lhs != rhs),
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
    ) -> Result<Cow<'r, Value>, InternalExecutorError> {
        let expr = self.execute_expression(unary.expression)?;
        let value = expr.as_ref();
        let result = match unary.op {
            UnaryOperator::Plus => self.identity(value)?,
            UnaryOperator::Minus => self.negate(value)?,
            UnaryOperator::Bang => self.logical_negate(value)?,
        };
        Ok(Cow::Owned(result))
    }

    fn execute_cast(&self, cast: &ResolvedCast) -> Result<Cow<'r, Value>, InternalExecutorError> {
        let child = self.execute_expression(cast.child)?;
        let child = child.as_ref();
        let value = match (child, cast.new_ty) {
            (Value::Int32(previous), Type::I64) => Value::Int64(*previous as i64),
            (Value::Float32(previous), Type::F64) => Value::Float64(*previous as f64),
            _ => return Err(error_factory::invalid_cast(child, &cast.new_ty)),
        };
        Ok(Cow::Owned(value))
    }

    fn execute_literal(
        &self,
        literal: &ResolvedLiteral,
    ) -> Result<Cow<'r, Value>, InternalExecutorError> {
        let field = match literal {
            ResolvedLiteral::String(s) => Value::String(s.clone()),
            ResolvedLiteral::Float32(f32) => Value::Float32(*f32),
            ResolvedLiteral::Float64(f64) => Value::Float64(*f64),
            ResolvedLiteral::Int32(i32) => Value::Int32(*i32),
            ResolvedLiteral::Int64(i64) => Value::Int64(*i64),
            ResolvedLiteral::Bool(b) => Value::Bool(*b),
            ResolvedLiteral::Date(date) => Value::Date(DbDate::from(*date)),
            ResolvedLiteral::DateTime(primitive_date_time) => {
                Value::DateTime(DbDateTime::from(*primitive_date_time))
            }
        };
        Ok(Cow::Owned(field))
    }

    /// Computes `lhs + rhs`.
    fn arithmetic_add(&self, lhs: &Value, rhs: &Value) -> Result<Value, InternalExecutorError> {
        let value = match (lhs, rhs) {
            (Value::Int32(lhs), Value::Int32(rhs)) => Value::Int32(lhs + rhs),
            (Value::Int64(lhs), Value::Int64(rhs)) => Value::Int64(lhs + rhs),
            (Value::Float32(lhs), Value::Float32(rhs)) => Value::Float32(lhs + rhs),
            (Value::Float64(lhs), Value::Float64(rhs)) => Value::Float64(lhs + rhs),
            (Value::String(lhs), Value::String(rhs)) => Value::String(format!("{lhs}{rhs}")),
            _ => return Err(error_factory::incompatible_types(lhs, rhs)),
        };
        Ok(value)
    }

    /// Computes `lhs - rhs`.
    fn arithmetic_sub(&self, lhs: &Value, rhs: &Value) -> Result<Value, InternalExecutorError> {
        let value = match (lhs, rhs) {
            (Value::Int32(lhs), Value::Int32(rhs)) => Value::Int32(lhs - rhs),
            (Value::Int64(lhs), Value::Int64(rhs)) => Value::Int64(lhs - rhs),
            (Value::Float32(lhs), Value::Float32(rhs)) => Value::Float32(lhs - rhs),
            (Value::Float64(lhs), Value::Float64(rhs)) => Value::Float64(lhs - rhs),
            _ => return Err(error_factory::incompatible_types(lhs, rhs)),
        };
        Ok(value)
    }

    /// Computes `lhs * rhs`.
    fn arithmetic_mul(&self, lhs: &Value, rhs: &Value) -> Result<Value, InternalExecutorError> {
        let value = match (lhs, rhs) {
            (Value::Int32(lhs), Value::Int32(rhs)) => Value::Int32(lhs * rhs),
            (Value::Int64(lhs), Value::Int64(rhs)) => Value::Int64(lhs * rhs),
            (Value::Float32(lhs), Value::Float32(rhs)) => Value::Float32(lhs * rhs),
            (Value::Float64(lhs), Value::Float64(rhs)) => Value::Float64(lhs * rhs),
            _ => return Err(error_factory::incompatible_types(lhs, rhs)),
        };
        Ok(value)
    }

    /// Computes `lhs / rhs`.
    fn arithmetic_div(&self, lhs: &Value, rhs: &Value) -> Result<Value, InternalExecutorError> {
        let value = match (lhs, rhs) {
            (Value::Int32(lhs), Value::Int32(rhs)) => {
                if *rhs == 0 {
                    return Err(error_factory::div_by_zero());
                }
                Value::Int32(lhs / rhs)
            }
            (Value::Int64(lhs), Value::Int64(rhs)) => {
                if *rhs == 0 {
                    return Err(error_factory::div_by_zero());
                }
                Value::Int64(lhs / rhs)
            }
            (Value::Float32(lhs), Value::Float32(rhs)) => {
                if *rhs == 0.0 {
                    return Err(error_factory::div_by_zero());
                }
                Value::Float32(lhs / rhs)
            }
            (Value::Float64(lhs), Value::Float64(rhs)) => {
                if *rhs == 0.0 {
                    return Err(error_factory::div_by_zero());
                }
                Value::Float64(lhs / rhs)
            }
            _ => return Err(error_factory::incompatible_types(lhs, rhs)),
        };
        Ok(value)
    }

    /// Computes `lhs % rhs`.
    fn arithmetic_mod(&self, lhs: &Value, rhs: &Value) -> Result<Value, InternalExecutorError> {
        let value = match (lhs, rhs) {
            (Value::Int32(lhs), Value::Int32(rhs)) => {
                if *rhs == 0 {
                    return Err(error_factory::mod_by_zero());
                }
                Value::Int32(lhs % rhs)
            }
            (Value::Int64(lhs), Value::Int64(rhs)) => {
                if *rhs == 0 {
                    return Err(error_factory::mod_by_zero());
                }
                Value::Int64(lhs % rhs)
            }
            _ => return Err(error_factory::incompatible_types(lhs, rhs)),
        };
        Ok(value)
    }

    /// Computes `lhs < rhs`.
    fn compare_lt(&self, lhs: &Value, rhs: &Value) -> Result<Value, InternalExecutorError> {
        let result = match (lhs, rhs) {
            (Value::Int32(lhs), Value::Int32(rhs)) => lhs < rhs,
            (Value::Int64(lhs), Value::Int64(rhs)) => lhs < rhs,
            (Value::Float32(lhs), Value::Float32(rhs)) => lhs < rhs,
            (Value::Float64(lhs), Value::Float64(rhs)) => lhs < rhs,
            (Value::String(lhs), Value::String(rhs)) => lhs < rhs,
            (Value::Date(lhs), Value::Date(rhs)) => lhs < rhs,
            (Value::DateTime(lhs), Value::DateTime(rhs)) => lhs < rhs,
            _ => return Err(error_factory::incompatible_types(lhs, rhs)),
        };
        Ok(Value::Bool(result))
    }

    /// Computes `lhs <= rhs`.
    fn compare_lte(&self, lhs: &Value, rhs: &Value) -> Result<Value, InternalExecutorError> {
        let result = match (lhs, rhs) {
            (Value::Int32(lhs), Value::Int32(rhs)) => lhs <= rhs,
            (Value::Int64(lhs), Value::Int64(rhs)) => lhs <= rhs,
            (Value::Float32(lhs), Value::Float32(rhs)) => lhs <= rhs,
            (Value::Float64(lhs), Value::Float64(rhs)) => lhs <= rhs,
            (Value::String(lhs), Value::String(rhs)) => lhs <= rhs,
            (Value::Date(lhs), Value::Date(rhs)) => lhs <= rhs,
            (Value::DateTime(lhs), Value::DateTime(rhs)) => lhs <= rhs,
            _ => return Err(error_factory::incompatible_types(lhs, rhs)),
        };
        Ok(Value::Bool(result))
    }

    /// Computes `!value`.
    fn logical_negate(&self, value: &Value) -> Result<Value, InternalExecutorError> {
        let value = value
            .as_bool()
            .ok_or(error_factory::unexpected_type("bool", value))?;
        Ok(Value::Bool(!value))
    }

    /// Computes `+value`.
    fn identity(&self, value: &Value) -> Result<Value, InternalExecutorError> {
        match value {
            Value::Int32(value) => Ok(Value::Int32(*value)),
            Value::Int64(value) => Ok(Value::Int64(*value)),
            Value::Float32(value) => Ok(Value::Float32(*value)),
            Value::Float64(value) => Ok(Value::Float64(*value)),
            _ => Err(error_factory::unexpected_type("any numeric type", value)),
        }
    }

    /// Computes `-value`.
    fn negate(&self, value: &Value) -> Result<Value, InternalExecutorError> {
        match value {
            Value::Int32(value) => Ok(Value::Int32(-value)),
            Value::Int64(value) => Ok(Value::Int64(-value)),
            Value::Float32(value) => Ok(Value::Float32(-value)),
            Value::Float64(value) => Ok(Value::Float64(-value)),
            _ => Err(error_factory::unexpected_type("any numeric type", value)),
        }
    }

    /// Returns name of the table pointed by `table_id`.
    fn table_name(&self, table_id: ResolvedNodeId) -> Result<&'q str, InternalExecutorError> {
        match self.ast.node(table_id) {
            ResolvedExpression::TableRef(table_ref) => Ok(&table_ref.name),
            other => Err(error_factory::invalid_node_type(format!("{:?}", other))),
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
            Value::Int32(1).into(),
            Value::Int32(25).into(),
            Value::Float64(100.5).into(),
            Value::Bool(true).into(),
            Value::String("Alice".into()).into(),
        ])
    }

    #[test]
    fn test_execute_column_ref() {
        let (tree, expr_id) = parse_expression("age = age");
        let record = create_test_record();

        let executor = ExpressionExecutor::with_single_record(&record, &tree);
        let result = executor.execute_expression(expr_id).unwrap();

        assert_eq!(result.as_ref(), &Value::Bool(true));
    }

    #[test]
    fn test_execute_literal_int() {
        let (tree, expr_id) = parse_expression("42 = 42");

        let executor = ExpressionExecutor::empty(&tree);
        let result = executor.execute_expression(expr_id).unwrap();

        assert_eq!(result.as_ref(), &Value::Bool(true));
    }

    #[test]
    fn test_arithmetic_add() {
        let (tree, expr_id) = parse_expression("5 + 3 = 8");

        let executor = ExpressionExecutor::empty(&tree);
        let result = executor.execute_expression(expr_id).unwrap();

        assert_eq!(result.as_ref(), &Value::Bool(true));
    }

    #[test]
    fn test_comparison_greater_than() {
        let (tree, expr_id) = parse_expression("10 > 5");

        let executor = ExpressionExecutor::empty(&tree);
        let result = executor.execute_expression(expr_id).unwrap();

        assert_eq!(result.as_ref(), &Value::Bool(true));
    }

    #[test]
    fn test_string_equality() {
        let (tree, expr_id) = parse_expression("name = 'Alice'");
        let record = create_test_record();

        let executor = ExpressionExecutor::with_single_record(&record, &tree);
        let result = executor.execute_expression(expr_id).unwrap();

        assert_eq!(result.as_ref(), &Value::Bool(true));
    }

    #[test]
    fn test_complex_expression() {
        let (tree, expr_id) = parse_expression("age > 20 AND active = true");
        let record = create_test_record();

        let executor = ExpressionExecutor::with_single_record(&record, &tree);
        let result = executor.execute_expression(expr_id).unwrap();

        assert_eq!(result.as_ref(), &Value::Bool(true));
    }

    #[test]
    fn test_division_by_zero() {
        let (tree, expr_id) = parse_expression("10 / 0 = 0");

        let executor = ExpressionExecutor::empty(&tree);
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

        let executor = ExpressionExecutor::empty(&tree);
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

        let executor = ExpressionExecutor::with_single_record(&record, &tree);
        let result = executor.execute_expression(expr_id).unwrap();

        assert_eq!(result.as_ref(), &Value::Bool(true));
    }

    #[test]
    fn test_logical_bang() {
        let (tree, expr_id) = parse_expression("!active");
        let record = create_test_record();

        let executor = ExpressionExecutor::with_single_record(&record, &tree);
        let result = executor.execute_expression(expr_id).unwrap();

        assert_eq!(result.as_ref(), &Value::Bool(false));
    }

    #[test]
    fn test_column_with_arithmetic() {
        let (tree, expr_id) = parse_expression("age + 5 = 30");
        let record = create_test_record();

        let executor = ExpressionExecutor::with_single_record(&record, &tree);
        let result = executor.execute_expression(expr_id).unwrap();

        assert_eq!(result.as_ref(), &Value::Bool(true));
    }

    #[test]
    fn test_float_arithmetic() {
        let (tree, expr_id) = parse_expression("score + 0.5 > 100.0");
        let record = create_test_record();

        let executor = ExpressionExecutor::with_single_record(&record, &tree);
        let result = executor.execute_expression(expr_id).unwrap();

        assert_eq!(result.as_ref(), &Value::Bool(true));
    }

    #[test]
    fn test_multiple_columns() {
        let (tree, expr_id) = parse_expression("age > 20 AND score > 90.0");
        let record = create_test_record();

        let executor = ExpressionExecutor::with_single_record(&record, &tree);
        let result = executor.execute_expression(expr_id).unwrap();

        assert_eq!(result.as_ref(), &Value::Bool(true));
    }

    #[test]
    fn test_logical_or_short_circuit() {
        let (tree, expr_id) = parse_expression("active = true OR age < 0");
        let record = create_test_record();

        let executor = ExpressionExecutor::with_single_record(&record, &tree);
        let result = executor.execute_expression(expr_id).unwrap();

        // Should be true due to short-circuit (first condition is true)
        assert_eq!(result.as_ref(), &Value::Bool(true));
    }

    #[test]
    fn test_logical_and_short_circuit() {
        let (tree, expr_id) = parse_expression("!active AND age > 0");
        let record = create_test_record();

        let executor = ExpressionExecutor::with_single_record(&record, &tree);
        let result = executor.execute_expression(expr_id).unwrap();

        // Should be false due to short-circuit (first condition is false)
        assert_eq!(result.as_ref(), &Value::Bool(false));
    }

    #[test]
    fn test_nested_expressions() {
        let (tree, expr_id) = parse_expression("(age + 5) * 2 = 60");
        let record = create_test_record();

        let executor = ExpressionExecutor::with_single_record(&record, &tree);
        let result = executor.execute_expression(expr_id).unwrap();

        assert_eq!(result.as_ref(), &Value::Bool(true));
    }

    #[test]
    fn test_string_concatenation() {
        let (tree, expr_id) = parse_expression("name + ' Smith' = 'Alice Smith'");
        let record = create_test_record();

        let executor = ExpressionExecutor::with_single_record(&record, &tree);
        let result = executor.execute_expression(expr_id).unwrap();

        assert_eq!(result.as_ref(), &Value::Bool(true));
    }
}

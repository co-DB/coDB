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
            _ => return Err(self.invalid_cast(child, &cast.new_ty)),
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
            _ => return Err(self.incompatible_types(lhs, rhs)),
        };
        Ok(field)
    }

    fn arithmetic_sub(&self, lhs: &Field, rhs: &Field) -> Result<Field, InternalExecutorError> {
        let field = match (lhs, rhs) {
            (Field::Int32(lhs), Field::Int32(rhs)) => Field::Int32(lhs - rhs),
            (Field::Int64(lhs), Field::Int64(rhs)) => Field::Int64(lhs - rhs),
            (Field::Float32(lhs), Field::Float32(rhs)) => Field::Float32(lhs - rhs),
            (Field::Float64(lhs), Field::Float64(rhs)) => Field::Float64(lhs - rhs),
            _ => return Err(self.incompatible_types(lhs, rhs)),
        };
        Ok(field)
    }

    fn arithmetic_mul(&self, lhs: &Field, rhs: &Field) -> Result<Field, InternalExecutorError> {
        let field = match (lhs, rhs) {
            (Field::Int32(lhs), Field::Int32(rhs)) => Field::Int32(lhs * rhs),
            (Field::Int64(lhs), Field::Int64(rhs)) => Field::Int64(lhs * rhs),
            (Field::Float32(lhs), Field::Float32(rhs)) => Field::Float32(lhs * rhs),
            (Field::Float64(lhs), Field::Float64(rhs)) => Field::Float64(lhs * rhs),
            _ => return Err(self.incompatible_types(lhs, rhs)),
        };
        Ok(field)
    }

    fn arithmetic_div(&self, lhs: &Field, rhs: &Field) -> Result<Field, InternalExecutorError> {
        let field = match (lhs, rhs) {
            (Field::Int32(lhs), Field::Int32(rhs)) => {
                if *rhs == 0 {
                    return Err(self.div_by_zero());
                }
                Field::Int32(lhs / rhs)
            }
            (Field::Int64(lhs), Field::Int64(rhs)) => {
                if *rhs == 0 {
                    return Err(self.div_by_zero());
                }
                Field::Int64(lhs / rhs)
            }
            (Field::Float32(lhs), Field::Float32(rhs)) => {
                if *rhs == 0.0 {
                    return Err(self.div_by_zero());
                }
                Field::Float32(lhs / rhs)
            }
            (Field::Float64(lhs), Field::Float64(rhs)) => {
                if *rhs == 0.0 {
                    return Err(self.div_by_zero());
                }
                Field::Float64(lhs / rhs)
            }
            _ => return Err(self.incompatible_types(lhs, rhs)),
        };
        Ok(field)
    }

    fn arithmetic_mod(&self, lhs: &Field, rhs: &Field) -> Result<Field, InternalExecutorError> {
        let field = match (lhs, rhs) {
            (Field::Int32(lhs), Field::Int32(rhs)) => {
                if *rhs == 0 {
                    return Err(self.mod_by_zero());
                }
                Field::Int32(lhs % rhs)
            }
            (Field::Int64(lhs), Field::Int64(rhs)) => {
                if *rhs == 0 {
                    return Err(self.mod_by_zero());
                }
                Field::Int64(lhs % rhs)
            }
            _ => return Err(self.incompatible_types(lhs, rhs)),
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
            _ => return Err(self.incompatible_types(lhs, rhs)),
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
            _ => return Err(self.incompatible_types(lhs, rhs)),
        };
        Ok(Field::Bool(result))
    }

    fn logical_bang(&self, value: &Field) -> Result<Field, InternalExecutorError> {
        let value = value.as_bool().ok_or(self.unexpected_type("bool", value))?;
        Ok(Field::Bool(!value))
    }

    fn make_positive(&self, value: &Field) -> Result<Field, InternalExecutorError> {
        match value {
            Field::Int32(value) => Ok(Field::Int32(value.abs())),
            Field::Int64(value) => Ok(Field::Int64(value.abs())),
            Field::Float32(value) => Ok(Field::Float32(value.abs())),
            Field::Float64(value) => Ok(Field::Float64(value.abs())),
            _ => Err(self.unexpected_type("any numeric type", value)),
        }
    }

    fn make_negative(&self, value: &Field) -> Result<Field, InternalExecutorError> {
        match value {
            Field::Int32(value) => Ok(Field::Int32(-value)),
            Field::Int64(value) => Ok(Field::Int64(-value)),
            Field::Float32(value) => Ok(Field::Float32(-value)),
            Field::Float64(value) => Ok(Field::Float64(-value)),
            _ => Err(self.unexpected_type("any numeric type", value)),
        }
    }

    fn unexpected_type(&self, expected: impl Into<String>, got: &Field) -> InternalExecutorError {
        InternalExecutorError::UnexpectedType {
            expected: expected.into(),
            got: format!("{:?}", got),
        }
    }

    fn incompatible_types(&self, lhs: &Field, rhs: &Field) -> InternalExecutorError {
        InternalExecutorError::IncompatibleTypes {
            lhs: format!("{:?}", lhs),
            rhs: format!("{:?}", rhs),
        }
    }

    fn mod_by_zero(&self) -> InternalExecutorError {
        InternalExecutorError::ArithmeticOperationError {
            message: "cannot modulo by 0".into(),
        }
    }

    fn div_by_zero(&self) -> InternalExecutorError {
        InternalExecutorError::ArithmeticOperationError {
            message: "cannot divide by 0".into(),
        }
    }

    fn invalid_cast(&self, child: &Field, new_type: &Type) -> InternalExecutorError {
        InternalExecutorError::InvalidCast {
            from: child.ty().to_string(),
            to: new_type.to_string(),
        }
    }
}

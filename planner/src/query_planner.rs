use crate::{
    query_plan::{QueryPlan, StatementPlan, StatementPlanItem},
    resolved_tree::{
        ResolvedCreateStatement, ResolvedExpression, ResolvedInsertStatement, ResolvedNodeId,
        ResolvedSelectStatement, ResolvedStatement, ResolvedTable, ResolvedTree,
    },
};

/// [`QueryPlanner`] is the last step of query processing. It transforms [`ResolvedTree`] into [`QueryPlan`], that should be returned to the user of this crate.
///
/// This module does not export any error type. The reason for this is that all possible errors where checked for
/// in [`Analyzer`]. At this point we are sure that every statement is correct (the only possibility for problem here is
/// if we programmed something incorrectly, this is why we use panic in a couple of places).
///
/// TODO: currently in the implementation we are using a lot of clones. We should analyze whether we can reduce it (by consuming elements from ResolvedTree, I feel that we only need to preserve nodes and we can just consume statements from tree when we are done with them, but it should be researched more before trying to do it I guess)
pub(crate) struct QueryPlanner {
    tree: ResolvedTree,
}

impl QueryPlanner {
    pub(crate) fn new(tree: ResolvedTree) -> QueryPlanner {
        QueryPlanner { tree }
    }

    /// Creates [`QueryPlan`] based on [Self::tree].
    pub(crate) fn plan_query(self) -> QueryPlan {
        let mut statements_plans = vec![];
        for statement in self.tree.statements() {
            let plan = self.plan_statement(statement);
            statements_plans.push(plan);
        }
        QueryPlan {
            plans: statements_plans,
            tree: self.tree,
        }
    }

    /// Transforms `statement` into [`StatementPlan`].
    fn plan_statement(&self, statement: &ResolvedStatement) -> StatementPlan {
        match statement {
            ResolvedStatement::Select(select) => self.plan_select_statement(select),
            ResolvedStatement::Insert(insert) => self.plan_insert_statement(insert),
            ResolvedStatement::Update(update) => todo!(),
            ResolvedStatement::Delete(delete) => todo!(),
            ResolvedStatement::Create(create) => self.plan_create_statement(create),
            ResolvedStatement::AlterAddColumn(alter_add_column) => todo!(),
            ResolvedStatement::AlterRenameColumn(alter_rename_column) => todo!(),
            ResolvedStatement::AlterRenameTable(alter_rename_table) => todo!(),
            ResolvedStatement::AlterDropColumn(alter_drop_column) => todo!(),
            ResolvedStatement::Truncate(truncate) => todo!(),
            ResolvedStatement::Drop(drop) => todo!(),
        }
    }

    fn plan_select_statement(&self, select: &ResolvedSelectStatement) -> StatementPlan {
        let mut plan = StatementPlan::new();

        let table = self.get_resolved_table(select.table);

        // TODO: if possible we should check if where clause contain only primary_key (index) and
        // in that case use IndexScan with proper range
        let mut root = plan.add_item(StatementPlanItem::table_scan(table.name.clone()));
        if let Some(where_node) = select.where_clause {
            root = plan.add_item(StatementPlanItem::filter(root, where_node));
        }
        plan.add_item(StatementPlanItem::projection(root, select.columns.clone()));

        plan
    }

    fn plan_insert_statement(&self, insert: &ResolvedInsertStatement) -> StatementPlan {
        let mut plan = StatementPlan::new();

        let table = self.get_resolved_table(insert.table);

        plan.add_item(StatementPlanItem::insert(
            table.name.clone(),
            insert.columns.clone(),
            insert.values.clone(),
        ));

        plan
    }

    fn plan_create_statement(&self, create: &ResolvedCreateStatement) -> StatementPlan {
        let mut plan = StatementPlan::new();

        plan.add_item(StatementPlanItem::create_table(
            create.table_name.clone(),
            create.primary_key_column.clone(),
            create.columns.clone(),
        ));

        plan
    }

    /// Helper to transform generic node into [`ResolvedTable`].
    fn get_resolved_table(&self, id: ResolvedNodeId) -> &ResolvedTable {
        let table_expr = self.tree.node(id);
        let table = match table_expr {
            ResolvedExpression::TableRef(t) => t,
            _ => panic!("Expected TableRef in SELECT statement"),
        };
        table
    }
}

#[cfg(test)]
mod tests {
    use metadata::types::Type;

    use crate::{
        query_plan::{CreateTable, Filter, Insert, Projection, StatementPlanItem, TableScan},
        query_planner::QueryPlanner,
        resolved_tree::{
            ResolvedColumn, ResolvedExpression, ResolvedInsertStatement, ResolvedLiteral,
            ResolvedSelectStatement, ResolvedStatement, ResolvedTable, ResolvedTree,
        },
    };

    fn assert_table_scan_item(item: &StatementPlanItem) -> &TableScan {
        match item {
            StatementPlanItem::TableScan(table_scan) => table_scan,
            _ => panic!("expected: table scan, got: {:?}", item),
        }
    }

    fn assert_filter_item(item: &StatementPlanItem) -> &Filter {
        match item {
            StatementPlanItem::Filter(filter) => filter,
            _ => panic!("expected: filter, got: {:?}", item),
        }
    }

    fn assert_projection_item(item: &StatementPlanItem) -> &Projection {
        match item {
            StatementPlanItem::Projection(projection) => projection,
            _ => panic!("expected: projection, got: {:?}", item),
        }
    }

    fn assert_insert_item(item: &StatementPlanItem) -> &Insert {
        match item {
            StatementPlanItem::Insert(insert) => insert,
            _ => panic!("expected: insert, got: {:?}", item),
        }
    }

    fn assert_create_table_item(item: &StatementPlanItem) -> &CreateTable {
        match item {
            StatementPlanItem::CreateTable(create_table) => create_table,
            _ => panic!("expected: create table, got: {:?}", item),
        }
    }

    #[test]
    fn query_planner_select_statement() {
        // Setup tree
        let mut tree = ResolvedTree::default();

        let table = ResolvedTable {
            name: "test".into(),
            primary_key_name: "test_pk".into(),
        };
        let table_id = tree.add_node(ResolvedExpression::TableRef(table));

        let col1 = ResolvedColumn {
            table: table_id,
            name: "col1".into(),
            ty: Type::F32,
            pos: 0,
        };
        let col1 = tree.add_node(ResolvedExpression::ColumnRef(col1));

        let col2 = ResolvedColumn {
            table: table_id,
            name: "col2".into(),
            ty: Type::F64,
            pos: 1,
        };
        let col2 = tree.add_node(ResolvedExpression::ColumnRef(col2));

        let select = ResolvedSelectStatement {
            table: table_id,
            columns: vec![col1, col2],
            where_clause: None,
        };

        tree.add_statement(ResolvedStatement::Select(select));

        // Plan query
        let qp = QueryPlanner::new(tree);
        let mut plan = qp.plan_query();

        // Assert we only got one plan
        assert_eq!(plan.plans.len(), 1);
        let select_plan = plan.plans.pop().unwrap();

        // Assert plan contains two items (projection -> scan table)
        assert_eq!(select_plan.items.len(), 2);

        // Assert projection is correct
        let projection_item = select_plan.root();
        let projection = assert_projection_item(projection_item);
        assert_eq!(projection.columns.len(), 2);
        assert_eq!(projection.columns[0], col1);
        assert_eq!(projection.columns[1], col2);

        // Assert scan table is correct
        let scan_table_item = select_plan.item(projection.data_source);
        let scan_table = assert_table_scan_item(scan_table_item);
        assert_eq!(scan_table.table_name, "test")
    }

    #[test]
    fn query_planner_select_statement_with_where_clause() {
        use crate::{operators::BinaryOperator, resolved_tree::ResolvedBinaryExpression};

        // Setup tree
        let mut tree = ResolvedTree::default();

        let table = ResolvedTable {
            name: "test".into(),
            primary_key_name: "test_pk".into(),
        };
        let table_id = tree.add_node(ResolvedExpression::TableRef(table));

        let col1 = ResolvedColumn {
            table: table_id,
            name: "col1".into(),
            ty: Type::F32,
            pos: 0,
        };
        let col1_id = tree.add_node(ResolvedExpression::ColumnRef(col1));

        let col2 = ResolvedColumn {
            table: table_id,
            name: "col2".into(),
            ty: Type::F64,
            pos: 1,
        };
        let col2_id = tree.add_node(ResolvedExpression::ColumnRef(col2));

        // Create WHERE clause: col1 > 5.0
        let literal = tree.add_node(ResolvedExpression::Literal(ResolvedLiteral::Float32(5.0)));
        let where_expr = tree.add_node(ResolvedExpression::Binary(ResolvedBinaryExpression {
            left: col1_id,
            right: literal,
            op: BinaryOperator::Greater,
            ty: Type::Bool,
        }));

        let select = ResolvedSelectStatement {
            table: table_id,
            columns: vec![col1_id, col2_id],
            where_clause: Some(where_expr),
        };

        tree.add_statement(ResolvedStatement::Select(select));

        // Plan query
        let qp = QueryPlanner::new(tree);
        let mut plan = qp.plan_query();

        // Assert we only got one plan
        assert_eq!(plan.plans.len(), 1);
        let select_plan = plan.plans.pop().unwrap();

        // Assert plan contains three items (projection -> filter -> table scan)
        assert_eq!(select_plan.items.len(), 3);

        // Assert projection is correct
        let projection_item = select_plan.root();
        let projection = assert_projection_item(projection_item);
        assert_eq!(projection.columns.len(), 2);
        assert_eq!(projection.columns[0], col1_id);
        assert_eq!(projection.columns[1], col2_id);

        // Assert filter is correct
        let filter_item = select_plan.item(projection.data_source);
        let filter = assert_filter_item(filter_item);
        assert_eq!(filter.predicate, where_expr);

        // Assert table scan is correct
        let scan_table_item = select_plan.item(filter.data_source);
        let scan_table = assert_table_scan_item(scan_table_item);
        assert_eq!(scan_table.table_name, "test");
    }

    #[test]
    fn query_planner_insert_statement() {
        // Setup tree
        let mut tree = ResolvedTree::default();

        let table = ResolvedTable {
            name: "test".into(),
            primary_key_name: "test_pk".into(),
        };
        let table_id = tree.add_node(ResolvedExpression::TableRef(table));

        let col1 = ResolvedColumn {
            table: table_id,
            name: "col1".into(),
            ty: Type::I32,
            pos: 0,
        };
        let col1_id = tree.add_node(ResolvedExpression::ColumnRef(col1));

        let col2 = ResolvedColumn {
            table: table_id,
            name: "col2".into(),
            ty: Type::String,
            pos: 1,
        };
        let col2_id = tree.add_node(ResolvedExpression::ColumnRef(col2));

        // Create values to insert
        let value1 = tree.add_node(ResolvedExpression::Literal(ResolvedLiteral::Int32(42)));
        let value2 = tree.add_node(ResolvedExpression::Literal(ResolvedLiteral::String(
            "hello".into(),
        )));

        let insert = ResolvedInsertStatement {
            table: table_id,
            columns: vec![col1_id, col2_id],
            values: vec![value1, value2],
        };

        tree.add_statement(ResolvedStatement::Insert(insert));

        // Plan query
        let qp = QueryPlanner::new(tree);
        let mut plan = qp.plan_query();

        // Assert we only got one plan
        assert_eq!(plan.plans.len(), 1);
        let insert_plan = plan.plans.pop().unwrap();

        // Assert plan contains only one item (insert)
        assert_eq!(insert_plan.items.len(), 1);

        // Assert insert is correct
        let insert_item = insert_plan.root();
        let insert = assert_insert_item(insert_item);
        assert_eq!(insert.table_name, "test");
        assert_eq!(insert.columns.len(), 2);
        assert_eq!(insert.columns[0], col1_id);
        assert_eq!(insert.columns[1], col2_id);
        assert_eq!(insert.values.len(), 2);
        assert_eq!(insert.values[0], value1);
        assert_eq!(insert.values[1], value2);
    }

    #[test]
    fn query_planner_create_table_statement() {
        use crate::resolved_tree::{
            ResolvedCreateColumnAddon, ResolvedCreateColumnDescriptor, ResolvedCreateStatement,
        };

        // Setup tree
        let mut tree = ResolvedTree::default();

        let col1 = ResolvedCreateColumnDescriptor {
            name: "id".into(),
            ty: Type::I32,
            addon: ResolvedCreateColumnAddon::PrimaryKey,
        };

        let col2 = ResolvedCreateColumnDescriptor {
            name: "name".into(),
            ty: Type::String,
            addon: ResolvedCreateColumnAddon::None,
        };

        let col3 = ResolvedCreateColumnDescriptor {
            name: "age".into(),
            ty: Type::I32,
            addon: ResolvedCreateColumnAddon::None,
        };

        let create = ResolvedCreateStatement {
            table_name: "users".into(),
            primary_key_column: col1.clone(),
            columns: vec![col2.clone(), col3.clone()],
        };

        tree.add_statement(ResolvedStatement::Create(create));

        // Plan query
        let qp = QueryPlanner::new(tree);
        let mut plan = qp.plan_query();

        // Assert we only got one plan
        assert_eq!(plan.plans.len(), 1);
        let create_plan = plan.plans.pop().unwrap();

        // Assert plan contains only one item (CreateTable)
        assert_eq!(create_plan.items.len(), 1);

        // Assert create table is correct
        let create_item = create_plan.root();
        let create_table = assert_create_table_item(create_item);

        assert_eq!(create_table.name, "users");
        assert_eq!(create_table.columns.len(), 2);

        assert_eq!(create_table.primary_key_column.name, "id");
        assert_eq!(create_table.primary_key_column.ty, Type::I32);
        assert_eq!(
            create_table.primary_key_column.addon,
            ResolvedCreateColumnAddon::PrimaryKey
        );

        assert_eq!(create_table.columns[0].name, "name");
        assert_eq!(create_table.columns[0].ty, Type::String);
        assert_eq!(
            create_table.columns[0].addon,
            ResolvedCreateColumnAddon::None
        );

        assert_eq!(create_table.columns[1].name, "age");
        assert_eq!(create_table.columns[1].ty, Type::I32);
        assert_eq!(
            create_table.columns[1].addon,
            ResolvedCreateColumnAddon::None
        );
    }
}

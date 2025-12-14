use crate::resolved_tree::ResolvedDeleteStatement;
use crate::{
    ast::OrderDirection,
    query_plan::{QueryPlan, SortOrder, StatementPlan, StatementPlanItem},
    resolved_tree::{
        ResolvedAlterAddColumnStatement, ResolvedAlterDropColumnStatement,
        ResolvedAlterRenameColumnStatement, ResolvedAlterRenameTableStatement, ResolvedColumn,
        ResolvedCreateStatement, ResolvedDropStatement, ResolvedExpression,
        ResolvedInsertStatement, ResolvedNodeId, ResolvedSelectStatement, ResolvedStatement,
        ResolvedTable, ResolvedTree, ResolvedTruncateStatement,
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
            ResolvedStatement::Delete(delete) => self.plan_delete_statement(delete),
            ResolvedStatement::Create(create) => self.plan_create_statement(create),
            ResolvedStatement::AlterAddColumn(alter_add_column) => {
                self.plan_alter_add_column_statement(alter_add_column)
            }
            ResolvedStatement::AlterRenameColumn(alter_rename_column) => {
                self.plan_alter_rename_column_statement(alter_rename_column)
            }
            ResolvedStatement::AlterRenameTable(alter_rename_table) => {
                self.plan_alter_rename_table_statement(alter_rename_table)
            }
            ResolvedStatement::AlterDropColumn(alter_drop_column) => {
                self.plan_alter_drop_column_statement(alter_drop_column)
            }
            ResolvedStatement::Truncate(truncate) => self.plan_truncate_table_statement(truncate),
            ResolvedStatement::Drop(drop) => self.plan_drop_table_statement(drop),
        }
    }

    fn plan_select_statement(&self, select: &ResolvedSelectStatement) -> StatementPlan {
        let mut plan = StatementPlan::new();

        let table = self.get_resolved_table(select.table);

        // TODO: Use index bounds from select.index_bounds
        let mut root = plan.add_item(StatementPlanItem::table_scan(table.name.clone()));

        // Handle where
        if let Some(where_node) = select.where_clause {
            root = plan.add_item(StatementPlanItem::filter(root, where_node));
        }

        // Handle order by
        if let Some(order_by) = &select.order_by {
            root = plan.add_item(StatementPlanItem::sort(
                root,
                order_by.column,
                self.map_order(&order_by.direction),
            ));
        }

        // Handle offset
        if let Some(offset) = select.offset {
            root = plan.add_item(StatementPlanItem::skip(root, offset));
        }

        // Handle limit
        if let Some(limit) = select.limit {
            root = plan.add_item(StatementPlanItem::limit(root, limit));
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

    fn plan_delete_statement(&self, delete: &ResolvedDeleteStatement) -> StatementPlan {
        let mut plan = StatementPlan::new();

        let table = self.get_resolved_table(delete.table);

        let mut root = plan.add_item(StatementPlanItem::table_scan(table.name.clone()));

        if let Some(where_node) = delete.where_clause {
            root = plan.add_item(StatementPlanItem::filter(root, where_node));
        }

        plan.add_item(StatementPlanItem::delete(root, table.name.clone()));

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

    fn plan_alter_add_column_statement(
        &self,
        alter_add_column: &ResolvedAlterAddColumnStatement,
    ) -> StatementPlan {
        let mut plan = StatementPlan::new();

        let table = self.get_resolved_table(alter_add_column.table);
        let (name, ty) = (&alter_add_column.column_name, alter_add_column.column_type);

        plan.add_item(StatementPlanItem::add_column(
            table.name.clone(),
            name.clone(),
            ty,
        ));

        plan
    }

    fn plan_alter_rename_column_statement(
        &self,
        alter_rename_column: &ResolvedAlterRenameColumnStatement,
    ) -> StatementPlan {
        let mut plan = StatementPlan::new();

        let table = self.get_resolved_table(alter_rename_column.table);
        let column = self.get_resolved_column(alter_rename_column.column);

        plan.add_item(StatementPlanItem::rename_column(
            table.name.clone(),
            column.name.clone(),
            alter_rename_column.new_name.clone(),
        ));

        plan
    }

    fn plan_alter_rename_table_statement(
        &self,
        alter_rename_table: &ResolvedAlterRenameTableStatement,
    ) -> StatementPlan {
        let mut plan = StatementPlan::new();

        let table = self.get_resolved_table(alter_rename_table.table);

        plan.add_item(StatementPlanItem::rename_table(
            table.name.clone(),
            alter_rename_table.new_name.clone(),
        ));

        plan
    }

    fn plan_alter_drop_column_statement(
        &self,
        alter_drop_column: &ResolvedAlterDropColumnStatement,
    ) -> StatementPlan {
        let mut plan = StatementPlan::new();

        let table = self.get_resolved_table(alter_drop_column.table);
        let column = self.get_resolved_column(alter_drop_column.column);

        plan.add_item(StatementPlanItem::remove_column(
            table.name.clone(),
            column.name.clone(),
        ));

        plan
    }

    fn plan_truncate_table_statement(
        &self,
        truncate_table: &ResolvedTruncateStatement,
    ) -> StatementPlan {
        let mut plan = StatementPlan::new();

        let table = self.get_resolved_table(truncate_table.table);

        plan.add_item(StatementPlanItem::clear_table(table.name.clone()));

        plan
    }

    fn plan_drop_table_statement(&self, drop_table: &ResolvedDropStatement) -> StatementPlan {
        let mut plan = StatementPlan::new();

        let table = self.get_resolved_table(drop_table.table);

        plan.add_item(StatementPlanItem::remove_table(table.name.clone()));

        plan
    }

    /// Helper to transform generic node into [`ResolvedTable`].
    fn get_resolved_table(&self, id: ResolvedNodeId) -> &ResolvedTable {
        let table_expr = self.tree.node(id);

        (match table_expr {
            ResolvedExpression::TableRef(t) => t,
            _ => panic!("Expected TableRef"),
        }) as _
    }

    /// Helper to transform generic node into [`ResolvedColumn`].
    fn get_resolved_column(&self, id: ResolvedNodeId) -> &ResolvedColumn {
        let column_expr = self.tree.node(id);

        (match column_expr {
            ResolvedExpression::ColumnRef(c) => c,
            _ => panic!("Expected ColumnRef"),
        }) as _
    }

    fn map_order(&self, order: &OrderDirection) -> SortOrder {
        match order {
            OrderDirection::Ascending => SortOrder::Ascending,
            OrderDirection::Descending => SortOrder::Descending,
        }
    }
}

#[cfg(test)]
mod tests {
    use types::schema::Type;

    use crate::resolved_tree::{
        IndexBounds, ResolvedAlterRenameColumnStatement, ResolvedDropStatement,
        ResolvedTruncateStatement,
    };
    use crate::{
        ast::OrderDirection,
        operators::BinaryOperator,
        query_plan::{
            AddColumn, CreateTable, Filter, Insert, Limit, Projection, QueryPlan, RemoveColumn,
            Skip, Sort, SortOrder, StatementPlanItem, TableScan,
        },
        query_planner::QueryPlanner,
        resolved_tree::{
            ResolvedAlterAddColumnStatement, ResolvedAlterDropColumnStatement,
            ResolvedBinaryExpression, ResolvedColumn, ResolvedExpression, ResolvedInsertStatement,
            ResolvedLiteral, ResolvedNodeId, ResolvedOrderByDetails, ResolvedSelectStatement,
            ResolvedStatement, ResolvedTable, ResolvedTree,
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

    fn assert_sort_item(item: &StatementPlanItem) -> &Sort {
        match item {
            StatementPlanItem::Sort(sort) => sort,
            _ => panic!("expected: sort, got: {:?}", item),
        }
    }

    fn assert_limit_item(item: &StatementPlanItem) -> &Limit {
        match item {
            StatementPlanItem::Limit(limit) => limit,
            _ => panic!("expected: limit, got: {:?}", item),
        }
    }

    fn assert_skip_item(item: &StatementPlanItem) -> &Skip {
        match item {
            StatementPlanItem::Skip(skip) => skip,
            _ => panic!("expected: skip, got: {:?}", item),
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

    fn assert_add_column_item(item: &StatementPlanItem) -> &AddColumn {
        match item {
            StatementPlanItem::AddColumn(add_column) => add_column,
            _ => panic!("expected: add column, got: {:?}", item),
        }
    }

    fn assert_remove_column_item(item: &StatementPlanItem) -> &RemoveColumn {
        match item {
            StatementPlanItem::RemoveColumn(remove_column) => remove_column,
            _ => panic!("expected: remove column, got: {:?}", item),
        }
    }

    struct SelectStatementBuilder {
        tree: ResolvedTree,
        table_id: ResolvedNodeId,
        columns: Vec<ResolvedNodeId>,
        where_clause: Option<ResolvedNodeId>,
        order_by: Option<ResolvedOrderByDetails>,
        limit: Option<u32>,
        offset: Option<u32>,
        index_bounds: Option<IndexBounds>,
    }

    impl SelectStatementBuilder {
        fn new() -> Self {
            let mut tree = ResolvedTree::default();

            let table = ResolvedTable {
                name: "test".into(),
                primary_key_name: "test_pk".into(),
            };
            let table_id = tree.add_node(ResolvedExpression::TableRef(table));

            SelectStatementBuilder {
                tree,
                table_id,
                columns: vec![],
                where_clause: None,
                order_by: None,
                limit: None,
                offset: None,
                index_bounds: None,
            }
        }

        fn add_column(&mut self, name: &str, ty: Type) -> ResolvedNodeId {
            let col = ResolvedColumn {
                table: self.table_id,
                name: name.into(),
                ty,
                pos: self.columns.len() as _,
            };
            let col_id = self.tree.add_node(ResolvedExpression::ColumnRef(col));
            self.columns.push(col_id);
            col_id
        }

        fn add_where_greater_than(&mut self, col_id: ResolvedNodeId, value: i32) -> ResolvedNodeId {
            let literal = self
                .tree
                .add_node(ResolvedExpression::Literal(ResolvedLiteral::Int32(value)));
            let where_expr =
                self.tree
                    .add_node(ResolvedExpression::Binary(ResolvedBinaryExpression {
                        left: col_id,
                        right: literal,
                        op: BinaryOperator::Greater,
                        ty: Type::Bool,
                    }));
            self.where_clause = Some(where_expr);
            where_expr
        }

        fn with_order_by(
            &mut self,
            col_id: ResolvedNodeId,
            direction: OrderDirection,
        ) -> &mut Self {
            self.order_by = Some(ResolvedOrderByDetails {
                column: col_id,
                direction,
            });
            self
        }

        fn with_limit(&mut self, count: u32) -> &mut Self {
            self.limit = Some(count);
            self
        }

        fn with_offset(&mut self, count: u32) -> &mut Self {
            self.offset = Some(count);
            self
        }

        fn build(mut self) -> ResolvedTree {
            let select = ResolvedSelectStatement {
                table: self.table_id,
                columns: self.columns,
                where_clause: self.where_clause,
                order_by: self.order_by,
                limit: self.limit,
                offset: self.offset,
                index_bounds: self.index_bounds,
            };

            self.tree.add_statement(ResolvedStatement::Select(select));
            self.tree
        }
    }

    // Helper to plan and extract the select statement plan
    fn plan_select(tree: ResolvedTree) -> (QueryPlan, usize) {
        let qp = QueryPlanner::new(tree);
        let plan = qp.plan_query();
        assert_eq!(plan.plans.len(), 1);
        let num_items = plan.plans[0].items.len();
        (plan, num_items)
    }

    #[test]
    fn query_planner_select_statement() {
        let mut builder = SelectStatementBuilder::new();
        let col1 = builder.add_column("col1", Type::F32);
        let col2 = builder.add_column("col2", Type::F64);
        let tree = builder.build();

        let (mut plan, num_items) = plan_select(tree);
        // Assert plan contains two items (projection -> scan table)
        assert_eq!(num_items, 2);
        let select_plan = plan.plans.pop().unwrap();

        // Assert projection is correct
        let projection_item = select_plan.root();
        let projection = assert_projection_item(projection_item);
        assert_eq!(projection.columns, vec![col1, col2]);

        // Assert scan table is correct
        let scan_table_item = select_plan.item(projection.data_source);
        let scan_table = assert_table_scan_item(scan_table_item);
        assert_eq!(scan_table.table_name, "test")
    }

    #[test]
    fn query_planner_select_statement_with_where_clause() {
        let mut builder = SelectStatementBuilder::new();
        let col1 = builder.add_column("col1", Type::F32);
        let col2 = builder.add_column("col2", Type::F64);
        let where_expr = builder.add_where_greater_than(col1, 5);
        let tree = builder.build();

        let (mut plan, num_items) = plan_select(tree);
        let select_plan = plan.plans.pop().unwrap();
        // Assert plan contains three items (projection -> filter -> table scan)
        assert_eq!(num_items, 3);

        // Assert projection is correct
        let projection_item = select_plan.root();
        let projection = assert_projection_item(projection_item);
        assert_eq!(projection.columns, vec![col1, col2]);

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
    fn query_planner_select_with_order_by_desc() {
        let mut builder = SelectStatementBuilder::new();
        let col1 = builder.add_column("col1", Type::String);
        builder.with_order_by(col1, OrderDirection::Descending);
        let tree = builder.build();

        let (mut plan, num_items) = plan_select(tree);
        let select_plan = plan.plans.pop().unwrap();
        // Assert plan contains three items (projection -> sort -> table scan)
        assert_eq!(num_items, 3);

        let projection_item = select_plan.root();
        let projection = assert_projection_item(projection_item);
        assert_eq!(projection.columns, vec![col1]);

        let sort_item = select_plan.item(projection.data_source);
        let sort = assert_sort_item(sort_item);
        assert_eq!(sort.column, col1);
        assert!(matches!(sort.order, SortOrder::Descending));
    }

    #[test]
    fn query_planner_select_with_offset_and_limit() {
        let mut builder = SelectStatementBuilder::new();
        let col1 = builder.add_column("col1", Type::I32);
        builder.with_limit(25).with_offset(10);
        let tree = builder.build();

        let (mut plan, num_items) = plan_select(tree);
        let select_plan = plan.plans.pop().unwrap();
        // Assert plan contains four items (projection -> limit -> offset -> table scan)
        assert_eq!(num_items, 4);

        let projection_item = select_plan.root();
        let projection = assert_projection_item(projection_item);
        assert_eq!(projection.columns, vec![col1]);

        let limit_item = select_plan.item(projection.data_source);
        let limit = assert_limit_item(limit_item);
        assert_eq!(limit.count, 25);

        let skip_item = select_plan.item(limit.data_source);
        let skip = assert_skip_item(skip_item);
        assert_eq!(skip.count, 10);

        let scan_table_item = select_plan.item(skip.data_source);
        let scan_table = assert_table_scan_item(scan_table_item);
        assert_eq!(scan_table.table_name, "test");
    }

    #[test]
    fn query_planner_select_with_all_clauses() {
        let mut builder = SelectStatementBuilder::new();
        let col1 = builder.add_column("col1", Type::I32);
        let col2 = builder.add_column("col2", Type::String);
        let where_expr = builder.add_where_greater_than(col1, 10);

        builder
            .with_order_by(col2, OrderDirection::Descending)
            .with_limit(50)
            .with_offset(5);

        let tree = builder.build();

        let (mut plan, num_items) = plan_select(tree);
        let select_plan = plan.plans.pop().unwrap();
        // Assert plan contains six items (projection -> limit -> offset -> sort -> filter -> table scan)
        assert_eq!(num_items, 6);

        let projection_item = select_plan.root();
        let projection = assert_projection_item(projection_item);
        assert_eq!(projection.columns, vec![col1, col2]);

        let limit_item = select_plan.item(projection.data_source);
        let limit = assert_limit_item(limit_item);
        assert_eq!(limit.count, 50);

        let skip_item = select_plan.item(limit.data_source);
        let skip = assert_skip_item(skip_item);
        assert_eq!(skip.count, 5);

        let sort_item = select_plan.item(skip.data_source);
        let sort = assert_sort_item(sort_item);
        assert_eq!(sort.column, col2);
        assert!(matches!(sort.order, SortOrder::Descending));

        let filter_item = select_plan.item(sort.data_source);
        let filter = assert_filter_item(filter_item);
        assert_eq!(filter.predicate, where_expr);

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

    #[test]
    fn query_planner_alter_add_column_statement() {
        // Setup tree
        let mut tree = ResolvedTree::default();

        let table = ResolvedTable {
            name: "users".into(),
            primary_key_name: "id".into(),
        };
        let table_id = tree.add_node(ResolvedExpression::TableRef(table));

        let alter_add_column = ResolvedAlterAddColumnStatement {
            table: table_id,
            column_name: "email".into(),
            column_type: Type::String,
        };

        tree.add_statement(ResolvedStatement::AlterAddColumn(alter_add_column));

        // Plan query
        let qp = QueryPlanner::new(tree);
        let mut plan = qp.plan_query();

        // Assert we only got one plan
        assert_eq!(plan.plans.len(), 1);
        let alter_plan = plan.plans.pop().unwrap();

        // Assert plan contains only one item (AddColumn)
        assert_eq!(alter_plan.items.len(), 1);

        // Assert add column is correct
        let add_column_item = alter_plan.root();
        let add_column = assert_add_column_item(add_column_item);

        assert_eq!(add_column.table_name, "users");
        assert_eq!(add_column.column_name, "email");
        assert_eq!(add_column.column_ty, Type::String);
    }

    #[test]
    fn query_planner_alter_rename_column_statement() {
        // Setup tree
        let mut tree = ResolvedTree::default();

        let table = ResolvedTable {
            name: "users".into(),
            primary_key_name: "id".into(),
        };
        let table_id = tree.add_node(ResolvedExpression::TableRef(table));

        let column = ResolvedColumn {
            table: table_id,
            name: "email".into(),
            ty: Type::String,
            pos: 2,
        };
        let column_id = tree.add_node(ResolvedExpression::ColumnRef(column));

        let alter_rename_column = ResolvedAlterRenameColumnStatement {
            table: table_id,
            column: column_id,
            new_name: "email_address".into(),
        };

        tree.add_statement(ResolvedStatement::AlterRenameColumn(alter_rename_column));

        // Plan query
        let qp = QueryPlanner::new(tree);
        let mut plan = qp.plan_query();

        // Assert we only got one plan
        assert_eq!(plan.plans.len(), 1);
        let alter_plan = plan.plans.pop().unwrap();

        // Assert plan contains only one item (RenameColumn)
        assert_eq!(alter_plan.items.len(), 1);

        // Assert rename column is correct
        let rename_column_item = alter_plan.root();
        let rename_column = match rename_column_item {
            StatementPlanItem::RenameColumn(rename_column) => rename_column,
            _ => panic!("expected: rename column, got: {:?}", rename_column_item),
        };

        assert_eq!(rename_column.table_name, "users");
        assert_eq!(rename_column.prev_column_name, "email");
        assert_eq!(rename_column.new_column_name, "email_address");
    }

    #[test]
    fn query_planner_alter_rename_table_statement() {
        use crate::resolved_tree::ResolvedAlterRenameTableStatement;

        // Setup tree
        let mut tree = ResolvedTree::default();

        let table = ResolvedTable {
            name: "users".into(),
            primary_key_name: "id".into(),
        };
        let table_id = tree.add_node(ResolvedExpression::TableRef(table));

        let alter_rename_table = ResolvedAlterRenameTableStatement {
            table: table_id,
            new_name: "customers".into(),
        };

        tree.add_statement(ResolvedStatement::AlterRenameTable(alter_rename_table));

        // Plan query
        let qp = QueryPlanner::new(tree);
        let mut plan = qp.plan_query();

        // Assert we only got one plan
        assert_eq!(plan.plans.len(), 1);
        let alter_plan = plan.plans.pop().unwrap();

        // Assert plan contains only one item (RenameTable)
        assert_eq!(alter_plan.items.len(), 1);

        // Assert rename table is correct
        let rename_table_item = alter_plan.root();
        let rename_table = match rename_table_item {
            StatementPlanItem::RenameTable(rename_table) => rename_table,
            _ => panic!("expected: rename table, got: {:?}", rename_table_item),
        };

        assert_eq!(rename_table.prev_name, "users");
        assert_eq!(rename_table.new_name, "customers");
    }

    #[test]
    fn query_planner_alter_drop_column_statement() {
        // Setup tree
        let mut tree = ResolvedTree::default();

        let table = ResolvedTable {
            name: "users".into(),
            primary_key_name: "id".into(),
        };
        let table_id = tree.add_node(ResolvedExpression::TableRef(table));

        let column = ResolvedColumn {
            table: table_id,
            name: "email".into(),
            ty: Type::String,
            pos: 2,
        };
        let column_id = tree.add_node(ResolvedExpression::ColumnRef(column));

        let alter_drop_column = ResolvedAlterDropColumnStatement {
            table: table_id,
            column: column_id,
        };

        tree.add_statement(ResolvedStatement::AlterDropColumn(alter_drop_column));

        // Plan query
        let qp = QueryPlanner::new(tree);
        let mut plan = qp.plan_query();

        // Assert we only got one plan
        assert_eq!(plan.plans.len(), 1);
        let alter_plan = plan.plans.pop().unwrap();

        // Assert plan contains only one item (RemoveColumn)
        assert_eq!(alter_plan.items.len(), 1);

        // Assert remove column is correct
        let remove_column_item = alter_plan.root();
        let remove_column = assert_remove_column_item(remove_column_item);

        assert_eq!(remove_column.table_name, "users");
        assert_eq!(remove_column.column_name, "email");
    }

    #[test]
    fn query_planner_truncate_table_statement() {
        // Setup tree
        let mut tree = ResolvedTree::default();

        let table = ResolvedTable {
            name: "users".into(),
            primary_key_name: "id".into(),
        };
        let table_id = tree.add_node(ResolvedExpression::TableRef(table));

        let truncate = ResolvedTruncateStatement { table: table_id };

        tree.add_statement(ResolvedStatement::Truncate(truncate));

        // Plan query
        let qp = QueryPlanner::new(tree);
        let mut plan = qp.plan_query();

        // Assert we only got one plan
        assert_eq!(plan.plans.len(), 1);
        let truncate_plan = plan.plans.pop().unwrap();

        // Assert plan contains only one item (ClearTable)
        assert_eq!(truncate_plan.items.len(), 1);

        // Assert clear table is correct
        let clear_table_item = truncate_plan.root();
        let clear_table = match clear_table_item {
            StatementPlanItem::ClearTable(clear_table) => clear_table,
            _ => panic!("expected: clear table, got: {:?}", clear_table_item),
        };

        assert_eq!(clear_table.name, "users");
    }

    #[test]
    fn query_planner_drop_table_statement() {
        // Setup tree
        let mut tree = ResolvedTree::default();

        let table = ResolvedTable {
            name: "users".into(),
            primary_key_name: "id".into(),
        };
        let table_id = tree.add_node(ResolvedExpression::TableRef(table));

        let drop = ResolvedDropStatement { table: table_id };

        tree.add_statement(ResolvedStatement::Drop(drop));

        // Plan query
        let qp = QueryPlanner::new(tree);
        let mut plan = qp.plan_query();

        // Assert we only got one plan
        assert_eq!(plan.plans.len(), 1);
        let drop_plan = plan.plans.pop().unwrap();

        // Assert plan contains only one item (RemoveTable)
        assert_eq!(drop_plan.items.len(), 1);

        // Assert remove table is correct
        let remove_table_item = drop_plan.root();
        let remove_table = match remove_table_item {
            StatementPlanItem::RemoveTable(remove_table) => remove_table,
            _ => panic!("expected: remove table, got: {:?}", remove_table_item),
        };

        assert_eq!(remove_table.name, "users");
    }
}

use crate::{
    query_plan::{QueryPlan, StatementPlan, StatementPlanItem},
    resolved_tree::{
        ResolvedExpression, ResolvedNodeId, ResolvedSelectStatement, ResolvedStatement,
        ResolvedTable, ResolvedTree,
    },
};

pub(crate) struct QueryPlanner {
    tree: ResolvedTree,
}

impl QueryPlanner {
    pub(crate) fn new(tree: ResolvedTree) -> QueryPlanner {
        QueryPlanner { tree }
    }

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

    fn plan_statement(&self, statement: &ResolvedStatement) -> StatementPlan {
        match statement {
            ResolvedStatement::Select(select) => self.plan_select_statement(select),
            ResolvedStatement::Insert(insert) => todo!(),
            ResolvedStatement::Update(update) => todo!(),
            ResolvedStatement::Delete(delete) => todo!(),
            ResolvedStatement::Create(create) => todo!(),
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

        let mut root = plan.add_item(StatementPlanItem::table_scan(table.name.clone()));
        if let Some(where_node) = select.where_clause {
            root = plan.add_item(StatementPlanItem::filter(root, where_node));
        }
        plan.add_item(StatementPlanItem::projection(root, select.columns.clone()));

        plan
    }

    fn get_resolved_table(&self, id: ResolvedNodeId) -> &ResolvedTable {
        let table_expr = self.tree.node(id);
        let table = match table_expr {
            ResolvedExpression::TableRef(t) => t,
            _ => panic!("Expected TableRef in SELECT statement"),
        };
        table
    }
}

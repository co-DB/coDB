use crate::resolved_tree::{ResolvedNodeId, ResolvedTree};

pub struct QueryPlan {
    pub plans: Vec<StatementPlan>,
    pub tree: ResolvedTree,
}

pub struct StatementPlan {
    pub items: Vec<StatementPlanItem>,
}

impl StatementPlan {
    pub(crate) fn new() -> Self {
        StatementPlan { items: vec![] }
    }

    pub(crate) fn add_item(&mut self, item: StatementPlanItem) -> StatementPlanItemId {
        self.items.push(item);
        StatementPlanItemId::new(self.items.len() - 1)
    }

    pub fn root(&self) -> StatementPlanItemId {
        StatementPlanItemId::new(self.items.len() - 1)
    }
}

/// [`StatementPlanItemId`] is used for indexing items inside [`StatementPlan`].
///
/// Works similarly to [`NodeId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatementPlanItemId(usize);

impl StatementPlanItemId {
    fn new(id: usize) -> Self {
        StatementPlanItemId(id)
    }
}

pub enum StatementPlanItem {
    TableScan(TableScan),
    IndexScan(IndexScan),
    Filter(Filter),
    Projection(Projection),
}

impl StatementPlanItem {
    pub(crate) fn table_scan(table_name: String) -> Self {
        StatementPlanItem::TableScan(TableScan { table_name })
    }

    pub(crate) fn filter(data: StatementPlanItemId, predicate: ResolvedNodeId) -> Self {
        StatementPlanItem::Filter(Filter { data, predicate })
    }

    pub(crate) fn projection(data: StatementPlanItemId, columns: Vec<ResolvedNodeId>) -> Self {
        StatementPlanItem::Projection(Projection { data, columns })
    }
}

/// Sequential scan of all table (loads every record).
pub struct TableScan {
    pub table_name: String,
}

/// Uses index (currently index = primary key) to load only relevant records from the table.
pub struct IndexScan {
    pub table_name: String,
    pub start: Option<ResolvedNodeId>,
    pub end: Option<ResolvedNodeId>,
}

/// Applies filter defined in `predicate` to `data`.
pub struct Filter {
    pub data: StatementPlanItemId,
    pub predicate: ResolvedNodeId,
}

/// Returns only `columns` from `data`.
pub struct Projection {
    pub data: StatementPlanItemId,
    pub columns: Vec<ResolvedNodeId>,
}

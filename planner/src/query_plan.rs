use crate::resolved_tree::{ResolvedCreateColumnDescriptor, ResolvedNodeId, ResolvedTree};

/// [`QueryPlan`] represents the logical steps that should be executed to perform the query.
///
/// Each statement in query is transformed into [`StatementPlan`] and stored inside [`QueryPlan::plans`].
/// [`QueryPlan::plans`] should be executed in order they are stored.
///
/// Each plan can reference nodes from [`StatementPlan::tree`], which is [`ResolvedTree`]
/// that represents the same query.
pub struct QueryPlan {
    pub plans: Vec<StatementPlan>,
    pub tree: ResolvedTree,
}

/// [`StatementPlan`] represents the logical steps that should be executed to perform the single statement.
/// In the simplest case, when query contains only one statement, [`QueryPlan`] can be reduced to [`StatementPlan`].
pub struct StatementPlan {
    pub(crate) items: Vec<StatementPlanItem>,
}

impl StatementPlan {
    pub(crate) fn new() -> Self {
        StatementPlan { items: vec![] }
    }

    pub(crate) fn add_item(&mut self, item: StatementPlanItem) -> StatementPlanItemId {
        self.items.push(item);
        StatementPlanItemId::new(self.items.len() - 1)
    }

    /// Returns the [`StatementPlanItemId`] of item that should be executed first.
    pub fn root(&self) -> StatementPlanItemId {
        StatementPlanItemId::new(self.items.len() - 1)
    }

    pub fn item(&self, id: StatementPlanItemId) -> &StatementPlanItem {
        &self.items[id.0]
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

/// The building block used to describe [`StatementPlan`].
#[derive(Debug)]
pub enum StatementPlanItem {
    TableScan(TableScan),
    IndexScan(IndexScan),
    Filter(Filter),
    Projection(Projection),
    Insert(Insert),
    CreateTable(CreateTable),
}

/// Functions for constructing each [`StatementPlanItem`].
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

    pub(crate) fn insert(
        table_name: String,
        columns: Vec<ResolvedNodeId>,
        values: Vec<ResolvedNodeId>,
    ) -> Self {
        StatementPlanItem::Insert(Insert {
            table_name,
            columns,
            values,
        })
    }

    pub(crate) fn create_table(name: String, columns: Vec<ResolvedCreateColumnDescriptor>) -> Self {
        StatementPlanItem::CreateTable(CreateTable { name, columns })
    }
}

/// Sequential scan of all table (loads every record).
#[derive(Debug)]
pub struct TableScan {
    pub table_name: String,
}

/// Uses index (currently index = primary key) to load only relevant records from the table.
#[derive(Debug)]
pub struct IndexScan {
    pub table_name: String,
    pub start: Option<ResolvedNodeId>,
    pub end: Option<ResolvedNodeId>,
}

/// Applies filter defined in `predicate` to `data`.
#[derive(Debug)]
pub struct Filter {
    pub data: StatementPlanItemId,
    pub predicate: ResolvedNodeId,
}

/// Returns only `columns` from `data`.
#[derive(Debug)]
pub struct Projection {
    pub data: StatementPlanItemId,
    pub columns: Vec<ResolvedNodeId>,
}

/// Inserts single element into table.
#[derive(Debug)]
pub struct Insert {
    pub table_name: String,
    pub columns: Vec<ResolvedNodeId>,
    pub values: Vec<ResolvedNodeId>,
}

/// Creates a new table
#[derive(Debug)]
pub struct CreateTable {
    pub name: String,
    pub columns: Vec<ResolvedCreateColumnDescriptor>,
}

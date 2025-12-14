use types::schema::Type;

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

    /// Returns the [`StatementPlanItem`] that should be executed first.
    pub fn root(&self) -> &StatementPlanItem {
        &self.items[self.items.len() - 1]
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
    // Data sources
    TableScan(TableScan),
    IndexScan(IndexScan),

    // Operators (can be chained)
    Filter(Filter),
    Sort(Sort),
    Limit(Limit),
    Skip(Skip),

    // Terminal operators (must always be root)
    Projection(Projection),
    Insert(Insert),
    Delete(Delete),
    Update(Update),
    CreateTable(CreateTable),
    RemoveTable(RemoveTable),
    ClearTable(ClearTable),
    RenameTable(RenameTable),
    AddColumn(AddColumn),
    RemoveColumn(RemoveColumn),
    RenameColumn(RenameColumn),
}

impl StatementPlanItem {
    /// Returns `true` if [`StatementPlanItem`] can be root and returns result set.
    pub fn produces_result_set(&self) -> bool {
        matches!(self, StatementPlanItem::Projection(_))
    }
}

/// Functions for constructing each [`StatementPlanItem`].
impl StatementPlanItem {
    pub(crate) fn table_scan(table_name: String) -> Self {
        StatementPlanItem::TableScan(TableScan { table_name })
    }

    pub(crate) fn filter(data_source: StatementPlanItemId, predicate: ResolvedNodeId) -> Self {
        StatementPlanItem::Filter(Filter {
            data_source,
            predicate,
        })
    }

    pub(crate) fn sort(
        data_source: StatementPlanItemId,
        column: ResolvedNodeId,
        order: SortOrder,
    ) -> Self {
        StatementPlanItem::Sort(Sort {
            data_source,
            column,
            order,
        })
    }

    pub(crate) fn limit(data_source: StatementPlanItemId, count: u32) -> Self {
        StatementPlanItem::Limit(Limit { data_source, count })
    }

    pub(crate) fn skip(data_source: StatementPlanItemId, count: u32) -> Self {
        StatementPlanItem::Skip(Skip { data_source, count })
    }

    pub(crate) fn projection(
        data_source: StatementPlanItemId,
        columns: Vec<ResolvedNodeId>,
    ) -> Self {
        StatementPlanItem::Projection(Projection {
            data_source,
            columns,
        })
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

    pub(crate) fn delete(data_source: StatementPlanItemId, table_name: String) -> Self {
        StatementPlanItem::Delete(Delete {
            data_source,
            table_name,
        })
    }

    pub(crate) fn update(
        data_source: StatementPlanItemId,
        table_name: String,
        columns: Vec<ResolvedNodeId>,
        values: Vec<ResolvedNodeId>,
    ) -> Self {
        StatementPlanItem::Update(Update {
            data_source,
            table_name,
            columns,
            values,
        })
    }

    pub(crate) fn create_table(
        name: String,
        primary_key_column: ResolvedCreateColumnDescriptor,
        columns: Vec<ResolvedCreateColumnDescriptor>,
    ) -> Self {
        StatementPlanItem::CreateTable(CreateTable {
            name,
            primary_key_column,
            columns,
        })
    }

    pub(crate) fn remove_table(name: String) -> Self {
        StatementPlanItem::RemoveTable(RemoveTable { name })
    }

    pub(crate) fn clear_table(name: String) -> Self {
        StatementPlanItem::ClearTable(ClearTable { name })
    }

    pub(crate) fn rename_table(prev_name: String, new_name: String) -> Self {
        StatementPlanItem::RenameTable(RenameTable {
            prev_name,
            new_name,
        })
    }

    pub(crate) fn add_column(table_name: String, column_name: String, column_ty: Type) -> Self {
        StatementPlanItem::AddColumn(AddColumn {
            table_name,
            column_name,
            column_ty,
        })
    }

    pub(crate) fn remove_column(table_name: String, column_name: String) -> Self {
        StatementPlanItem::RemoveColumn(RemoveColumn {
            table_name,
            column_name,
        })
    }

    pub(crate) fn rename_column(
        table_name: String,
        prev_column_name: String,
        new_column_name: String,
    ) -> Self {
        StatementPlanItem::RenameColumn(RenameColumn {
            table_name,
            prev_column_name,
            new_column_name,
        })
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

/// Applies filter defined in `predicate` to `data_source`.
#[derive(Debug)]
pub struct Filter {
    pub data_source: StatementPlanItemId,
    pub predicate: ResolvedNodeId,
}

/// Sorts the `data_source` by `column` in specified `order`.
#[derive(Debug)]
pub struct Sort {
    pub data_source: StatementPlanItemId,
    pub column: ResolvedNodeId,
    pub order: SortOrder,
}

#[derive(Debug, Clone, Copy)]
pub enum SortOrder {
    Ascending,
    Descending,
}

/// Limits the number of rows from `data_source` to `count`.
#[derive(Debug)]
pub struct Limit {
    pub data_source: StatementPlanItemId,
    pub count: u32,
}

/// Skips first `count` rows from `data_source`.
#[derive(Debug)]
pub struct Skip {
    pub data_source: StatementPlanItemId,
    pub count: u32,
}

/// Returns only `columns` from `data_source`.
#[derive(Debug)]
pub struct Projection {
    pub data_source: StatementPlanItemId,
    pub columns: Vec<ResolvedNodeId>,
}

/// Inserts single element into table.
#[derive(Debug)]
pub struct Insert {
    pub table_name: String,
    pub columns: Vec<ResolvedNodeId>,
    pub values: Vec<ResolvedNodeId>,
}

/// Deletes elements from table.
#[derive(Debug)]
pub struct Delete {
    pub data_source: StatementPlanItemId,
    pub table_name: String,
}

/// Updates elements in table.
#[derive(Debug)]
pub struct Update {
    pub data_source: StatementPlanItemId,
    pub table_name: String,
    pub columns: Vec<ResolvedNodeId>,
    pub values: Vec<ResolvedNodeId>,
}

/// Creates a new table.
#[derive(Debug)]
pub struct CreateTable {
    pub name: String,
    pub primary_key_column: ResolvedCreateColumnDescriptor,
    pub columns: Vec<ResolvedCreateColumnDescriptor>,
}

/// Removes a table
#[derive(Debug)]
pub struct RemoveTable {
    pub name: String,
}

/// Removes all records from the table
#[derive(Debug)]
pub struct ClearTable {
    pub name: String,
}

/// Renames a table
#[derive(Debug)]
pub struct RenameTable {
    pub prev_name: String,
    pub new_name: String,
}

/// Adds a new column.
#[derive(Debug)]
pub struct AddColumn {
    pub table_name: String,
    pub column_name: String,
    pub column_ty: Type,
}

/// Removes a column.
#[derive(Debug)]
pub struct RemoveColumn {
    pub table_name: String,
    pub column_name: String,
}

/// Renames a column
#[derive(Debug)]
pub struct RenameColumn {
    pub table_name: String,
    pub prev_column_name: String,
    pub new_column_name: String,
}

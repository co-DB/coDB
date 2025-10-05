use crate::b_tree_node::{
    BTreeInternalNode, BTreeKey, BTreeLeafNode, BTreeNode, BTreeNodeError, NodeSearchResult,
    NodeType, RecordPointer, get_node_type,
};
use crate::cache::{Cache, CacheError, FilePageRef, PinnedReadPage};
use crate::files_manager::FileKey;
use crate::paged_file::PageId;
use crate::slotted_page::PageType::BTreeInternal;
use crate::slotted_page::{SlotId, SlottedPage};
use std::marker::PhantomData;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum BTreeError {
    #[error("cache error occurred: {0}")]
    CacheError(#[from] CacheError),
    #[error("node error occurred: {0}")]
    NodeError(#[from] BTreeNodeError),
}
pub(crate) struct BTree<Key: BTreeKey> {
    _key_marker: PhantomData<Key>,
    file_key: FileKey,
    cache: Arc<Cache>,
    root_page_id: PageId,
}

#[derive(Debug)]
/// Enum representing possible outcomes of a search operation.
pub(crate) enum TreeSearchResult {
    /// Leaf node: exact match found.
    Found { record_ptr: RecordPointer },

    /// Leaf node: key not found, but insertion point identified.
    NotFound { insert_slot_id: SlotId },
}

impl<Key: BTreeKey> BTree<Key> {
    fn search(&self, key: &Key) -> Result<TreeSearchResult, BTreeError> {
        let mut current_page_id = self.root_page_id;

        loop {
            let page = self
                .cache
                .pin_read(&FilePageRef::new(current_page_id, self.file_key.clone()))?;

            let node_type = get_node_type(&page)?;

            match node_type {
                NodeType::Internal => {
                    let node = BTreeInternalNode::<PinnedReadPage, Key>::new(page)?;
                    match node.search(key)? {
                        NodeSearchResult::FollowChild { child_ptr } => current_page_id = child_ptr,
                        _ => unreachable!(),
                    }
                }
                NodeType::Leaf => {
                    let node = BTreeLeafNode::<PinnedReadPage, Key>::new(page)?;
                    match node.search(key)? {
                        NodeSearchResult::Found { record_ptr } => {
                            return Ok(TreeSearchResult::Found { record_ptr });
                        }
                        NodeSearchResult::NotFoundLeaf { insert_slot_id } => {
                            return Ok(TreeSearchResult::NotFound { insert_slot_id });
                        }
                        _ => unreachable!(),
                    }
                }
            }
        }
    }
}

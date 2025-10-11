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

impl<Key: BTreeKey> BTree<Key> {
    //TODO: Implement a way to read/write metadata (separate struct?)
    fn new(cache: Arc<Cache>, root_page_id: PageId, file_key: FileKey) -> Self {
        Self {
            cache,
            file_key,
            root_page_id,
            _key_marker: PhantomData,
        }
    }

    pub fn search(&self, key: &Key) -> Result<Option<RecordPointer>, BTreeError> {
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
                            return Ok(Some(record_ptr));
                        }
                        NodeSearchResult::NotFoundLeaf { .. } => {
                            return Ok(None);
                        }
                        _ => unreachable!(),
                    }
                }
            }
        }
    }

    /// Insert strategy:
    ///
    /// 1) Optimistic:
    /// First we go down the tree using only read latches and after reaching the leaf we try to upgrade
    /// the latch to write. This can fail
    pub(crate) fn insert() {}
}

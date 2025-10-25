use crate::b_tree_node::{
    BTreeInternalNode, BTreeKey, BTreeLeafNode, BTreeNode, BTreeNodeError, LeafNodeSearchResult,
    NodeInsertResult, NodeType, RecordPointer, get_node_type,
};
use crate::cache::{Cache, CacheError, FilePageRef, PageWrite, PinnedReadPage, PinnedWritePage};
use crate::data_types::{DbSerializable, DbSerializationError};
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
    #[error("tried to insert a duplicate key")]
    DuplicateKey,
    #[error("deserialization error occurred: {0}")]
    DeserializationError(#[from] DbSerializationError),
}

pub(crate) struct BTree<Key: BTreeKey> {
    _key_marker: PhantomData<Key>,
    file_key: FileKey,
    cache: Arc<Cache>,
    root_page_id: PageId,
    is_unique: bool,
}

impl<Key: BTreeKey> BTree<Key> {
    fn new(cache: Arc<Cache>, root_page_id: PageId, file_key: FileKey, is_unique: bool) -> Self {
        Self {
            cache,
            file_key,
            root_page_id,
            is_unique,
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
                    current_page_id = node.search(key)?.child_ptr;
                }
                NodeType::Leaf => {
                    let node = BTreeLeafNode::<PinnedReadPage, Key>::new(page)?;
                    return match node.search(key)? {
                        LeafNodeSearchResult::Found { record_ptr } => Ok(Some(record_ptr)),
                        LeafNodeSearchResult::NotFoundLeaf { .. } => Ok(None),
                    };
                }
            }
        }
    }

    /// Insert strategy:
    ///
    /// 1) Optimistic:
    /// First we go down the tree using only read latches and after reaching the leaf we try to upgrade
    /// the latch to write. This can fail
    pub(crate) fn insert(
        &mut self,
        key: Key,
        record_pointer: RecordPointer,
    ) -> Result<(), BTreeError> {
        let optimistic_succeeded = self.insert_optimistic(&key, &record_pointer)?;
        if optimistic_succeeded {
            return Ok(());
        }
        self.insert_pessimistic(key, record_pointer)
    }

    fn insert_optimistic(
        &mut self,
        key: &Key,
        record_pointer: &RecordPointer,
    ) -> Result<bool, BTreeError> {
        let mut current_page_id = self.root_page_id;

        loop {
            let page = self
                .cache
                .pin_read(&FilePageRef::new(current_page_id, self.file_key.clone()))?;

            let node_type = get_node_type(&page)?;

            match node_type {
                NodeType::Internal => {
                    let node = BTreeInternalNode::<PinnedReadPage, Key>::new(page)?;
                    current_page_id = node.search(key)?.child_ptr;
                }
                NodeType::Leaf => {
                    drop(page);
                    let write_page = self
                        .cache
                        .pin_write(&FilePageRef::new(current_page_id, self.file_key.clone()))?;

                    let mut node = BTreeLeafNode::<PinnedWritePage, Key>::new(write_page)?;

                    return match node.insert(key.clone(), record_pointer.clone(), self.is_unique)? {
                        NodeInsertResult::Success => Ok(true),
                        NodeInsertResult::PageFull => Ok(false),
                        NodeInsertResult::KeyAlreadyExists => Err(BTreeError::DuplicateKey),
                    };
                }
            }
        }
    }

    fn insert_pessimistic(
        &mut self,
        key: Key,
        record_pointer: RecordPointer,
    ) -> Result<(), BTreeError> {
        let mut current_page_id = self.root_page_id;

        let mut latch_stack: Vec<(PageId, BTreeInternalNode<PinnedWritePage, Key>)> =
            Vec::with_capacity(16);
        loop {
            let page = self
                .cache
                .pin_write(&FilePageRef::new(current_page_id, self.file_key.clone()))?;

            let node_type = get_node_type(&page)?;

            match node_type {
                NodeType::Internal => {
                    let node = BTreeInternalNode::<PinnedWritePage, Key>::new(page)?;

                    current_page_id = node.search(&key)?.child_ptr;

                    if node.can_fit_another()? {
                        latch_stack.clear();
                    }

                    latch_stack.push((current_page_id, node));
                }
                NodeType::Leaf => {
                    let mut node = BTreeLeafNode::<PinnedWritePage, Key>::new(page)?;

                    return match node.insert(key.clone(), record_pointer.clone(), self.is_unique)? {
                        NodeInsertResult::Success => Ok(()),
                        NodeInsertResult::PageFull => self.split_and_propagate(
                            latch_stack,
                            node,
                            key.clone(),
                            record_pointer.clone(),
                            self.is_unique,
                        ),
                        NodeInsertResult::KeyAlreadyExists => Err(BTreeError::DuplicateKey),
                    };
                }
            }
        }
    }

    fn split_and_propagate(
        &mut self,
        mut internal_nodes: Vec<(PageId, BTreeInternalNode<PinnedWritePage, Key>)>,
        mut leaf_node: BTreeLeafNode<PinnedWritePage, Key>,
        key: Key,
        record_pointer: RecordPointer,
        is_unique: bool,
    ) -> Result<(), BTreeError> {
        // First we split half (by size) of the key in leaf node and get the separator key.
        let (records, separator_key) = leaf_node.split_keys()?;

        // We must get the next leaf's page id from the leaf node being split.
        let next_leaf_id = leaf_node.next_leaf_id()?;

        // Then we create a new leaf node that will take those keys.
        let (new_page, new_leaf_id) = self.cache.allocate_page(&self.file_key)?;
        let mut new_leaf_node =
            BTreeLeafNode::<PinnedWritePage, Key>::initialize(new_page, next_leaf_id);

        // Insert the keys into the newly created leaf.
        new_leaf_node.batch_insert(records)?;

        // Make the old leaf point to the new one
        leaf_node.set_next_leaf_id(Some(new_leaf_id))?;

        if separator_key > key {
            leaf_node.insert(key, record_pointer, is_unique)?;
        } else if separator_key < key {
            new_leaf_node.insert(key, record_pointer, is_unique)?;
        } else if is_unique {
            return Err(BTreeError::DuplicateKey);
        } else {
            // TODO: Change this for indexes.
            unimplemented!()
        }

        // We have the leaf nodes set up correctly, so we must now propagate the separator key
        // along with the new leaf node page id up to the parent internal node.

        // This means we are in root.
        if internal_nodes.is_empty() {
            // Since we need to put the new separator key somewhere, we need to create a new root.
            // TODO: Change the root page id here
            return self.create_new_root(self.root_page_id, separator_key, new_leaf_id);
        } else {
            let mut current_separator_key = separator_key.clone();
            let mut child_page_id = new_leaf_id;
            while let Some((page_id, mut internal_node)) = internal_nodes.pop() {
                match internal_node.insert(current_separator_key.clone(), page_id)? {
                    NodeInsertResult::KeyAlreadyExists => return Err(BTreeError::DuplicateKey),
                    NodeInsertResult::Success => return Ok(()),
                    NodeInsertResult::PageFull => {
                        // Need to split this internal node too
                        let (split_records, new_separator) = internal_node.split_keys()?;

                        let (new_internal_page, new_internal_id) =
                            self.cache.allocate_page(&self.file_key)?;
                        let mut new_internal_node =
                            BTreeInternalNode::<PinnedWritePage, Key>::initialize(
                                new_internal_page,
                            );

                        // The first record in split_records contains the leftmost child
                        // pointer for the new internal node. This is because the key of this record
                        // is moved to the parent node and thus its pointer (which points to a page
                        // with keys >= key[0] && < key[1]) now fulfills the criteria to be leftmost
                        // child pointer.
                        let (_, child_ptr_bytes) = Key::deserialize(&split_records[0])?;
                        let (child_ptr, _) = PageId::deserialize(child_ptr_bytes)?;

                        // We don't insert the first key as it is moved up to parent
                        new_internal_node.batch_insert(split_records[1..].to_vec())?;
                        new_internal_node.set_leftmost_child_id(child_ptr)?;

                        if new_separator > current_separator_key {
                            internal_node.insert(current_separator_key, child_page_id)?;
                        } else if new_separator < current_separator_key {
                            new_internal_node.insert(current_separator_key, child_page_id)?;
                        } else if is_unique {
                            return Err(BTreeError::DuplicateKey);
                        } else {
                            // TODO: Change this for indexes.
                            unimplemented!()
                        }

                        current_separator_key = new_separator;
                        child_page_id = new_internal_id;

                        // If this was the last internal node (the root), create new root
                        // TODO: Change the root page id here
                        if internal_nodes.is_empty() {
                            return self.create_new_root(
                                self.root_page_id,
                                current_separator_key,
                                child_page_id,
                            );
                        }
                    }
                };
            }
        }
        Ok(())
    }

    fn create_new_root(
        &mut self,
        left_child_id: PageId,
        separator_key: Key,
        right_child_id: PageId,
    ) -> Result<(), BTreeError> {
        let (new_root_page, new_root_id) = self.cache.allocate_page(&self.file_key)?;
        let mut new_root = BTreeInternalNode::<PinnedWritePage, Key>::initialize(new_root_page);

        // Set the leftmost child to point to the left child (old root or left sibling)
        new_root.set_leftmost_child_id(left_child_id)?;

        // Insert the separator key with the right child pointer
        new_root.insert(separator_key, right_child_id)?;

        // TODO: Set new root id when root page manager exists
        // e.g Self::set_new_root_id(new_root_id);

        Ok(())
    }
}

use crate::b_tree_node::{
    BTreeInternalNode, BTreeKey, BTreeLeafNode, BTreeNode, BTreeNodeError, LeafNodeSearchResult,
    NodeInsertResult, NodeType, RecordPointer, get_node_type,
};
use crate::cache::{Cache, CacheError, FilePageRef, PageWrite, PinnedReadPage, PinnedWritePage};
use crate::data_types::{DbSerializable, DbSerializationError};
use crate::files_manager::FileKey;
use crate::paged_file::{Page, PageId};
use crate::slotted_page::SlottedPage;
use bytemuck::{Pod, PodCastError, Zeroable};
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
    #[error("metadata of the b-tree was corrupted: {reason}")]
    CorruptMetadata { reason: String },
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
struct BTreeMetadata {
    magic_number: [u8; 4],
    root_page_id: PageId,
}

impl BTreeMetadata {
    const CODB_MAGIC_NUMBER: [u8; 4] = [0xC, 0x0, 0xD, 0xB];
    fn new(root_page_id: PageId) -> Self {
        Self {
            magic_number: BTreeMetadata::CODB_MAGIC_NUMBER,
            root_page_id,
        }
    }
}

impl TryFrom<&Page> for BTreeMetadata {
    type Error = BTreeError;
    fn try_from(value: &Page) -> Result<Self, Self::Error> {
        let result = bytemuck::try_from_bytes::<BTreeMetadata>(value);
        match result {
            Ok(metadata) => {
                if metadata.magic_number != Self::CODB_MAGIC_NUMBER {
                    return Err(BTreeError::CorruptMetadata {
                        reason: format!("invalid magic number ('{:?}')", metadata.magic_number),
                    });
                }
                Ok(*metadata)
            }
            Err(e) => Err(BTreeError::CorruptMetadata {
                reason: e.to_string(),
            }),
        }
    }
}

pub(crate) struct BTree<Key: BTreeKey> {
    _key_marker: PhantomData<Key>,
    file_key: FileKey,
    cache: Arc<Cache>,
    is_unique: bool,
}

impl<Key: BTreeKey> BTree<Key> {
    const METADATA_PAGE_ID: PageId = 1;
    fn read_root_page_id(&self) -> Result<PageId, BTreeError> {
        let metadata_page = self.cache.pin_read(&FilePageRef::new(
            BTree::<Key>::METADATA_PAGE_ID,
            self.file_key.clone(),
        ))?;

        let metadata = BTreeMetadata::try_from(metadata_page.page())?;

        Ok(metadata.root_page_id)
    }

    fn new(cache: Arc<Cache>, file_key: FileKey, is_unique: bool) -> Self {
        Self {
            cache,
            file_key,
            is_unique,
            _key_marker: PhantomData,
        }
    }

    pub fn search(&self, key: &Key) -> Result<Option<RecordPointer>, BTreeError> {
        let mut current_page_id = self.read_root_page_id()?;

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
    /// 1) Optimistic
    /// First we go down the tree using only read latches and after reaching the leaf we upgrade
    /// the latch to write. Then we try to insert the new key into leaf, which can fail if the node
    /// is full and needs to be split. We can't do that right there, as we don't have write latches on
    /// nodes up the path that may need to be split too. Thus we need to retry the whole operation
    /// keeping the write latches - the pessimistic strategy.
    ///
    /// 2) Pessimistic
    /// Here we go down the tree while keeping write latches on nodes that may need to take in an
    /// additional key (meaning their child may need to be split). We let go of a latched node if
    /// their child, that we descend into, has enough space to fit another key. That's because
    /// we know that the child can't become full and won't need to be split. If we reach the leaf
    /// and the node is still full we start the recursive split operation.
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
        let mut current_page_id = self.read_root_page_id()?;

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

                    return match node.insert(key.clone(), record_pointer.clone())? {
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
        let mut metadata_page = Some(self.cache.pin_write(&FilePageRef::new(
            BTree::<Key>::METADATA_PAGE_ID,
            self.file_key.clone(),
        ))?);

        let root_page_id =
            BTreeMetadata::try_from(metadata_page.as_ref().unwrap().page())?.root_page_id;

        let mut current_page_id = root_page_id;
        let mut latch_stack: Vec<BTreeInternalNode<PinnedWritePage, Key>> = Vec::with_capacity(16);

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
                        // We can always drop it since if it was dropped before it will just
                        // drop(None) which is a no-op.
                        drop(metadata_page.take());
                        latch_stack.clear();
                    }

                    latch_stack.push(node);
                }
                NodeType::Leaf => {
                    let mut node = BTreeLeafNode::<PinnedWritePage, Key>::new(page)?;

                    return match node.insert(key.clone(), record_pointer.clone())? {
                        NodeInsertResult::Success => Ok(()),
                        NodeInsertResult::PageFull => self.split_and_propagate(
                            latch_stack,
                            node,
                            key.clone(),
                            record_pointer.clone(),
                            metadata_page.take(),
                        ),
                        NodeInsertResult::KeyAlreadyExists => Err(BTreeError::DuplicateKey),
                    };
                }
            }
        }
    }

    fn split_and_propagate(
        &mut self,
        mut internal_nodes: Vec<BTreeInternalNode<PinnedWritePage, Key>>,
        mut leaf_node: BTreeLeafNode<PinnedWritePage, Key>,
        key: Key,
        record_pointer: RecordPointer,
        metadata_page: Option<PinnedWritePage>,
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
            leaf_node.insert(key, record_pointer)?;
        } else if separator_key < key {
            new_leaf_node.insert(key, record_pointer)?;
        } else {
            return Err(BTreeError::DuplicateKey);
        }
        // We have the leaf nodes set up correctly, so we must now propagate the separator key
        // along with the new leaf node page id up to the parent internal node.

        // This means we are in root.
        if internal_nodes.is_empty() {
            // We can unwrap here because as the metadata is guaranteed to be non-released here.
            let root_page_id =
                BTreeMetadata::try_from(metadata_page.as_ref().unwrap().page())?.root_page_id;
            return self.create_new_root(
                root_page_id,
                separator_key,
                new_leaf_id,
                metadata_page.unwrap(),
            );
        } else {
            let mut current_separator_key = separator_key.clone();
            let mut child_page_id = new_leaf_id;
            while let Some(mut internal_node) = internal_nodes.pop() {
                match internal_node.insert(current_separator_key.clone(), child_page_id)? {
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
                        } else {
                            return Err(BTreeError::DuplicateKey);
                        }

                        current_separator_key = new_separator;
                        child_page_id = new_internal_id;

                        // If this was the last internal node (the root), create new root
                        if internal_nodes.is_empty() {
                            // We can unwrap here because as the metadata is guaranteed to be non-released here.
                            let root_page_id =
                                BTreeMetadata::try_from(metadata_page.as_ref().unwrap().page())?
                                    .root_page_id;
                            return self.create_new_root(
                                root_page_id,
                                current_separator_key,
                                child_page_id,
                                metadata_page.unwrap(),
                            );
                        }

                        drop(internal_node);
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
        mut metadata_page: PinnedWritePage,
    ) -> Result<(), BTreeError> {
        let (new_root_page, new_root_id) = self.cache.allocate_page(&self.file_key)?;
        let mut new_root = BTreeInternalNode::<PinnedWritePage, Key>::initialize(new_root_page);

        // Set the leftmost child to point to the left child (old root or left sibling)
        new_root.set_leftmost_child_id(left_child_id)?;

        // Insert the separator key with the right child pointer
        new_root.insert(separator_key, right_child_id)?;

        let metadata = match bytemuck::try_from_bytes_mut::<BTreeMetadata>(metadata_page.page_mut())
        {
            Ok(metadata) => metadata,
            Err(e) => {
                return Err(BTreeError::CorruptMetadata {
                    reason: e.to_string(),
                });
            }
        };

        metadata.root_page_id = new_root_id;

        Ok(())
    }
}

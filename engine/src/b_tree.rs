use storage::paged_file::{Page, PageId, PagedFileError};

use crate::b_tree_node::{
    BTreeInternalNode, BTreeLeafNode, BTreeNodeError, ChildPosition, LeafNodeSearchResult,
    NodeDeleteResult, NodeInsertResult, NodeType, get_node_type,
};
use crate::heap_file::RecordPtr;
use crate::slotted_page::SlotId;
use bytemuck::{Pod, Zeroable};
use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use storage::cache::{Cache, CacheError, FilePageRef, PageWrite, PinnedReadPage, PinnedWritePage};
use storage::files_manager::FileKey;
use thiserror::Error;
use types::serialization::{DbSerializable, DbSerializationError};

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

/// Metadata stored at the start of the B-tree, including
/// a magic number for validation and the root page ID.
#[derive(Pod, Zeroable, Copy, Clone, Debug)]
#[repr(C)]
struct BTreeMetadata {
    magic_number: [u8; 4],
    root_page_id: PageId,
}

impl BTreeMetadata {
    const CODB_MAGIC_NUMBER: [u8; 4] = [0xC, 0x0, 0xD, 0xB];

    const SIZE: usize = size_of::<BTreeMetadata>();

    /// Creates a new metadata instance with a specified root page ID.
    fn new(root_page_id: PageId) -> Self {
        Self {
            magic_number: BTreeMetadata::CODB_MAGIC_NUMBER,
            root_page_id,
        }
    }

    fn save_to_page(&self, page: &mut impl PageWrite) {
        let metadata = BTreeMetadata::new(self.root_page_id);
        let metadata_bytes = bytemuck::bytes_of(&metadata);
        page.data_mut()[0..metadata_bytes.len()].copy_from_slice(metadata_bytes);
    }
}

impl TryFrom<&Page> for BTreeMetadata {
    type Error = BTreeError;
    fn try_from(value: &Page) -> Result<Self, Self::Error> {
        let result = bytemuck::try_from_bytes::<BTreeMetadata>(&value[..BTreeMetadata::SIZE]);
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

/// Result of an optimistic operation (insert/delete) attempt
#[derive(Debug)]
enum OptimisticOperationResult {
    Success,
    StructuralChangeRetry,
    FullNodeRetry,
}

/// Represents a snapshot of a page's version for optimistic concurrency control
struct PageVersion {
    page_id: PageId,
    version: u16,
}

impl PageVersion {
    fn new(page_id: PageId, version: u16) -> Self {
        Self { page_id, version }
    }
}

/// Stores information about the path from root to leaf during a pessimistic insert, including
/// ancestor nodes and metadata page.
struct PessimisticPath {
    latch_stack: Vec<LatchHandle>,
    leaf_page_id: PageId,
    metadata_page: Option<PinnedWritePage>,
}

/// A helper struct for all the necessary data needed for performing operations on a node during
/// tree restructuring operations (split,merge,redistribution).
struct LatchHandle {
    /// ID of the page containing the node.
    page_id: PageId,
    /// The actual node.
    node: BTreeInternalNode<PinnedWritePage>,
    /// Position of the child we descended into in the node. Used in delete to get the chosen node's
    /// siblings.
    child_pos: ChildPosition,
}

impl LatchHandle {
    fn new(
        page_id: PageId,
        node: BTreeInternalNode<PinnedWritePage>,
        child_pos: ChildPosition,
    ) -> Self {
        Self {
            page_id,
            node,
            child_pos,
        }
    }
}
impl PessimisticPath {
    fn new(
        latch_stack: Vec<LatchHandle>,
        leaf_page_id: PageId,
        metadata_page: Option<PinnedWritePage>,
    ) -> Self {
        Self {
            latch_stack,
            leaf_page_id,
            metadata_page,
        }
    }
}

struct NodeLatch {
    node_page_id: PageId,
    node: BTreeInternalNode<PinnedWritePage>,
}

struct MergeContext {
    parent_id: PageId,
    parent_node: BTreeInternalNode<PinnedWritePage>,
    child_pos: ChildPosition,
    internal_nodes: Vec<LatchHandle>,
    metadata_page: Option<PinnedWritePage>,
}

/// Structure responsible for managing on-disk index files.
///
/// Each [`BTree`] instance corresponds to a single physical file on disk.
pub(crate) struct BTree {
    file_key: FileKey,
    cache: Arc<Cache>,
    /// A concurrent hash map for storing the current structural version numbers (how many times a
    /// node was split or merged) for each node in the B-Tree. Used for optimistic insert to avoid
    /// inserting into a leaf based on a stale path.
    structural_version_numbers: DashMap<PageId, AtomicU16>,
}

impl BTree {
    const METADATA_PAGE_ID: PageId = 1;

    /// Reads the root page ID from the metadata page.
    fn read_root_page_id(&self) -> Result<PageId, BTreeError> {
        let metadata_page = self.cache.pin_read(&FilePageRef::new(
            BTree::METADATA_PAGE_ID,
            self.file_key.clone(),
        ))?;

        let metadata = BTreeMetadata::try_from(metadata_page.page())?;

        Ok(metadata.root_page_id)
    }

    fn new(cache: Arc<Cache>, file_key: FileKey) -> Result<Self, BTreeError> {
        Ok(Self {
            cache,
            file_key,
            structural_version_numbers: DashMap::new(),
        })
    }

    fn page_ref(&self, page_id: PageId) -> FilePageRef {
        FilePageRef::new(page_id, self.file_key.clone())
    }

    fn pin_write(&self, page_id: PageId) -> Result<PinnedWritePage, BTreeError> {
        Ok(self.cache.pin_write(&self.page_ref(page_id))?)
    }

    fn pin_read(&self, page_id: PageId) -> Result<PinnedReadPage, BTreeError> {
        Ok(self.cache.pin_read(&self.page_ref(page_id))?)
    }

    fn pin_leaf_for_write(
        &self,
        leaf_page_id: PageId,
    ) -> Result<BTreeLeafNode<PinnedWritePage>, BTreeError> {
        let page = self.pin_write(leaf_page_id)?;
        Ok(BTreeLeafNode::<PinnedWritePage>::new(page)?)
    }

    fn pin_internal_for_write(
        &self,
        internal_page_id: PageId,
    ) -> Result<BTreeInternalNode<PinnedWritePage>, BTreeError> {
        let page = self.pin_write(internal_page_id)?;
        Ok(BTreeInternalNode::<PinnedWritePage>::new(page)?)
    }

    /// Searches for a key in the B-tree and returns the corresponding record pointer (to heap
    /// file record) if the key was found.
    pub fn search(&self, key: &[u8]) -> Result<Option<RecordPtr>, BTreeError> {
        let mut current_page_id = self.read_root_page_id()?;

        loop {
            let page = self.pin_read(current_page_id)?;

            let node_type = get_node_type(&page)?;

            match node_type {
                NodeType::Internal => {
                    let node = BTreeInternalNode::<PinnedReadPage>::new(page)?;
                    current_page_id = node.search(key)?.child_ptr();
                }
                NodeType::Leaf => {
                    let node = BTreeLeafNode::<PinnedReadPage>::new(page)?;
                    return match node.search(key)? {
                        LeafNodeSearchResult::Found { record_ptr, .. } => Ok(Some(record_ptr)),
                        LeafNodeSearchResult::NotFoundLeaf { .. } => Ok(None),
                    };
                }
            }
        }
    }

    /// Insert strategy:
    ///
    /// Optimistic:
    /// First we go down the tree using only read latches and after reaching the leaf we upgrade
    /// the latch to write. If any ancestor was structurally changed (meaning split or merged), we
    /// try to retry with optimistic again. Then we try to insert the new key into leaf, which can
    /// fail if the node is full and needs to be split. We can't do that right there, as we don't
    /// have write latches on nodes up the path that may need to be split too. Thus, we need to
    /// retry the whole operation keeping the write latches - the pessimistic strategy.
    ///
    /// Pessimistic:
    /// Here we go down the tree while keeping write latches on nodes that may need to take in an
    /// additional key (meaning their child may need to be split). We let go of a latched node if
    /// their child, that we descend into, has enough space to fit another key. That's because
    /// we know that the child can't become full and won't need to be split. If we reach the leaf
    /// and the node is still full we start the recursive split operation.
    pub(crate) fn insert(&self, key: &[u8], record_pointer: RecordPtr) -> Result<(), BTreeError> {
        let optimistic_result = self.insert_optimistic(key, &record_pointer)?;
        match optimistic_result {
            OptimisticOperationResult::Success => Ok(()),
            OptimisticOperationResult::StructuralChangeRetry => {
                // Retry optimistically before going to pessimistic insert. This makes sense as
                // splits of a node in a given path shouldn't occur too often.
                match self.insert_optimistic(key, &record_pointer)? {
                    OptimisticOperationResult::Success => Ok(()),
                    _ => self.insert_pessimistic(key, record_pointer),
                }
            }
            OptimisticOperationResult::FullNodeRetry => {
                self.insert_pessimistic(key, record_pointer)
            }
        }
    }

    /// Traverses the tree to the leaf while recording page versions for optimistic concurrency checks.
    fn traverse_with_versions(&self, key: &[u8]) -> Result<(PageId, Vec<PageVersion>), BTreeError> {
        let mut current_page_id = self.read_root_page_id()?;
        let mut path_versions = Vec::new();

        loop {
            // Record page version before descending (necessary here before page read, because if a
            // split happens before pin it won't work)
            let version = self
                .structural_version_numbers
                .get(&current_page_id)
                .map_or(0, |v| v.load(Ordering::Acquire));

            let page = self
                .cache
                .pin_read(&FilePageRef::new(current_page_id, self.file_key.clone()))?;

            path_versions.push(PageVersion::new(current_page_id, version));

            match get_node_type(&page)? {
                NodeType::Internal => {
                    let node = BTreeInternalNode::<PinnedReadPage>::new(page)?;
                    current_page_id = node.search(key)?.child_ptr();
                }
                NodeType::Leaf => {
                    return Ok((current_page_id, path_versions));
                }
            }
        }
    }

    /// Checks if any pages in the path have changed since they were read optimistically.
    fn detect_structural_changes(&self, path_versions: &[PageVersion]) -> bool {
        path_versions.iter().any(|page_version| {
            let current_version = self
                .structural_version_numbers
                .get(&page_version.page_id)
                .map_or(0, |v| v.load(Ordering::Acquire));
            page_version.version != current_version
        })
    }

    /// Performs an optimistic insert attempt at the leaf node.
    fn insert_optimistic(
        &self,
        key: &[u8],
        record_pointer: &RecordPtr,
    ) -> Result<OptimisticOperationResult, BTreeError> {
        // Traverse and collect version info.
        let (leaf_page_id, path_versions) = self.traverse_with_versions(key)?;

        // Try upgrading to a write latch on the leaf.
        let mut leaf_node = self.pin_leaf_for_write(leaf_page_id)?;

        // Check if any node in the path changed structurally.
        if self.detect_structural_changes(&path_versions) {
            return Ok(OptimisticOperationResult::StructuralChangeRetry);
        }

        match leaf_node.insert(key, *record_pointer)? {
            NodeInsertResult::Success => Ok(OptimisticOperationResult::Success),
            NodeInsertResult::PageFull => Ok(OptimisticOperationResult::FullNodeRetry),
            NodeInsertResult::KeyAlreadyExists => Err(BTreeError::DuplicateKey),
        }
    }

    ///  Traverses the tree while keeping write latches on nodes that may need to split.
    fn traverse_pessimistic(
        &self,
        key: &[u8],
        drop_lock_fn: impl Fn(&BTreeInternalNode<PinnedWritePage>) -> Result<bool, BTreeError>,
    ) -> Result<PessimisticPath, BTreeError> {
        // Pin the metadata (in case root splits)
        let mut metadata_page = Some(self.pin_write(Self::METADATA_PAGE_ID)?);
        let mut current_page_id =
            BTreeMetadata::try_from(metadata_page.as_ref().unwrap().page())?.root_page_id;

        // We need to keep a write pin on all ancestors in case we need to insert a separator key or
        // split them.
        let mut latch_stack = Vec::with_capacity(16);

        loop {
            let page = self.pin_write(current_page_id)?;
            match get_node_type(&page)? {
                NodeType::Internal => {
                    let node = BTreeInternalNode::<PinnedWritePage>::new(page)?;
                    let old_page_id = current_page_id;

                    let search_result = node.search(key)?;

                    current_page_id = search_result.child_ptr();
                    let child_pos = search_result.child_pos();

                    // If child can fit another, we can safely drop ancestors and metadata.
                    if drop_lock_fn(&node)? {
                        drop(metadata_page.take());
                        latch_stack.clear();
                    }

                    let latch_handle = LatchHandle::new(old_page_id, node, child_pos);
                    latch_stack.push(latch_handle);
                }
                NodeType::Leaf => {
                    return Ok(PessimisticPath {
                        latch_stack,
                        leaf_page_id: current_page_id,
                        metadata_page,
                    });
                }
            }
        }
    }

    /// Performs a pessimistic insert, splitting nodes as necessary.
    fn insert_pessimistic(&self, key: &[u8], record_pointer: RecordPtr) -> Result<(), BTreeError> {
        let path = self.traverse_pessimistic(key, |node| Ok(node.can_fit_another()?))?;

        let mut leaf = self.pin_leaf_for_write(path.leaf_page_id)?;

        match leaf.insert(key, record_pointer)? {
            NodeInsertResult::Success => Ok(()),
            NodeInsertResult::PageFull => self.split_and_propagate(
                path.latch_stack,
                (path.leaf_page_id, leaf),
                key,
                record_pointer,
                path.metadata_page,
            ),
            NodeInsertResult::KeyAlreadyExists => Err(BTreeError::DuplicateKey),
        }
    }

    fn update_structural_version(&self, page_id: PageId) {
        self.structural_version_numbers
            .entry(page_id)
            .or_insert(AtomicU16::new(0))
            .fetch_add(1, Ordering::Release);
    }

    /// Allocates a new leaf page and initializes it with the given `next_leaf_id`.
    fn allocate_and_init_leaf(
        &self,
        next_leaf_id: Option<PageId>,
    ) -> Result<(PageId, BTreeLeafNode<PinnedWritePage>), BTreeError> {
        let (new_page, new_leaf_id) = self.cache.allocate_page(&self.file_key)?;
        Ok((
            new_leaf_id,
            BTreeLeafNode::<PinnedWritePage>::initialize(new_page, next_leaf_id)?,
        ))
    }

    /// Allocates a new internal page and initializes it with the given `leftmost_child_id`.
    fn allocate_and_init_internal(
        &self,
        leftmost_child_id: PageId,
    ) -> Result<(PageId, BTreeInternalNode<PinnedWritePage>), BTreeError> {
        let (new_page, new_internal_id) = self.cache.allocate_page(&self.file_key)?;
        Ok((
            new_internal_id,
            BTreeInternalNode::<PinnedWritePage>::initialize(new_page, leftmost_child_id)?,
        ))
    }

    /// Splits a leaf node and propagates the separator up the tree.
    fn split_and_propagate(
        &self,
        internal_nodes: Vec<LatchHandle>,
        leaf_node: (PageId, BTreeLeafNode<PinnedWritePage>),
        key: &[u8],
        record_pointer: RecordPtr,
        metadata_page: Option<PinnedWritePage>,
    ) -> Result<(), BTreeError> {
        let (leaf_page_id, leaf_node) = leaf_node;
        // Split the leaf and get separator + new leaf id
        let (separator_key, new_leaf_id) =
            self.split_leaf(leaf_page_id, leaf_node, key, record_pointer)?;

        // Propagate separator up the stack (or create new root if stack empty)
        self.propagate_separator_up(internal_nodes, separator_key, new_leaf_id, metadata_page)
    }

    /// Splits the provided leaf node and creates a new one with the split keys. Returns a pair
    /// of separator key and the new leaf node's page id.
    fn split_leaf(
        &self,
        leaf_page_id: PageId,
        mut leaf_node: BTreeLeafNode<PinnedWritePage>,
        key: &[u8],
        record_pointer: RecordPtr,
    ) -> Result<(Vec<u8>, PageId), BTreeError> {
        // Split keys of the leaf.
        let (records_to_move, separator_key) = leaf_node.split_keys()?;

        let next_leaf_id = leaf_node.next_leaf_id()?;

        // Allocate and initialize new leaf page.
        let (new_leaf_id, mut new_leaf_node) = self.allocate_and_init_leaf(next_leaf_id)?;

        // Insert split records into new leaf.
        new_leaf_node.batch_insert(records_to_move)?;

        // Point old leaf to new leaf.
        leaf_node.set_next_leaf_id(Some(new_leaf_id))?;

        // Insert the starting key into correct node.
        if key < separator_key.as_slice() {
            leaf_node.insert(key, record_pointer)?;
        } else if key > separator_key.as_slice() {
            new_leaf_node.insert(key, record_pointer)?;
        } else {
            return Err(BTreeError::DuplicateKey);
        }

        // Bump version.
        self.update_structural_version(leaf_page_id);

        Ok((separator_key, new_leaf_id))
    }

    /// Propagates a separator key upwards through internal nodes, splitting parents as needed and
    /// creating a new root if necessary.
    fn propagate_separator_up(
        &self,
        mut internal_nodes: Vec<LatchHandle>,
        mut current_separator_key: Vec<u8>,
        mut child_page_id: PageId,
        metadata_page: Option<PinnedWritePage>,
    ) -> Result<(), BTreeError> {
        // If there are no parents, create new root.
        if internal_nodes.is_empty() {
            // Must be Some here or this doesn't work.
            let metadata = metadata_page.unwrap();
            let root_page_id = BTreeMetadata::try_from(metadata.page())?.root_page_id;
            return self.create_new_root(
                root_page_id,
                current_separator_key,
                child_page_id,
                metadata,
            );
        }

        // Otherwise, walk ancestors from bottom to top.
        while let Some(handle) = internal_nodes.pop() {
            let LatchHandle {
                page_id: parent_page_id,
                node: mut parent_node,
                ..
            } = handle;
            match parent_node.insert(current_separator_key.as_slice(), child_page_id)? {
                NodeInsertResult::Success => return Ok(()),
                NodeInsertResult::KeyAlreadyExists => return Err(BTreeError::DuplicateKey),
                NodeInsertResult::PageFull => {
                    // Parent is full,so we must split it and continue upward.

                    // Split this internal node: obtain its split_records and new_separator
                    let (split_records, new_separator) = parent_node.split_keys()?;

                    // The first record's child pointer becomes the leftmost child of the new internal node.
                    let key_bytes_end = split_records[0].len() - size_of::<PageId>();
                    let (leftmost_child_ptr, _) =
                        PageId::deserialize(&split_records[0][key_bytes_end..])?;

                    // Allocate and initialize new internal page.
                    let (new_internal_id, mut new_internal_node) =
                        self.allocate_and_init_internal(leftmost_child_ptr)?;

                    // Move the remaining split_records[1..] into the new internal node.
                    // Here we omit the 0th record as in internal nodes it is moved up to the parent
                    // and doesn't remain in the child too like in leaf nodes.
                    new_internal_node.batch_insert(split_records[1..].to_vec())?;

                    // Insert separator key into correct node.
                    if current_separator_key < new_separator {
                        parent_node.insert(current_separator_key.as_slice(), child_page_id)?;
                    } else if current_separator_key > new_separator {
                        new_internal_node
                            .insert(current_separator_key.as_slice(), child_page_id)?;
                    } else {
                        return Err(BTreeError::DuplicateKey);
                    }

                    // Bump version for parent to indicate structural change.
                    self.update_structural_version(parent_page_id);

                    // The new separator and new child id will be propagated up
                    current_separator_key = new_separator;
                    child_page_id = new_internal_id;

                    // If we've emptied the latch stack (parent was root), create new root
                    if internal_nodes.is_empty() {
                        // Must be Some here or this doesn't work.
                        let metadata = metadata_page.unwrap();
                        let root_page_id = BTreeMetadata::try_from(metadata.page())?.root_page_id;
                        return self.create_new_root(
                            root_page_id,
                            current_separator_key,
                            child_page_id,
                            metadata,
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// Creates a new root and updates the metadata page.
    fn create_new_root(
        &self,
        left_child_id: PageId,
        separator_key: Vec<u8>,
        right_child_id: PageId,
        mut metadata_page: PinnedWritePage,
    ) -> Result<(), BTreeError> {
        let (new_root_id, mut new_root) = self.allocate_and_init_internal(left_child_id)?;

        // Insert the separator key with the right child pointer
        new_root.insert(separator_key.as_slice(), right_child_id)?;

        // Update the metadata to point to the new root
        let metadata_bytes = &mut metadata_page.page_mut()[0..size_of::<BTreeMetadata>()];
        let metadata =
            bytemuck::try_from_bytes_mut::<BTreeMetadata>(metadata_bytes).map_err(|e| {
                BTreeError::CorruptMetadata {
                    reason: e.to_string(),
                }
            })?;

        metadata.root_page_id = new_root_id;

        Ok(())
    }

    /// Deletes a key from the B-Tree. Works similarly to insert in that it first tries an optimistic
    /// approach, which uses read latches and only upgrades to write latch for the leaf node the key
    /// is found in. If that fails (due to structural change of the path or the possibility of underflow
    /// of one of the ancestors) we go to pessimistic strategy, which keeps write latches on all
    /// nodes in the path until we are sure that they won't need to be merged/redistributed into.
    /// For a little more info read [`insert`] docs.
    pub fn delete(&self, key: &[u8]) -> Result<(), BTreeError> {
        let optimistic_result = self.delete_optimistic(key)?;
        match optimistic_result {
            OptimisticOperationResult::Success => Ok(()),
            OptimisticOperationResult::StructuralChangeRetry => {
                // Retry optimistically before going to pessimistic delete. This makes sense as
                // merges of a node in a given path shouldn't occur too often.
                match self.delete_optimistic(key)? {
                    OptimisticOperationResult::Success => Ok(()),
                    _ => self.delete_pessimistic(key),
                }
            }
            OptimisticOperationResult::FullNodeRetry => self.delete_pessimistic(key),
        }
    }

    /// Tries to optimistically delete a key, failing if a need to redistribute keys or merge or
    /// a structural change occurs.
    fn delete_optimistic(&self, key: &[u8]) -> Result<OptimisticOperationResult, BTreeError> {
        // Traverse and collect version info.
        let (leaf_page_id, path_versions) = self.traverse_with_versions(key)?;

        // Try upgrading to a write latch on the leaf.
        let mut leaf_node = self.pin_leaf_for_write(leaf_page_id)?;

        // Check if any node in the path changed structurally.
        if self.detect_structural_changes(&path_versions) {
            return Ok(OptimisticOperationResult::StructuralChangeRetry);
        }

        match leaf_node.delete(key)? {
            NodeDeleteResult::Success => Ok(OptimisticOperationResult::Success),
            NodeDeleteResult::SuccessUnderflow => Ok(OptimisticOperationResult::FullNodeRetry),
            NodeDeleteResult::KeyDoesNotExist => {
                Ok(OptimisticOperationResult::StructuralChangeRetry)
            }
        }
    }

    /// Deletes the key, while keeping write latches on ancestor nodes, which may structurally change
    /// (possibly even root). Merges and redistributes as needed.
    fn delete_pessimistic(&self, key: &[u8]) -> Result<(), BTreeError> {
        let path =
            self.traverse_pessimistic(key, |node| Ok(node.will_not_underflow_after_delete()?))?;

        let mut leaf = self.pin_leaf_for_write(path.leaf_page_id)?;

        match leaf.delete(key)? {
            NodeDeleteResult::Success => Ok(()),
            NodeDeleteResult::SuccessUnderflow => self.redistribute_or_merge(
                path.latch_stack,
                (path.leaf_page_id, leaf),
                path.metadata_page,
            ),
            NodeDeleteResult::KeyDoesNotExist => Err(BTreeError::DuplicateKey),
        }
    }

    /// A function for fixing a leaf node that has an underflow (less than [`BTreeNode:UNDERFLOW_BOUNDARY`]
    /// fraction of space filled). First tries to move a key from one of the siblings (right, then left)
    /// and if that fails merges with one of the sibling (left one, unless it doesn't exist).
    fn redistribute_or_merge(
        &self,
        mut internal_nodes: Vec<LatchHandle>,
        leaf_node: (PageId, BTreeLeafNode<PinnedWritePage>),
        metadata_page: Option<PinnedWritePage>,
    ) -> Result<(), BTreeError> {
        let (node_id, mut node) = leaf_node;

        // We need the parent for separator updates.
        let parent = match internal_nodes.pop() {
            Some(p) => p,
            None => {
                // No parent means this is the root, so we can have an underflow here.
                return Ok(());
            }
        };

        let LatchHandle {
            page_id: parent_id,
            node: mut parent_node,
            child_pos,
        } = parent;

        self.update_structural_version(node_id);

        // First, try moving a key from right sibling.
        if let Some(right_sibling_id) = node.next_leaf_id()? {
            let mut right_sibling = self.pin_leaf_for_write(right_sibling_id)?;

            if right_sibling.can_redistribute_key()? {
                // Move first key from right sibling to current node.
                let redistributed_record = right_sibling.remove_first_key()?;
                node.insert_record(redistributed_record.as_slice())?;

                // Update separator in parent - new separator is the new first key of right sibling.
                let new_separator_key = right_sibling.get_first_key()?;
                let slot_id = child_pos
                    .slot_id()
                    .expect("child_pos must be AfterSlot to have a right sibling");
                parent_node.update_separator_at_slot(slot_id, &new_separator_key)?;

                return Ok(());
            }

            // No left sibling, so we need to merge.
            if child_pos == ChildPosition::Leftmost {
                let ctx = MergeContext {
                    parent_id,
                    parent_node,
                    child_pos,
                    internal_nodes,
                    metadata_page,
                };
                return self.merge_with_right_sibling(node, right_sibling_id, right_sibling, ctx);
            }
        }

        // Now do the same with left sibling.
        let preceding_child_pos = parent_node
            .get_preceding_child_pos(child_pos)?
            .expect("We know child pos is not Leftmost, so this will always return a position");

        let left_node_id = parent_node.get_child_ptr_at(preceding_child_pos)?;
        let mut left_sibling = self.pin_leaf_for_write(left_node_id)?;

        if left_sibling.can_redistribute_key()? {
            // Move last key from left sibling to current node.
            let redistributed_record = left_sibling.remove_last_key()?;

            // The redistributed key becomes the new separator (it's now the first key of current node).
            let new_separator_key = Self::extract_key_from_leaf_record(&redistributed_record);
            let slot_id = preceding_child_pos
                .slot_id()
                .expect("preceding_child_pos must be AfterSlot");
            parent_node.update_separator_at_slot(slot_id, &new_separator_key)?;

            node.insert_record(redistributed_record.as_slice())?;
            return Ok(());
        }

        // Can't redistribute keys from neither node meaning we must merge (left node is more
        // convenient here).
        let ctx = MergeContext {
            parent_id,
            parent_node,
            child_pos,
            internal_nodes,
            metadata_page,
        };
        self.merge_with_left_sibling(node, node_id, left_sibling, ctx)
    }

    /// Merges with right sibling, moving all keys to the current node and freeing the sibling
    /// page.
    fn merge_with_right_sibling(
        &self,
        mut node: BTreeLeafNode<PinnedWritePage>,
        sibling_id: PageId,
        sibling_node: BTreeLeafNode<PinnedWritePage>,
        mut ctx: MergeContext,
    ) -> Result<(), BTreeError> {
        // Move all keys from right sibling to node.
        let keys_to_move = sibling_node.get_all_records()?;
        for key in keys_to_move {
            node.insert_record(key)?;
        }

        node.set_next_leaf_id(sibling_node.next_leaf_id()?)?;

        let next_child_pos = ctx
            .parent_node
            .get_next_child_pos(ctx.child_pos)?
            .expect("We wouldn't have gone into this function if right sibling didn't exist");

        let slot_id = next_child_pos
            .slot_id()
            .expect("next_child_pos must be AfterSlot");

        ctx.parent_node.delete_at(slot_id)?;

        self.free_page(sibling_id)?;

        if ctx.parent_node.is_underflow()? && !ctx.internal_nodes.is_empty() {
            self.handle_internal_underflow(ctx)?;
        }

        Ok(())
    }

    /// Merges with left sibling, moving all keys to the current node and freeing the sibling
    /// page.
    fn merge_with_left_sibling(
        &self,
        node: BTreeLeafNode<PinnedWritePage>,
        node_id: PageId,
        mut sibling_node: BTreeLeafNode<PinnedWritePage>,
        mut ctx: MergeContext,
    ) -> Result<(), BTreeError> {
        // Move all keys from left sibling to node.
        let keys_to_move = node.get_all_records()?;
        for key in keys_to_move {
            sibling_node.insert_record(&key)?;
        }

        sibling_node.set_next_leaf_id(node.next_leaf_id()?)?;

        let slot_id = ctx
            .child_pos
            .slot_id()
            .expect("child_pos must be AfterSlot for merge_with_left_sibling");
        ctx.parent_node.delete_at(slot_id)?;

        self.free_page(node_id)?;

        if ctx.parent_node.is_underflow()? && !ctx.internal_nodes.is_empty() {
            self.handle_internal_underflow(ctx)?;
        }

        Ok(())
    }

    /// Frees the page at disk level and removes it from structural version numbers map.
    fn free_page(&self, page_id: PageId) -> Result<(), BTreeError> {
        self.structural_version_numbers.remove(&page_id);
        let page_ref = self.page_ref(page_id);
        Ok(self.cache.free_page(&page_ref)?)
    }

    /// Takes care of restructuring an internal node if it has an underflow. Works similarly to
    /// [`redistribute_or_merge`] just for internal nodes.
    fn handle_internal_underflow(&self, mut ctx: MergeContext) -> Result<(), BTreeError> {
        // The node that has an underflow (it was the parent when handling the leaf).
        let underflow_id = ctx.parent_id;
        let mut underflow_node = ctx.parent_node;

        let parent_handle = match ctx.internal_nodes.pop() {
            // underflow_node is the root.
            None => {
                return self.handle_root_underflow(
                    underflow_node,
                    underflow_id,
                    ctx.metadata_page.unwrap(),
                );
            }
            Some(handle) => handle,
        };

        let LatchHandle {
            page_id: parent_id,
            node: mut parent_node,
            child_pos: underflow_child_pos,
        } = parent_handle;

        self.update_structural_version(parent_id);

        // First try to redistribute from right sibling.
        if let Some(right_sibling_pos) = parent_node.get_next_child_pos(underflow_child_pos)? {
            let right_sibling_id = parent_node.get_child_ptr_at(right_sibling_pos)?;
            let mut right_sibling = self.pin_internal_for_write(right_sibling_id)?;

            if right_sibling.can_redistribute_key()? {
                let separator_slot = right_sibling_pos
                    .slot_id()
                    .expect("right_sibling_pos must be AfterSlot");
                return self.redistribute_from_right_internal(
                    &mut underflow_node,
                    &mut right_sibling,
                    &mut parent_node,
                    separator_slot,
                );
            }

            // No left sibling, must merge with right
            if matches!(underflow_child_pos, ChildPosition::Leftmost) {
                let separator_slot = right_sibling_pos
                    .slot_id()
                    .expect("right_sibling_pos must be AfterSlot");
                let new_ctx = MergeContext {
                    parent_id,
                    parent_node,
                    child_pos: underflow_child_pos,
                    internal_nodes: ctx.internal_nodes,
                    metadata_page: ctx.metadata_page,
                };
                return self.merge_internal_with_right_sibling(
                    underflow_node,
                    right_sibling,
                    right_sibling_id,
                    separator_slot,
                    new_ctx,
                );
            }
        }

        let preceding_child_pos = parent_node
            .get_preceding_child_pos(underflow_child_pos)?
            .expect("underflow_child_pos is not Leftmost, so there must be a preceding position");
        let left_sibling_id = parent_node.get_child_ptr_at(preceding_child_pos)?;
        let mut left_sibling = self.pin_internal_for_write(left_sibling_id)?;

        let separator_slot = underflow_child_pos
            .slot_id()
            .expect("underflow_child_pos must be AfterSlot when we have a left sibling");

        if left_sibling.can_redistribute_key()? {
            return self.redistribute_from_left_internal(
                &mut underflow_node,
                &mut left_sibling,
                &mut parent_node,
                separator_slot,
            );
        }

        let new_ctx = MergeContext {
            parent_id,
            parent_node,
            child_pos: underflow_child_pos,
            internal_nodes: ctx.internal_nodes,
            metadata_page: ctx.metadata_page,
        };
        self.merge_internal_with_left_sibling(underflow_node, underflow_id, left_sibling, new_ctx)
    }

    /// Redistributes a key from the right sibling to the underflowing node.
    ///
    /// The separator in the parent comes down to the underflowing node,
    /// and the first key of the right sibling becomes the new separator.
    fn redistribute_from_right_internal(
        &self,
        underflow_node: &mut BTreeInternalNode<PinnedWritePage>,
        right_sibling: &mut BTreeInternalNode<PinnedWritePage>,
        parent_node: &mut BTreeInternalNode<PinnedWritePage>,
        separator_slot: SlotId,
    ) -> Result<(), BTreeError> {
        // Get current separator from parent
        let separator_key = parent_node.get_key(separator_slot)?.to_vec();

        // Get first key and old leftmost from right sibling
        // This removes the first key and updates right's leftmost to be the child ptr from that slot
        let (new_separator_key, old_right_leftmost) = right_sibling.remove_first_key()?;

        // Insert separator into underflow_node with old_right_leftmost as child
        underflow_node.insert(separator_key.as_slice(), old_right_leftmost)?;

        // Update parent's separator to be the new separator key
        parent_node.update_separator_at_slot(separator_slot, &new_separator_key)?;

        Ok(())
    }

    /// Redistributes a key from the left sibling to the underflowing node.
    ///
    /// The separator in the parent comes down to the underflowing node,
    /// and the last key of the left sibling becomes the new separator.
    fn redistribute_from_left_internal(
        &self,
        underflow_node: &mut BTreeInternalNode<PinnedWritePage>,
        left_sibling: &mut BTreeInternalNode<PinnedWritePage>,
        parent_node: &mut BTreeInternalNode<PinnedWritePage>,
        separator_slot: SlotId,
    ) -> Result<(), BTreeError> {
        // Get current separator from parent
        let separator_key = parent_node.get_key(separator_slot)?.to_vec();

        // Get last key and its child from left sibling
        let (new_separator_key, left_last_child) = left_sibling.remove_last_key()?;

        // Insert separator at the beginning of underflow_node with left_last_child as new leftmost
        underflow_node.insert_first_with_new_leftmost(&separator_key, left_last_child)?;

        // Update parent's separator to be the new separator key
        parent_node.update_separator_at_slot(separator_slot, &new_separator_key)?;

        Ok(())
    }

    /// Merges the underflowing node with its right sibling.
    ///
    /// The separator from parent comes down, all keys from right sibling move to underflow_node,
    /// and the right sibling is freed.
    fn merge_internal_with_right_sibling(
        &self,
        mut underflow_node: BTreeInternalNode<PinnedWritePage>,
        right_sibling: BTreeInternalNode<PinnedWritePage>,
        right_sibling_id: PageId,
        separator_slot: SlotId,
        mut ctx: MergeContext,
    ) -> Result<(), BTreeError> {
        let separator_key = ctx.parent_node.get_key(separator_slot)?.to_vec();

        let right_leftmost = right_sibling.get_child_ptr_at(ChildPosition::Leftmost)?;

        underflow_node.insert(&separator_key, right_leftmost)?;

        let right_records = right_sibling.get_all_records()?;
        for record in right_records {
            let key_bytes_end = record.len() - size_of::<PageId>();
            let key = &record[..key_bytes_end];
            let (child_ptr, _) = PageId::deserialize(&record[key_bytes_end..])?;
            underflow_node.insert(key, child_ptr)?;
        }

        ctx.parent_node.delete_at(separator_slot)?;

        self.free_page(right_sibling_id)?;

        if ctx.parent_node.is_underflow()? {
            let new_ctx = MergeContext {
                parent_id: ctx.parent_id,
                parent_node: ctx.parent_node,
                child_pos: ctx.child_pos,
                internal_nodes: ctx.internal_nodes,
                metadata_page: ctx.metadata_page,
            };
            return self.handle_internal_underflow(new_ctx);
        }

        Ok(())
    }

    /// Merges the underflowing node with its left sibling.
    ///
    /// The separator from parent comes down, all keys from underflow_node move to left sibling,
    /// and the underflow_node is freed.
    fn merge_internal_with_left_sibling(
        &self,
        underflow_node: BTreeInternalNode<PinnedWritePage>,
        underflow_id: PageId,
        mut left_sibling: BTreeInternalNode<PinnedWritePage>,
        mut ctx: MergeContext,
    ) -> Result<(), BTreeError> {
        let separator_slot = ctx
            .child_pos
            .slot_id()
            .expect("child_pos must be AfterSlot for merge_with_left_sibling");

        let separator_key = ctx.parent_node.get_key(separator_slot)?.to_vec();

        let underflow_leftmost = underflow_node.get_child_ptr_at(ChildPosition::Leftmost)?;

        left_sibling.insert(&separator_key, underflow_leftmost)?;

        let underflow_records = underflow_node.get_all_records()?;
        for record in underflow_records {
            let key_bytes_end = record.len() - size_of::<PageId>();
            let key = &record[..key_bytes_end];
            let (child_ptr, _) = PageId::deserialize(&record[key_bytes_end..])?;
            left_sibling.insert(key, child_ptr)?;
        }

        // Delete separator from parent
        ctx.parent_node.delete_at(separator_slot)?;

        // Free underflow_node page
        self.free_page(underflow_id)?;

        // Check if parent underflows
        if ctx.parent_node.is_underflow()? {
            let new_ctx = MergeContext {
                parent_id: ctx.parent_id,
                parent_node: ctx.parent_node,
                child_pos: ctx.child_pos,
                internal_nodes: ctx.internal_nodes,
                metadata_page: ctx.metadata_page,
            };
            return self.handle_internal_underflow(new_ctx);
        }

        Ok(())
    }

    /// Handles the underflow of root. If it has one child, it becomes the root.
    fn handle_root_underflow(
        &self,
        root_node: BTreeInternalNode<PinnedWritePage>,
        root_id: PageId,
        mut metadata_page: PinnedWritePage,
    ) -> Result<(), BTreeError> {
        // If root has more than one child we can keep it with the underflow.
        if root_node.num_children()? == 1 {
            let child_id = root_node.get_child_ptr_at(ChildPosition::Leftmost)?;

            let metadata_bytes = &mut metadata_page.page_mut()[0..size_of::<BTreeMetadata>()];
            let metadata =
                bytemuck::try_from_bytes_mut::<BTreeMetadata>(metadata_bytes).map_err(|e| {
                    BTreeError::CorruptMetadata {
                        reason: e.to_string(),
                    }
                })?;

            metadata.root_page_id = child_id;
            self.free_page(root_id)?;
        }

        Ok(())
    }

    /// Extracts the key portion from a leaf record (key + RecordPtr).
    fn extract_key_from_leaf_record(record: &[u8]) -> Vec<u8> {
        let serialized_record_ptr_size = size_of::<PageId>() + size_of::<SlotId>();
        let key_bytes_end = record.len() - serialized_record_ptr_size;
        record[..key_bytes_end].to_vec()
    }
}

struct BTreeFactory {
    file_key: FileKey,
    cache: Arc<Cache>,
}

impl BTreeFactory {
    pub fn new(file_key: FileKey, cache: Arc<Cache>) -> Self {
        BTreeFactory { file_key, cache }
    }

    pub fn create_btree(self) -> Result<BTree, BTreeError> {
        self.initialize_metadata_and_root_if_not_exist()?;

        BTree::new(self.cache, self.file_key)
    }

    fn initialize_metadata_and_root_if_not_exist(&self) -> Result<(), BTreeError> {
        let key = self.file_page_ref(BTree::METADATA_PAGE_ID);
        match self.cache.pin_read(&key) {
            Ok(page) => {
                // Check that it is the correct metadata
                BTreeMetadata::try_from(page.page())?;
                Ok(())
            }
            Err(e) => {
                if let CacheError::PagedFileError(PagedFileError::InvalidPageId(
                    BTree::METADATA_PAGE_ID,
                )) = e
                {
                    // This mean that metadata page was not allocated yet, so the file was just created
                    let (mut metadata_page, metadata_page_id) =
                        self.cache.allocate_page(key.file_key())?;

                    if metadata_page_id != BTree::METADATA_PAGE_ID {
                        return Ok(());
                    }

                    let (root_page, root_page_id) = self.cache.allocate_page(key.file_key())?;

                    BTreeLeafNode::<PinnedWritePage>::initialize(root_page, None)?;

                    let metadata = BTreeMetadata::new(root_page_id);

                    metadata.save_to_page(&mut metadata_page);

                    Ok(())
                } else {
                    Err(e.into())
                }
            }
        }
    }

    /// Creates a `FilePageRef` for `page_id` using b-tree file key.
    fn file_page_ref(&self, page_id: PageId) -> FilePageRef {
        FilePageRef::new(page_id, self.file_key.clone())
    }
}
#[cfg(test)]
mod test {
    use super::*;
    use std::time::{Instant, SystemTime, UNIX_EPOCH};
    use std::{fs, thread};
    use storage::files_manager::FilesManager;
    use tempfile::{TempDir, tempdir};

    /// Creates a test cache and files manager in a temporary directory
    fn setup_test_cache() -> (Arc<Cache>, FileKey, TempDir) {
        let temp_dir = tempdir().unwrap();
        let db_dir = temp_dir.path().join("test_db");
        fs::create_dir_all(&db_dir).unwrap();

        let files_manager = Arc::new(FilesManager::new(db_dir).unwrap());
        let cache = Cache::new(2000, files_manager.clone());

        // Use a unique file name for each test to avoid conflicts
        let file_key = FileKey::index(format!(
            "test_btree_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        (cache, file_key, temp_dir)
    }

    fn create_empty_btree(cache: Arc<Cache>, file_key: FileKey) -> Result<BTree, BTreeError> {
        let (mut metadata_page, metadata_page_id) = cache.allocate_page(&file_key)?;
        assert_eq!(metadata_page_id, BTree::METADATA_PAGE_ID);

        let (root_page, root_id) = cache.allocate_page(&file_key)?;
        BTreeLeafNode::<PinnedWritePage>::initialize(root_page, None)?;

        let metadata = BTreeMetadata::new(root_id);
        let metadata_bytes = bytemuck::bytes_of(&metadata);
        metadata_page.page_mut()[0..metadata_bytes.len()].copy_from_slice(metadata_bytes);

        drop(metadata_page);

        BTree::new(cache, file_key)
    }

    fn create_btree_with_data(
        cache: Arc<Cache>,
        file_key: FileKey,
        data: Vec<(Vec<u8>, RecordPtr)>,
    ) -> Result<BTree, BTreeError> {
        let btree = create_empty_btree(cache, file_key)?;
        for (key, record_ptr) in data {
            btree.insert(&key, record_ptr)?;
        }
        Ok(btree)
    }

    fn record_ptr(page_id: PageId, slot_id: u16) -> RecordPtr {
        RecordPtr::new(page_id, slot_id)
    }

    fn key_i32(value: i32) -> Vec<u8> {
        let mut buf = Vec::new();
        value.serialize(&mut buf);
        buf
    }

    fn key_string(value: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        value.to_string().serialize(&mut buf);
        buf
    }

    fn random_string(len: usize) -> String {
        use rand::{Rng, distr::Alphanumeric};
        rand::rng()
            .sample_iter(&Alphanumeric)
            .take(len)
            .map(char::from)
            .collect()
    }

    #[test]
    fn test_create_empty_btree() {
        let (cache, file_key, _temp_dir) = setup_test_cache();
        let btree = create_empty_btree(cache, file_key);
        assert!(btree.is_ok());
    }

    #[test]
    fn test_search_empty_btree() {
        let (cache, file_key, _temp_dir) = setup_test_cache();
        let btree = create_empty_btree(cache, file_key).unwrap();

        assert_eq!(btree.search(&key_i32(42)).unwrap(), None);
    }

    #[test]
    fn test_insert_and_search_single_key() {
        let (cache, file_key, _temp_dir) = setup_test_cache();
        let btree = create_empty_btree(cache, file_key).unwrap();

        let ptr = record_ptr(10, 5);
        let key = key_i32(42);

        btree.insert(&key, ptr).unwrap();
        assert_eq!(btree.search(&key).unwrap(), Some(ptr));
    }

    #[test]
    fn test_insert_multiple_ordered_keys() {
        let (cache, file_key, _temp_dir) = setup_test_cache();

        let data = vec![
            (key_i32(10), record_ptr(1, 0)),
            (key_i32(20), record_ptr(1, 1)),
            (key_i32(30), record_ptr(1, 2)),
            (key_i32(40), record_ptr(2, 0)),
            (key_i32(50), record_ptr(2, 1)),
        ];

        let btree = create_btree_with_data(cache, file_key, data.clone()).unwrap();

        for (key, expected_ptr) in data {
            assert_eq!(btree.search(&key).unwrap(), Some(expected_ptr));
        }
    }

    #[test]
    fn test_insert_unordered_keys() {
        let (cache, file_key, _temp_dir) = setup_test_cache();

        let data = vec![
            (key_i32(50), record_ptr(5, 0)),
            (key_i32(30), record_ptr(3, 0)),
            (key_i32(70), record_ptr(7, 0)),
            (key_i32(20), record_ptr(2, 0)),
            (key_i32(40), record_ptr(4, 0)),
            (key_i32(60), record_ptr(6, 0)),
            (key_i32(80), record_ptr(8, 0)),
        ];

        let btree = create_btree_with_data(cache, file_key, data.clone()).unwrap();

        for (key, expected_ptr) in data {
            assert_eq!(btree.search(&key).unwrap(), Some(expected_ptr));
        }
    }

    #[test]
    fn test_insert_reverse_order() {
        let (cache, file_key, _temp_dir) = setup_test_cache();
        let btree = create_empty_btree(cache, file_key).unwrap();

        for i in (0..20).rev() {
            btree.insert(&key_i32(i), record_ptr(i as u32, 0)).unwrap();
        }

        for i in 0..20 {
            assert!(btree.search(&key_i32(i)).unwrap().is_some());
        }
    }

    #[test]
    fn test_insert_duplicate_key_returns_error() {
        let (cache, file_key, _temp_dir) = setup_test_cache();
        let btree = create_empty_btree(cache, file_key).unwrap();

        let key = key_i32(42);
        btree.insert(&key, record_ptr(10, 5)).unwrap();

        let result = btree.insert(&key, record_ptr(20, 10));
        assert!(matches!(result, Err(BTreeError::DuplicateKey)));
    }

    #[test]
    fn test_search_nonexistent_keys() {
        let (cache, file_key, _temp_dir) = setup_test_cache();

        let data = vec![
            (key_i32(10), record_ptr(1, 0)),
            (key_i32(20), record_ptr(1, 1)),
            (key_i32(30), record_ptr(1, 2)),
        ];

        let btree = create_btree_with_data(cache, file_key, data).unwrap();

        assert_eq!(btree.search(&key_i32(5)).unwrap(), None);
        assert_eq!(btree.search(&key_i32(15)).unwrap(), None);
        assert_eq!(btree.search(&key_i32(25)).unwrap(), None);
        assert_eq!(btree.search(&key_i32(100)).unwrap(), None);
    }

    #[test]
    fn test_search_boundary_keys() {
        let (cache, file_key, _temp_dir) = setup_test_cache();
        let btree = create_empty_btree(cache, file_key).unwrap();

        let keys = [1, 5, 10, 15, 20];
        for &k in &keys {
            btree.insert(&key_i32(k), record_ptr(k as u32, 0)).unwrap();
        }

        for &k in &keys {
            assert!(btree.search(&key_i32(k)).unwrap().is_some());
        }

        assert_eq!(btree.search(&key_i32(0)).unwrap(), None);
        assert_eq!(btree.search(&key_i32(21)).unwrap(), None);
    }

    #[test]
    fn test_insert_enough_to_trigger_splits() {
        let (cache, file_key, _temp_dir) = setup_test_cache();
        let btree = create_empty_btree(cache, file_key).unwrap();
        let num_keys = 2000;

        for i in 0..num_keys {
            btree
                .insert(&key_i32(i), record_ptr(i as u32, 0))
                .unwrap_or_else(|_| panic!("Failed to insert key {}", i));
        }

        for i in 0..num_keys {
            assert!(
                btree.search(&key_i32(i)).unwrap().is_some(),
                "Key {} not found after splits",
                i
            );
        }
    }

    #[test]
    fn test_insert_random_order_with_splits() {
        use rand::seq::SliceRandom;

        let (cache, file_key, _temp_dir) = setup_test_cache();
        let btree = create_empty_btree(cache, file_key).unwrap();

        let mut keys: Vec<i32> = (0..1000).collect();
        keys.shuffle(&mut rand::rng());

        for &key in &keys {
            btree
                .insert(&key_i32(key), record_ptr(key as u32, 0))
                .unwrap();
        }

        for &key in &keys {
            assert!(btree.search(&key_i32(key)).unwrap().is_some());
        }
    }

    #[test]
    fn test_large_string_keys_trigger_splits() {
        let (cache, file_key, _temp_dir) = setup_test_cache();
        let btree = create_empty_btree(cache, file_key).unwrap();

        let mut keys = Vec::new();
        for i in 0..32 {
            let key = format!("KEY_{}_{}", i, random_string(480));
            keys.push(key.clone());

            btree
                .insert(&key_string(&key), record_ptr(i, 0))
                .unwrap_or_else(|_| panic!("Failed to insert large key {}", i));
        }

        for (i, key) in keys.iter().enumerate() {
            assert_eq!(
                btree.search(&key_string(key)).unwrap(),
                Some(record_ptr(i as u32, 0))
            );
        }
    }

    #[test]
    fn test_mixed_size_keys() {
        let (cache, file_key, _temp_dir) = setup_test_cache();
        let btree = create_empty_btree(cache, file_key).unwrap();

        for i in 0..100 {
            if i % 3 == 0 {
                // Large key
                let key = key_string(&format!("large_key_{}_{}", i, random_string(200)));
                btree.insert(&key, record_ptr(i, 0)).unwrap();
            } else {
                // Small key
                btree.insert(&key_i32(i as i32), record_ptr(i, 0)).unwrap();
            }
        }

        // Verify we can find some of them
        assert!(btree.search(&key_i32(1)).unwrap().is_some());
        assert!(btree.search(&key_i32(2)).unwrap().is_some());
    }

    #[test]
    fn test_sequential_insert_after_splits() {
        let (cache, file_key, _temp_dir) = setup_test_cache();
        let btree = create_empty_btree(cache, file_key).unwrap();

        for i in 0..500 {
            btree.insert(&key_i32(i), record_ptr(i as u32, 0)).unwrap();
        }

        // Insert more keys after splits
        for i in 500..1000 {
            btree.insert(&key_i32(i), record_ptr(i as u32, 0)).unwrap();
        }

        // Verify everything
        for i in 0..1000 {
            assert!(btree.search(&key_i32(i)).unwrap().is_some());
        }
    }

    #[test]
    fn test_concurrent_inserts() {
        let (cache, file_key, _temp_dir) = setup_test_cache();
        let btree = Arc::new(create_empty_btree(cache, file_key).unwrap());

        let num_threads = 32;
        let keys_per_thread = 250;
        let total_keys = num_threads * keys_per_thread;

        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let btree = btree.clone();
                thread::spawn(move || {
                    for i in 0..keys_per_thread {
                        let key_val = thread_id * keys_per_thread + i;
                        btree
                            .insert(&key_i32(key_val), record_ptr(key_val as u32, 0))
                            .unwrap();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let mut missing = vec![];
        for i in 0..total_keys {
            if btree.search(&key_i32(i)).unwrap().is_none() {
                missing.push(i);
            }
        }

        assert!(missing.is_empty(), "Missing keys: {:?}", missing);
    }

    #[test]
    fn test_concurrent_searches_and_inserts() {
        let (cache, file_key, _temp_dir) = setup_test_cache();
        let btree = Arc::new(create_empty_btree(cache, file_key).unwrap());

        let writer = {
            let btree = btree.clone();
            thread::spawn(move || {
                for i in 0..500 {
                    btree.insert(&key_i32(i), record_ptr(i as u32, 0)).unwrap();
                }
            })
        };

        let reader = {
            let btree = btree.clone();
            thread::spawn(move || {
                for _ in 0..200 {
                    let key_val = rand::random::<u16>() as i32 / 2;
                    let _ = btree.search(&key_i32(key_val)).unwrap();
                }
            })
        };

        writer.join().unwrap();
        reader.join().unwrap();
    }

    #[test]
    fn test_multiple_concurrent_readers() {
        let (cache, file_key, _temp_dir) = setup_test_cache();
        let btree = Arc::new(create_empty_btree(cache, file_key).unwrap());

        // Pre-populate with data
        for i in 0..1000 {
            btree.insert(&key_i32(i), record_ptr(i as u32, 0)).unwrap();
        }

        // Spawn multiple reader threads
        let handles: Vec<_> = (0..16)
            .map(|_| {
                let btree = btree.clone();
                thread::spawn(move || {
                    for i in 0..100 {
                        let result = btree.search(&key_i32(i)).unwrap();
                        assert!(result.is_some());
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_empty_key() {
        let (cache, file_key, _temp_dir) = setup_test_cache();
        let btree = create_empty_btree(cache, file_key).unwrap();

        let empty_key = vec![];
        let ptr = record_ptr(1, 0);

        btree.insert(&empty_key, ptr).unwrap();
        assert_eq!(btree.search(&empty_key).unwrap(), Some(ptr));
    }

    #[test]
    fn test_single_byte_keys() {
        let (cache, file_key, _temp_dir) = setup_test_cache();
        let btree = create_empty_btree(cache, file_key).unwrap();

        for byte in 0u8..100 {
            btree.insert(&[byte], record_ptr(byte as u32, 0)).unwrap();
        }

        for byte in 0u8..100 {
            assert!(btree.search(&[byte]).unwrap().is_some());
        }
    }

    #[test]
    fn test_identical_prefix_keys() {
        let (cache, file_key, _temp_dir) = setup_test_cache();
        let btree = create_empty_btree(cache, file_key).unwrap();

        let keys = [
            key_string("prefix_a"),
            key_string("prefix_aa"),
            key_string("prefix_aaa"),
            key_string("prefix_b"),
        ];

        for (i, key) in keys.iter().enumerate() {
            btree.insert(key, record_ptr(i as u32, 0)).unwrap();
        }

        for (i, key) in keys.iter().enumerate() {
            assert_eq!(btree.search(key).unwrap(), Some(record_ptr(i as u32, 0)));
        }
    }

    #[test]
    fn test_alternating_insert_search() {
        let (cache, file_key, _temp_dir) = setup_test_cache();
        let btree = create_empty_btree(cache, file_key).unwrap();

        for i in 0..100 {
            btree.insert(&key_i32(i), record_ptr(i as u32, 0)).unwrap();

            assert_eq!(
                btree.search(&key_i32(i)).unwrap(),
                Some(record_ptr(i as u32, 0))
            );

            if i > 0 {
                assert!(btree.search(&key_i32(i - 1)).unwrap().is_some());
            }
        }
    }

    #[ignore = "TODO: need to figure out better way to handle benchmark tests"]
    #[test]
    fn benchmark_concurrent_inserts_random() {
        use rand::{rng, seq::SliceRandom};

        let num_threads = 1;
        let keys_per_thread = 1000;
        let total_keys = num_threads * keys_per_thread;

        println!("\n=== BTree Random Concurrent Insert Benchmark ===");
        println!(
            "Threads: {}, Keys per thread: {}, Total: {}\n",
            num_threads, keys_per_thread, total_keys
        );

        let mut key_values: Vec<i32> = (0..total_keys as i32).collect();
        key_values.shuffle(&mut rng());
        let key_values = Arc::new(key_values);

        let (cache, file_key, _temp_dir) = setup_test_cache();
        let btree_opt = Arc::new(create_empty_btree(cache, file_key).unwrap());

        let start = Instant::now();
        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let btree = btree_opt.clone();
                let keys = key_values.clone();
                thread::spawn(move || {
                    let start_idx = thread_id * keys_per_thread;
                    let end_idx = start_idx + keys_per_thread;
                    for &key_val in &keys[start_idx..end_idx] {
                        let _ = btree.insert(&key_i32(key_val), record_ptr(key_val as u32, 0));
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
        let optimistic_duration = start.elapsed();

        let (cache2, file_key2, _temp_dir2) = setup_test_cache();
        let btree_pessimistic = Arc::new(create_empty_btree(cache2, file_key2).unwrap());

        let start = Instant::now();
        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let btree = btree_pessimistic.clone();
                let keys = key_values.clone();
                thread::spawn(move || {
                    let start_idx = thread_id * keys_per_thread;
                    let end_idx = start_idx + keys_per_thread;
                    for &key_val in &keys[start_idx..end_idx] {
                        let _ = btree
                            .insert_pessimistic(&key_i32(key_val), record_ptr(key_val as u32, 0));
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
        let pessimistic_duration = start.elapsed();

        println!("Optimistic (with fallback): {:?}", optimistic_duration);
        println!("Pessimistic (direct):        {:?}", pessimistic_duration);
        println!(
            "\nSpeedup: {:.2}x",
            pessimistic_duration.as_secs_f64() / optimistic_duration.as_secs_f64()
        );
    }
}

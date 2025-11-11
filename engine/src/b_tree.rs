use storage::paged_file::{Page, PageId};

use crate::b_tree_node::{
    BTreeInternalNode, BTreeKey, BTreeLeafNode, BTreeNodeError, LeafNodeSearchResult,
    NodeInsertResult, NodeType, get_node_type,
};
use crate::data_types::{DbSerializable, DbSerializationError};
use crate::heap_file::RecordPtr;
use bytemuck::{Pod, Zeroable};
use dashmap::DashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use storage::cache::{Cache, CacheError, FilePageRef, PinnedReadPage, PinnedWritePage};
use storage::files_manager::FileKey;
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

/// Result of an optimistic insert attempt
#[derive(Debug)]
enum OptimisticInsertResult {
    InsertSucceeded,
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
struct PessimisticPath<Key: BTreeKey> {
    latch_stack: Vec<(PageId, BTreeInternalNode<PinnedWritePage, Key>)>,
    leaf_page_id: PageId,
    metadata_page: Option<PinnedWritePage>,
}

impl<Key: BTreeKey> PessimisticPath<Key> {
    fn new(
        latch_stack: Vec<(PageId, BTreeInternalNode<PinnedWritePage, Key>)>,
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

/// Structure responsible for managing on-disk index files.
///
/// Each [`BTree`] instance corresponds to a single physical file on disk.
pub(crate) struct BTree<Key: BTreeKey> {
    _key_marker: PhantomData<Key>,
    file_key: FileKey,
    cache: Arc<Cache>,
    /// A concurrent hash map for storing the current structural version numbers (how many times a
    /// node was split or merged) for each node in the B-Tree. Used for optimistic insert to avoid
    /// inserting into a leaf based on a stale path.
    structural_version_numbers: DashMap<PageId, AtomicU16>,
}

impl<Key: BTreeKey> BTree<Key> {
    const METADATA_PAGE_ID: PageId = 1;

    /// Reads the root page ID from the metadata page.
    fn read_root_page_id(&self) -> Result<PageId, BTreeError> {
        let metadata_page = self.cache.pin_read(&FilePageRef::new(
            BTree::<Key>::METADATA_PAGE_ID,
            self.file_key.clone(),
        ))?;

        let metadata = BTreeMetadata::try_from(metadata_page.page())?;

        Ok(metadata.root_page_id)
    }

    fn new(cache: Arc<Cache>, file_key: FileKey) -> Result<Self, BTreeError> {
        Ok(Self {
            cache,
            file_key,
            _key_marker: PhantomData,
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
    ) -> Result<BTreeLeafNode<PinnedWritePage, Key>, BTreeError> {
        let page = self.pin_write(leaf_page_id)?;
        Ok(BTreeLeafNode::<PinnedWritePage, Key>::new(page)?)
    }

    /// Searches for a key in the B-tree and returns the corresponding record pointer (to heap
    /// file record) if the key was found.
    pub fn search(&self, key: &Key) -> Result<Option<RecordPtr>, BTreeError> {
        let mut current_page_id = self.read_root_page_id()?;

        loop {
            let page = self.pin_read(current_page_id)?;

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
    pub(crate) fn insert(&self, key: Key, record_pointer: RecordPtr) -> Result<(), BTreeError> {
        let optimistic_result = self.insert_optimistic(&key, &record_pointer)?;
        match optimistic_result {
            OptimisticInsertResult::InsertSucceeded => Ok(()),
            OptimisticInsertResult::StructuralChangeRetry => {
                // Retry optimistically before going to pessimistic insert. This makes sense as
                // splits of a node in a given path shouldn't occur too often.
                match self.insert_optimistic(&key, &record_pointer)? {
                    OptimisticInsertResult::InsertSucceeded => Ok(()),
                    _ => self.insert_pessimistic(key, record_pointer),
                }
            }
            OptimisticInsertResult::FullNodeRetry => self.insert_pessimistic(key, record_pointer),
        }
    }

    /// Traverses the tree to the leaf while recording page versions for optimistic concurrency checks.
    fn traverse_with_versions(&self, key: &Key) -> Result<(PageId, Vec<PageVersion>), BTreeError> {
        let mut current_page_id = self.read_root_page_id()?;
        let mut path_versions = Vec::new();

        loop {
            let page = self
                .cache
                .pin_read(&FilePageRef::new(current_page_id, self.file_key.clone()))?;

            // Record page version before descending
            let version = self
                .structural_version_numbers
                .get(&current_page_id)
                .map_or(0, |v| v.load(Ordering::Acquire));
            path_versions.push(PageVersion::new(current_page_id, version));

            match get_node_type(&page)? {
                NodeType::Internal => {
                    let node = BTreeInternalNode::<PinnedReadPage, Key>::new(page)?;
                    current_page_id = node.search(key)?.child_ptr;
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
        key: &Key,
        record_pointer: &RecordPtr,
    ) -> Result<OptimisticInsertResult, BTreeError> {
        // Traverse and collect version info.
        let (leaf_page_id, path_versions) = self.traverse_with_versions(key)?;

        // Try upgrading to a write latch on the leaf.
        let mut leaf_node = self.pin_leaf_for_write(leaf_page_id)?;

        // Check if any node in the path changed structurally.
        if self.detect_structural_changes(&path_versions) {
            return Ok(OptimisticInsertResult::StructuralChangeRetry);
        }

        match leaf_node.insert(key.clone(), *record_pointer)? {
            NodeInsertResult::Success => Ok(OptimisticInsertResult::InsertSucceeded),
            NodeInsertResult::PageFull => Ok(OptimisticInsertResult::FullNodeRetry),
            NodeInsertResult::KeyAlreadyExists => Err(BTreeError::DuplicateKey),
        }
    }

    ///  Traverses the tree while keeping write latches on nodes that may need to split.
    fn traverse_pessimistic(&self, key: &Key) -> Result<PessimisticPath<Key>, BTreeError> {
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
                    let node = BTreeInternalNode::<PinnedWritePage, Key>::new(page)?;
                    let old_page_id = current_page_id;

                    current_page_id = node.search(key)?.child_ptr;

                    // If child can fit another, we can safely drop ancestors and metadata.
                    if node.can_fit_another()? {
                        drop(metadata_page.take());
                        latch_stack.clear();
                    }

                    latch_stack.push((old_page_id, node));
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
    fn insert_pessimistic(&self, key: Key, record_pointer: RecordPtr) -> Result<(), BTreeError> {
        let path = self.traverse_pessimistic(&key)?;

        let mut leaf = self.pin_leaf_for_write(path.leaf_page_id)?;

        match leaf.insert(key.clone(), record_pointer)? {
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
    ) -> Result<(PageId, BTreeLeafNode<PinnedWritePage, Key>), BTreeError> {
        let (new_page, new_leaf_id) = self.cache.allocate_page(&self.file_key)?;
        Ok((
            new_leaf_id,
            BTreeLeafNode::<PinnedWritePage, Key>::initialize(new_page, next_leaf_id)?,
        ))
    }

    /// Allocates a new internal page and initializes it with the given `leftmost_child_id`.
    fn allocate_and_init_internal(
        &self,
        leftmost_child_id: PageId,
    ) -> Result<(PageId, BTreeInternalNode<PinnedWritePage, Key>), BTreeError> {
        let (new_page, new_internal_id) = self.cache.allocate_page(&self.file_key)?;
        Ok((
            new_internal_id,
            BTreeInternalNode::<PinnedWritePage, Key>::initialize(new_page, leftmost_child_id)?,
        ))
    }

    /// Splits a leaf node and propagates the separator up the tree.
    fn split_and_propagate(
        &self,
        internal_nodes: Vec<(PageId, BTreeInternalNode<PinnedWritePage, Key>)>,
        leaf_node: (PageId, BTreeLeafNode<PinnedWritePage, Key>),
        key: Key,
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
        mut leaf_node: BTreeLeafNode<PinnedWritePage, Key>,
        key: Key,
        record_pointer: RecordPtr,
    ) -> Result<(Key, PageId), BTreeError> {
        // Bump version.
        self.update_structural_version(leaf_page_id);

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
        if key < separator_key {
            leaf_node.insert(key, record_pointer)?;
        } else if key > separator_key {
            new_leaf_node.insert(key, record_pointer)?;
        } else {
            return Err(BTreeError::DuplicateKey);
        }

        Ok((separator_key, new_leaf_id))
    }

    /// Propagates a separator key upwards through internal nodes, splitting parents as needed and
    /// creating a new root if necessary.
    fn propagate_separator_up(
        &self,
        mut internal_nodes: Vec<(PageId, BTreeInternalNode<PinnedWritePage, Key>)>,
        mut current_separator_key: Key,
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
        while let Some((parent_page_id, mut parent_node)) = internal_nodes.pop() {
            match parent_node.insert(current_separator_key.clone(), child_page_id)? {
                NodeInsertResult::Success => return Ok(()),
                NodeInsertResult::KeyAlreadyExists => return Err(BTreeError::DuplicateKey),
                NodeInsertResult::PageFull => {
                    // Parent is full,so we must split it and continue upward.

                    // Bump version for parent to indicate structural change.
                    self.update_structural_version(parent_page_id);

                    // Split this internal node: obtain its split_records and new_separator
                    let (split_records, new_separator) = parent_node.split_keys()?;

                    // The first record's child pointer becomes the leftmost child of the new internal node.
                    let (_, leftmost_child_bytes) = Key::deserialize(&split_records[0])?;
                    let (leftmost_child_ptr, _) = PageId::deserialize(leftmost_child_bytes)?;

                    // Allocate and initialize new internal page.
                    let (new_internal_id, mut new_internal_node) =
                        self.allocate_and_init_internal(leftmost_child_ptr)?;

                    // Move the remaining split_records[1..] into the new internal node.
                    // Here we omit the 0th record as in internal nodes it is moved up to the parent
                    // and doesn't remain in the child too like in leaf nodes.
                    new_internal_node.batch_insert(split_records[1..].to_vec())?;

                    // Insert separator key into correct node.
                    if current_separator_key < new_separator {
                        parent_node.insert(current_separator_key, child_page_id)?;
                    } else if current_separator_key > new_separator {
                        new_internal_node.insert(current_separator_key, child_page_id)?;
                    } else {
                        return Err(BTreeError::DuplicateKey);
                    }

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
        separator_key: Key,
        right_child_id: PageId,
        mut metadata_page: PinnedWritePage,
    ) -> Result<(), BTreeError> {
        let (new_root_id, mut new_root) = self.allocate_and_init_internal(left_child_id)?;

        // Insert the separator key with the right child pointer
        new_root.insert(separator_key, right_child_id)?;

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

        let files_manager = Arc::new(FilesManager::new(temp_dir.path(), "test_db").unwrap());
        let cache = Cache::new(200, files_manager.clone());

        // Use a unique file name for each test to avoid conflicts
        let file_key = FileKey::index(format!(
            "test_heap_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        (cache, file_key, temp_dir)
    }

    fn create_empty_btree<Key: BTreeKey>(
        cache: Arc<Cache>,
        file_key: FileKey,
    ) -> Result<BTree<Key>, BTreeError> {
        let (mut metadata_page, metadata_page_id) = cache.allocate_page(&file_key)?;

        assert_eq!(metadata_page_id, BTree::<Key>::METADATA_PAGE_ID);

        let (root_page, root_id) = cache.allocate_page(&file_key)?;

        BTreeLeafNode::<PinnedWritePage, Key>::initialize(root_page, None).unwrap();

        let metadata = BTreeMetadata::new(root_id);
        let metadata_bytes = bytemuck::bytes_of(&metadata);
        metadata_page.page_mut()[0..metadata_bytes.len()].copy_from_slice(metadata_bytes);

        drop(metadata_page);

        BTree::new(cache, file_key)
    }

    fn create_btree_with_data<Key: BTreeKey>(
        cache: Arc<Cache>,
        file_key: FileKey,
        data: Vec<(Key, RecordPtr)>,
    ) -> Result<BTree<Key>, BTreeError> {
        let btree = create_empty_btree(cache, file_key)?;

        for (key, record_ptr) in data {
            btree.insert(key, record_ptr)?;
        }

        Ok(btree)
    }

    #[test]
    fn test_create_empty_btree() {
        let (cache, file_key, _temp_dir) = setup_test_cache();

        let btree = create_empty_btree::<i32>(cache, file_key);
        assert!(btree.is_ok());
    }

    fn test_record_pointer(page_id: PageId, slot_id: u16) -> RecordPtr {
        RecordPtr::new(page_id, slot_id)
    }
    #[test]
    fn test_search_empty_btree() {
        let (cache, file_key, _temp_dir) = setup_test_cache();
        let btree = create_empty_btree::<i32>(cache, file_key).unwrap();

        let result = btree.search(&42);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_insert_single_key() {
        let (cache, file_key, _temp_dir) = setup_test_cache();
        let btree = create_empty_btree::<i32>(cache, file_key).unwrap();

        let record_ptr = test_record_pointer(10, 5);
        let result = btree.insert(42, record_ptr);
        assert!(result.is_ok());

        let search_result = btree.search(&42).unwrap();
        assert_eq!(search_result, Some(record_ptr));
    }

    #[test]
    fn test_insert_multiple_keys() {
        let (cache, file_key, _temp_dir) = setup_test_cache();

        let data = vec![
            (10, test_record_pointer(1, 0)),
            (20, test_record_pointer(1, 1)),
            (30, test_record_pointer(1, 2)),
            (40, test_record_pointer(2, 0)),
            (50, test_record_pointer(2, 1)),
        ];

        let btree = create_btree_with_data(cache, file_key, data.clone()).unwrap();

        for (key, expected_ptr) in data {
            let result = btree.search(&key).unwrap();
            assert_eq!(result, Some(expected_ptr));
        }
    }

    #[test]
    fn test_insert_duplicate_key_unique_btree() {
        let (cache, file_key, _temp_dir) = setup_test_cache();
        let btree = create_empty_btree::<i32>(cache, file_key).unwrap();

        let record_ptr1 = test_record_pointer(10, 5);
        let record_ptr2 = test_record_pointer(20, 10);

        btree.insert(42, record_ptr1).unwrap();
        let result = btree.insert(42, record_ptr2);

        assert!(matches!(result, Err(BTreeError::DuplicateKey)));
    }

    #[test]
    fn test_search_nonexistent_key() {
        let (cache, file_key, _temp_dir) = setup_test_cache();

        let data = vec![
            (10, test_record_pointer(1, 0)),
            (20, test_record_pointer(1, 1)),
            (30, test_record_pointer(1, 2)),
        ];

        let btree = create_btree_with_data(cache, file_key, data).unwrap();

        assert_eq!(btree.search(&5).unwrap(), None);
        assert_eq!(btree.search(&15).unwrap(), None);
        assert_eq!(btree.search(&100).unwrap(), None);
    }

    #[test]
    fn test_insert_unordered_keys() {
        let (cache, file_key, _temp_dir) = setup_test_cache();

        let data = vec![
            (50, test_record_pointer(5, 0)),
            (30, test_record_pointer(3, 0)),
            (70, test_record_pointer(7, 0)),
            (20, test_record_pointer(2, 0)),
            (40, test_record_pointer(4, 0)),
            (60, test_record_pointer(6, 0)),
            (80, test_record_pointer(8, 0)),
        ];

        let btree = create_btree_with_data(cache, file_key, data.clone()).unwrap();

        for (key, expected_ptr) in data {
            let result = btree.search(&key).unwrap();
            assert_eq!(result, Some(expected_ptr));
        }
    }

    #[test]
    fn test_insert_enough_to_split_test() {
        let (cache, file_key, _temp_dir) = setup_test_cache();
        let btree = create_empty_btree::<i32>(cache.clone(), file_key.clone()).unwrap();
        let num_keys = 2000;

        for i in 0..num_keys {
            let key = i;

            let result = btree.insert(
                key,
                test_record_pointer(key as PageId, (key % 65536) as u16),
            );

            assert!(result.is_ok(), "Failed to insert key {}: {:?}", key, result);
        }

        for i in 0..num_keys {
            let result = btree.search(&i);
            assert!(result.is_ok(), "Search failed for key {}: {:?}", i, result);
            assert!(result.unwrap().is_some(), "Key {} not found", i);
        }
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
    fn test_split_trigger_with_large_string_keys() {
        let (cache, file_key, _temp_dir) = setup_test_cache();
        let btree = create_empty_btree::<String>(cache, file_key).unwrap();

        for i in 0..32 {
            let key = format!("KEY_{}_{}", i, random_string(480));
            let ptr = test_record_pointer(i as PageId, 0);
            let result = btree.insert(key.clone(), ptr);
            assert!(result.is_ok(), "Failed insert {}", i);

            let found = btree.search(&key).unwrap();
            assert_eq!(found, Some(ptr));
        }
    }

    #[test]
    fn test_concurrent_inserts_arc_btree() {
        let (cache, file_key, _temp_dir) = setup_test_cache();
        let btree = Arc::new(create_empty_btree::<i32>(cache, file_key).unwrap());

        let num_threads = 32;
        let keys_per_thread = 250;

        let t = Instant::now();

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let btree = btree.clone();
                thread::spawn(move || {
                    for i in 0..keys_per_thread {
                        let key = t * keys_per_thread + i;
                        btree
                            .insert(key, test_record_pointer(key as u32, 0))
                            .unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let time = t.elapsed();

        println!("{}", time.as_secs_f32());
        let mut v = vec![];
        for i in 0..num_threads * keys_per_thread {
            let result = btree.search(&i).unwrap();
            if result.is_none() {
                v.push(i);
            }
        }
        for i in &v {
            println!("{}", i);
        }
        assert_eq!(v.len(), 0);
    }

    #[test]
    fn test_concurrent_searches_during_inserts() {
        use std::sync::Arc;
        use std::thread;

        let (cache, file_key, _temp_dir) = setup_test_cache();
        let btree = Arc::new(create_empty_btree::<i32>(cache, file_key).unwrap());

        let writer = {
            let btree = btree.clone();
            thread::spawn(move || {
                for i in 0..500 {
                    btree.insert(i, test_record_pointer(i as u32, 0)).unwrap();
                }
            })
        };

        let reader = {
            let btree = btree.clone();
            thread::spawn(move || {
                for _ in 0..200 {
                    let _ = btree.search(&(rand::random::<u16>() as i32 / 2)).unwrap();
                }
            })
        };

        writer.join().unwrap();
        reader.join().unwrap();
    }

    #[test]
    fn benchmark_concurrent_inserts_random() {
        use rand::{rng, seq::SliceRandom};
        use std::sync::Arc;
        use std::thread;

        let num_threads = 16;
        let keys_per_thread = 100;
        let total_keys = num_threads * keys_per_thread;

        println!("\n=== BTree Random Concurrent Insert Benchmark ===");
        println!(
            "Threads: {}, Keys per thread: {}, Total: {}\n",
            num_threads, keys_per_thread, total_keys
        );

        let mut keys: Vec<u32> = (0..total_keys as u32).collect();
        keys.shuffle(&mut rng());
        let keys = Arc::new(keys);

        // Optimistic first before pessimistic
        let (cache, file_key, _temp_dir) = setup_test_cache();
        let btree_opt = Arc::new(create_empty_btree::<i32>(cache, file_key).unwrap());

        let start = Instant::now();
        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let btree = btree_opt.clone();
                let keys = keys.clone();
                thread::spawn(move || {
                    let start_idx = t * keys_per_thread;
                    let end_idx = start_idx + keys_per_thread;
                    for &key in &keys[start_idx..end_idx] {
                        match btree.insert(key as i32, test_record_pointer(key, 0)) {
                            Ok(_) => {}
                            Err(e) => {
                                println!("{}", e);
                            }
                        }
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
        let optimistic_duration = start.elapsed();

        // Only pessimistic
        let (cache2, file_key2, _temp_dir2) = setup_test_cache();
        let btree_pessimistic = Arc::new(create_empty_btree::<i32>(cache2, file_key2).unwrap());

        let start = Instant::now();
        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let btree = btree_pessimistic.clone();
                let keys = keys.clone();
                thread::spawn(move || {
                    let start_idx = t * keys_per_thread;
                    let end_idx = start_idx + keys_per_thread;
                    for &key in &keys[start_idx..end_idx] {
                        match btree.insert_pessimistic(key as i32, test_record_pointer(key, 0)) {
                            Ok(_) => {}
                            Err(e) => {
                                println!("{}", e);
                            }
                        };
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

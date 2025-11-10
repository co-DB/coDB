use crate::b_tree_node::{
    BTreeInternalNode, BTreeKey, BTreeLeafNode, BTreeNodeError, LeafNodeSearchResult,
    NodeInsertResult, NodeType, get_node_type,
};
use crate::cache::{Cache, CacheError, FilePageRef, PinnedReadPage, PinnedWritePage};
use crate::data_types::{DbSerializable, DbSerializationError};
use crate::files_manager::FileKey;
use crate::heap_file::RecordPtr;
use crate::paged_file::{Page, PageId};
use bytemuck::{Pod, Zeroable};
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

#[derive(Pod, Zeroable, Copy, Clone, Debug)]
#[repr(C)]
struct BTreeMetadata {
    magic_number: [u8; 4],
    root_page_id: PageId,
}

impl BTreeMetadata {
    const CODB_MAGIC_NUMBER: [u8; 4] = [0xC, 0x0, 0xD, 0xB];

    const SIZE: usize = size_of::<BTreeMetadata>();

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

#[derive(Debug)]
enum OptimisticInsertResult {
    InsertSucceeded,
    StructuralChangeRetry,
    FullNodeRetry,
}

#[derive(Clone)]
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

    pub fn search(&self, key: &Key) -> Result<Option<RecordPtr>, BTreeError> {
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
    pub(crate) fn insert(&self, key: Key, record_pointer: RecordPtr) -> Result<(), BTreeError> {
        let optimistic_result = self.insert_optimistic(&key, &record_pointer)?;
        match optimistic_result {
            OptimisticInsertResult::InsertSucceeded => Ok(()),
            OptimisticInsertResult::StructuralChangeRetry => {
                let retry_result = self.insert_optimistic(&key, &record_pointer)?;
                if matches!(retry_result, OptimisticInsertResult::InsertSucceeded) {
                    return Ok(());
                }
                Ok(self.insert_pessimistic(key, record_pointer)?)
            }
            OptimisticInsertResult::FullNodeRetry => self.insert_pessimistic(key, record_pointer),
        }
    }

    fn insert_optimistic(
        &self,
        key: &Key,
        record_pointer: &RecordPtr,
    ) -> Result<OptimisticInsertResult, BTreeError> {
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

                    return match node.insert(key.clone(), *record_pointer)? {
                        NodeInsertResult::Success => Ok(OptimisticInsertResult::InsertSucceeded),
                        NodeInsertResult::PageFull => Ok(OptimisticInsertResult::FullNodeRetry),
                        NodeInsertResult::KeyAlreadyExists => Err(BTreeError::DuplicateKey),
                    };
                }
            }
        }
    }

    fn insert_pessimistic(&self, key: Key, record_pointer: RecordPtr) -> Result<(), BTreeError> {
        // let mut metadata_page = Some(self.cache.pin_write(&FilePageRef::new(
        //     BTree::<Key>::METADATA_PAGE_ID,
        //     self.file_key.clone(),
        // ))?);
        //
        // let mut current_page_id =
        //     BTreeMetadata::try_from(metadata_page.as_ref().unwrap().page())?.root_page_id;

        let mut current_page_id = self.read_root_page_id()?;

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
                        // We drop the metadata page since we now know for sure that root page will
                        // stay the same.
                        // We can always drop it since if it was dropped before it will just
                        // drop(None) which is a no-op.
                        // drop(metadata_page.take());
                        latch_stack.clear();
                    }

                    latch_stack.push(node);
                }
                NodeType::Leaf => {
                    let mut node = BTreeLeafNode::<PinnedWritePage, Key>::new(page)?;

                    return match node.insert(key.clone(), record_pointer)? {
                        NodeInsertResult::Success => Ok(()),
                        NodeInsertResult::PageFull => self.split_and_propagate(
                            latch_stack,
                            node,
                            key.clone(),
                            record_pointer,
                            //metadata_page.take(),
                        ),
                        NodeInsertResult::KeyAlreadyExists => Err(BTreeError::DuplicateKey),
                    };
                }
            }
        }
    }

    fn split_and_propagate(
        &self,
        mut internal_nodes: Vec<BTreeInternalNode<PinnedWritePage, Key>>,
        mut leaf_node: BTreeLeafNode<PinnedWritePage, Key>,
        key: Key,
        record_pointer: RecordPtr,
        //metadata_page: Option<PinnedWritePage>,
    ) -> Result<(), BTreeError> {
        leaf_node.update_version_number()?;

        // First we get half of the keys along with the separator key from leaf node.
        let (records, separator_key) = leaf_node.split_keys()?;

        // We must get the next leaf's page id from the leaf node being split.
        let next_leaf_id = leaf_node.next_leaf_id()?;

        // Then we create a new leaf node that will take those keys.
        let (new_page, new_leaf_id) = self.cache.allocate_page(&self.file_key)?;
        let mut new_leaf_node =
            BTreeLeafNode::<PinnedWritePage, Key>::initialize(new_page, next_leaf_id)?;

        // Insert the keys into the newly created leaf.
        new_leaf_node.batch_insert(records)?;

        // Make the old leaf point to the new one
        leaf_node.set_next_leaf_id(Some(new_leaf_id))?;

        if key < separator_key {
            leaf_node.insert(key, record_pointer)?;
        } else if key > separator_key {
            new_leaf_node.insert(key, record_pointer)?;
        } else {
            return Err(BTreeError::DuplicateKey);
        }

        // We have the leaf nodes set up correctly, so we must now propagate the separator key
        // along with the new leaf node page id up to the parent internal node.

        // This means we are in root.
        if internal_nodes.is_empty() {
            // We can unwrap here because as the metadata is guaranteed to be non-released here.
            let metadata_page = self.cache.pin_write(&FilePageRef::new(
                BTree::<Key>::METADATA_PAGE_ID,
                self.file_key.clone(),
            ))?;

            let root_page_id = BTreeMetadata::try_from(metadata_page.page())?.root_page_id;
            return self.create_new_root(root_page_id, separator_key, new_leaf_id, metadata_page);
        } else {
            // This starts as being the key we copied from leaf node and the page id of the new leaf.
            // Now we want to insert the pair into the parent internal node.
            let mut current_separator_key = separator_key.clone();
            let mut child_page_id = new_leaf_id;

            while let Some(mut internal_node) = internal_nodes.pop() {
                match internal_node.insert(current_separator_key.clone(), child_page_id)? {
                    NodeInsertResult::KeyAlreadyExists => return Err(BTreeError::DuplicateKey),
                    NodeInsertResult::Success => return Ok(()),
                    NodeInsertResult::PageFull => {
                        internal_node.update_version_number()?;
                        // Internal node is full so we need to split it too. New separator here is
                        // the key from the internal node we want to insert upwards.
                        let (split_records, new_separator) = internal_node.split_keys()?;

                        let (new_internal_page, new_internal_id) =
                            self.cache.allocate_page(&self.file_key)?;

                        // The first record in split_records contains the leftmost child
                        // pointer for the new internal node. This is because the key of this record
                        // is moved to the parent node and thus its pointer (which points to a page
                        // with keys >= record[0].key && < record[1].key) now fulfills the criteria
                        // to be the leftmost child pointer.
                        let (_, child_ptr_bytes) = Key::deserialize(&split_records[0])?;
                        let (child_ptr, _) = PageId::deserialize(child_ptr_bytes)?;

                        let mut new_internal_node =
                            BTreeInternalNode::<PinnedWritePage, Key>::initialize(
                                new_internal_page,
                                child_ptr,
                            )?;

                        // We don't insert the first key as it is moved up to parent
                        new_internal_node.batch_insert(split_records[1..].to_vec())?;

                        if current_separator_key < new_separator {
                            internal_node.insert(current_separator_key, child_page_id)?;
                        } else if current_separator_key > new_separator {
                            new_internal_node.insert(current_separator_key, child_page_id)?;
                        } else {
                            return Err(BTreeError::DuplicateKey);
                        }

                        // The cycle repeats with the new separator and child page id.
                        current_separator_key = new_separator;
                        child_page_id = new_internal_id;

                        // If this was the last internal node (the root), create new root.
                        if internal_nodes.is_empty() {
                            // We can unwrap here, because the metadata is guaranteed to be non-released here.
                            // let root_page_id =
                            //     BTreeMetadata::try_from(metadata_page.as_ref().unwrap().page())?
                            //         .root_page_id;
                            //
                            // return self.create_new_root(
                            //     root_page_id,
                            //     current_separator_key,
                            //     child_page_id,
                            //     metadata_page.unwrap(),
                            // );
                            // We can unwrap here because as the metadata is guaranteed to be non-released here.
                            let metadata_page = self.cache.pin_write(&FilePageRef::new(
                                BTree::<Key>::METADATA_PAGE_ID,
                                self.file_key.clone(),
                            ))?;

                            let root_page_id =
                                BTreeMetadata::try_from(metadata_page.page())?.root_page_id;
                            return self.create_new_root(
                                root_page_id,
                                current_separator_key,
                                child_page_id,
                                metadata_page,
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
        &self,
        left_child_id: PageId,
        separator_key: Key,
        right_child_id: PageId,
        mut metadata_page: PinnedWritePage,
    ) -> Result<(), BTreeError> {
        let (new_root_page, new_root_id) = self.cache.allocate_page(&self.file_key)?;

        // Set the leftmost child to point to the left child (old root or left sibling)
        let mut new_root =
            BTreeInternalNode::<PinnedWritePage, Key>::initialize(new_root_page, left_child_id)?;

        // Insert the separator key with the right child pointer
        new_root.insert(separator_key, right_child_id)?;

        let metadata = match bytemuck::try_from_bytes_mut::<BTreeMetadata>(
            &mut metadata_page.page_mut()[0..size_of::<BTreeMetadata>()],
        ) {
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::files_manager::FilesManager;
    use std::time::{SystemTime, UNIX_EPOCH};
    use std::{fs, thread};
    use tempfile::{TempDir, tempdir};

    /// Creates a test cache and files manager in a temporary directory
    fn setup_test_cache() -> (Arc<Cache>, FileKey, TempDir) {
        let temp_dir = tempdir().unwrap();
        let db_dir = temp_dir.path().join("test_db");
        fs::create_dir_all(&db_dir).unwrap();

        let files_manager = Arc::new(FilesManager::new(temp_dir.path(), "test_db").unwrap());
        let cache = Cache::new(100, files_manager.clone());

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
        is_unique: bool,
    ) -> Result<BTree<Key>, BTreeError> {
        let (mut metadata_page, metadata_page_id) = cache.allocate_page(&file_key)?;

        assert_eq!(metadata_page_id, BTree::<Key>::METADATA_PAGE_ID);

        let (root_page, root_id) = cache.allocate_page(&file_key)?;

        BTreeLeafNode::<PinnedWritePage, Key>::initialize(root_page, None).unwrap();

        let metadata = BTreeMetadata::new(root_id);
        let metadata_bytes = bytemuck::bytes_of(&metadata);
        metadata_page.page_mut()[0..metadata_bytes.len()].copy_from_slice(metadata_bytes);

        drop(metadata_page);

        Ok(BTree::new(cache, file_key, is_unique))
    }

    fn create_btree_with_data<Key: BTreeKey>(
        cache: Arc<Cache>,
        file_key: FileKey,
        is_unique: bool,
        data: Vec<(Key, RecordPtr)>,
    ) -> Result<BTree<Key>, BTreeError> {
        let mut btree = create_empty_btree(cache, file_key, is_unique)?;

        for (key, record_ptr) in data {
            btree.insert(key, record_ptr)?;
        }

        Ok(btree)
    }

    #[test]
    fn test_create_empty_btree() {
        let (cache, file_key, _temp_dir) = setup_test_cache();

        let btree = create_empty_btree::<i32>(cache, file_key, true);
        assert!(btree.is_ok());
    }

    fn test_record_pointer(page_id: PageId, slot_id: u16) -> RecordPtr {
        RecordPtr::new(page_id, slot_id)
    }
    #[test]
    fn test_search_empty_btree() {
        let (cache, file_key, _temp_dir) = setup_test_cache();
        let btree = create_empty_btree::<i32>(cache, file_key, true).unwrap();

        let result = btree.search(&42);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_insert_single_key() {
        let (cache, file_key, _temp_dir) = setup_test_cache();
        let mut btree = create_empty_btree::<i32>(cache, file_key, true).unwrap();

        let record_ptr = test_record_pointer(10, 5);
        let result = btree.insert(42, record_ptr.clone());
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

        let btree = create_btree_with_data(cache, file_key, true, data.clone()).unwrap();

        for (key, expected_ptr) in data {
            let result = btree.search(&key).unwrap();
            assert_eq!(result, Some(expected_ptr));
        }
    }

    #[test]
    fn test_insert_duplicate_key_unique_btree() {
        let (cache, file_key, _temp_dir) = setup_test_cache();
        let mut btree = create_empty_btree::<i32>(cache, file_key, true).unwrap();

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

        let btree = create_btree_with_data(cache, file_key, true, data).unwrap();

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

        let btree = create_btree_with_data(cache, file_key, true, data.clone()).unwrap();

        for (key, expected_ptr) in data {
            let result = btree.search(&key).unwrap();
            assert_eq!(result, Some(expected_ptr));
        }
    }

    #[test]
    fn test_insert_enough_to_split_test() {
        let (cache, file_key, _temp_dir) = setup_test_cache();
        let mut btree = create_empty_btree::<i32>(cache.clone(), file_key.clone(), true).unwrap();
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
        let mut btree = create_empty_btree::<String>(cache, file_key, true).unwrap();

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
        let btree = Arc::new(create_empty_btree::<i32>(cache, file_key, true).unwrap());

        let num_threads = 8;
        let keys_per_thread = 250;

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let btree = btree.clone();
                thread::spawn(move || {
                    for i in 0..keys_per_thread {
                        let key = t * keys_per_thread + i;
                        //println!("Thread {} inserting key {}", t, key);
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
}

use std::collections::HashMap;

use storage::{
    cache::{PageRead, PinnedReadPage, PinnedWritePage},
    paged_file::PageId,
};

use crate::heap_file::{
    HeapFile,
    error::HeapFileError,
    pages::{BaseHeapPageHeader, HeapPage, OverflowPageHeader, RecordPageHeader},
};

/// Trait encapsulating logic behind locking pages with shared lock.
///
/// When dealing with record that spans multiple pages (multi-fragment record) pages shouldn't be read by hand,
/// but instead struct that implements this trait should be used.
pub(super) trait ReadPageLockChain<P>
where
    P: PageRead,
{
    /// Locks the next page
    fn advance(&mut self, next_page_id: PageId) -> Result<(), HeapFileError>;
    /// Gets current record page (undefined when only overflow page is held at the time).
    fn record_page(&self) -> &HeapPage<P, RecordPageHeader>;
    /// Gets current overflow page (undefined when only record page is held at the time).
    fn overflow_page(&self) -> &HeapPage<P, OverflowPageHeader>;
}

/// Trait encapsulating logic behind locking pages with exclusive lock.
///
/// When dealing with mutable record that spans multiple pages (multi-fragment record) pages shouldn't be read by hand,
/// but instead struct that implements this trait should be used.
pub(super) trait WritePageLockChain {
    /// Locks the next page
    fn advance(&mut self, next_page_id: PageId) -> Result<(), HeapFileError>;
    /// Gets mutable current record page (undefined when only overflow page is held at the time).
    fn record_page_mut(&mut self) -> &mut HeapPage<PinnedWritePage, RecordPageHeader>;
    /// Gets mutable current overflow page (undefined when only record page is held at the time).
    fn overflow_page_mut(&mut self) -> &mut HeapPage<PinnedWritePage, OverflowPageHeader>;
    /// Add pages directly to the collected pages vec
    fn add_pages(&mut self, pages: Vec<PinnedWritePage>);
}

/// Trait for collecting pages during operations that may allocate new pages.
pub(super) trait PageCollector {
    /// Adds a newly allocated or modified page to the collection
    fn consume_heap_page<H: BaseHeapPageHeader>(&mut self, heap_page: HeapPage<PinnedWritePage, H>);
}

/// Simple wrapper around Vec<PinnedWritePage> that implements PageCollector
pub(super) struct VecPageCollector<'a> {
    pages: &'a mut Vec<PinnedWritePage>,
}

impl<'a> VecPageCollector<'a> {
    pub fn new(pages: &'a mut Vec<PinnedWritePage>) -> Self {
        VecPageCollector { pages }
    }
}

impl<'a> PageCollector for VecPageCollector<'a> {
    fn consume_heap_page<H: BaseHeapPageHeader>(
        &mut self,
        heap_page: HeapPage<PinnedWritePage, H>,
    ) {
        self.pages.push(heap_page.page.into_page());
    }
}

/// Trait encapsulating logic behind sending locked pages as single MultiPageOperation to WAL.
pub(super) trait MultiPageOperationChain {
    fn drop_pages(self);
}

/// Helper struct used for locking pages when reading records.
///
/// Locking pages with this struct works as follows:
/// - read page
/// - read the record fragment
/// - read next page
/// - drop the previous page
pub(super) struct SharedPageLockChain<'hf, const BUCKETS_COUNT: usize> {
    heap_file: &'hf HeapFile<BUCKETS_COUNT>,
    record_page: Option<HeapPage<PinnedReadPage, RecordPageHeader>>,
    overflow_page: Option<HeapPage<PinnedReadPage, OverflowPageHeader>>,
}

impl<'hf, const BUCKETS_COUNT: usize> SharedPageLockChain<'hf, BUCKETS_COUNT> {
    /// Creates new [`SharedPageLockChain`] that starts with record page with `page_id`
    pub fn with_page_id(
        heap_file: &'hf HeapFile<BUCKETS_COUNT>,
        page_id: PageId,
    ) -> Result<Self, HeapFileError> {
        let record_page = heap_file.read_record_page(page_id)?;
        Ok(SharedPageLockChain {
            heap_file,
            record_page: Some(record_page),
            overflow_page: None,
        })
    }
}

impl<'hf, const BUCKETS_COUNT: usize> ReadPageLockChain<PinnedReadPage>
    for SharedPageLockChain<'hf, BUCKETS_COUNT>
{
    /// Locks the next page and only then drops the previous one
    fn advance(&mut self, next_page_id: PageId) -> Result<(), HeapFileError> {
        // Read next page
        let new_page = self.heap_file.read_overflow_page(next_page_id)?;

        // Drop previous page
        self.record_page = None;
        self.overflow_page = Some(new_page);
        Ok(())
    }

    fn record_page(&self) -> &HeapPage<PinnedReadPage, RecordPageHeader> {
        match &self.record_page {
            Some(record_page) => record_page,
            None => panic!(
                "invalid usage of SharedPageLockChain - tried to get record page when overflow page is stored instead"
            ),
        }
    }

    fn overflow_page(&self) -> &HeapPage<PinnedReadPage, OverflowPageHeader> {
        match &self.overflow_page {
            Some(overflow_page) => overflow_page,
            None => panic!(
                "invalid usage of SharedPageLockChain - tried to get overflow page when record page is stored instead"
            ),
        }
    }
}

/// Works the same as [`SharedPageLockChain`], but must be constructed with already read record page.
pub(super) struct SharedPageLockChainWithRecordPage<'hf, 'rp, const BUCKETS_COUNT: usize> {
    heap_file: &'hf HeapFile<BUCKETS_COUNT>,
    record_page: &'rp HeapPage<PinnedReadPage, RecordPageHeader>,
    overflow_page: Option<HeapPage<PinnedReadPage, OverflowPageHeader>>,
}

impl<'hf, 'rp, const BUCKETS_COUNT: usize>
    SharedPageLockChainWithRecordPage<'hf, 'rp, BUCKETS_COUNT>
{
    /// Creates new [`SharedPageLockChainWithRecordPage`] that starts with record page
    pub fn with_record(
        heap_file: &'hf HeapFile<BUCKETS_COUNT>,
        record_page: &'rp HeapPage<PinnedReadPage, RecordPageHeader>,
    ) -> Result<Self, HeapFileError> {
        Ok(SharedPageLockChainWithRecordPage {
            heap_file,
            record_page,
            overflow_page: None,
        })
    }
}

impl<'hf, 'rp, const BUCKETS_COUNT: usize> ReadPageLockChain<PinnedReadPage>
    for SharedPageLockChainWithRecordPage<'hf, 'rp, BUCKETS_COUNT>
{
    fn advance(&mut self, next_page_id: PageId) -> Result<(), HeapFileError> {
        let new_page = self.heap_file.read_overflow_page(next_page_id)?;
        self.overflow_page = Some(new_page);
        Ok(())
    }

    fn record_page(&self) -> &HeapPage<PinnedReadPage, RecordPageHeader> {
        self.record_page
    }

    fn overflow_page(&self) -> &HeapPage<PinnedReadPage, OverflowPageHeader> {
        match &self.overflow_page {
            Some(overflow_page) => overflow_page,
            None => panic!(
                "invalid usage of SharedPageLockChainWithRecordPage - tried to get overflow page when record page is stored instead"
            ),
        }
    }
}

/// Helper struct used for locking pages when modifying records.
///
/// Locking pages with this struct works as follows:
/// - read page
/// - modify record
/// - read next page
///
/// It locks all pages until the operation is done.
pub(super) struct ExclusivePageLockChain<'hf, const BUCKETS_COUNT: usize> {
    heap_file: &'hf HeapFile<BUCKETS_COUNT>,
    record_page: Option<HeapPage<PinnedWritePage, RecordPageHeader>>,
    overflow_page: Option<HeapPage<PinnedWritePage, OverflowPageHeader>>,
    pages: Vec<PinnedWritePage>,
}

impl<'hf, const BUCKETS_COUNT: usize> ExclusivePageLockChain<'hf, BUCKETS_COUNT> {
    /// Creates new [`ExclusivePageLockChain`] that starts with record page with `page_id`
    pub fn with_page_id(
        heap_file: &'hf HeapFile<BUCKETS_COUNT>,
        page_id: PageId,
    ) -> Result<Self, HeapFileError> {
        let record_page = heap_file.write_record_page(page_id)?;
        Ok(ExclusivePageLockChain {
            heap_file,
            record_page: Some(record_page),
            overflow_page: None,
            pages: vec![],
        })
    }
}

impl<'hf, const BUCKETS_COUNT: usize> WritePageLockChain
    for ExclusivePageLockChain<'hf, BUCKETS_COUNT>
{
    fn advance(&mut self, next_page_id: PageId) -> Result<(), HeapFileError> {
        let new_page = self.heap_file.write_overflow_page(next_page_id)?;
        if let Some(r) = self.record_page.take() {
            let p = r.page.into_page();
            self.pages.push(p);
        };
        if let Some(o) = self.overflow_page.take() {
            let p = o.page.into_page();
            self.pages.push(p);
        };
        self.overflow_page = Some(new_page);
        Ok(())
    }

    fn record_page_mut(&mut self) -> &mut HeapPage<PinnedWritePage, RecordPageHeader> {
        match &mut self.record_page {
            Some(record_page) => record_page,
            None => panic!(
                "invalid usage of ExclusivePageLockChain - tried to get record page when overflow page is stored instead"
            ),
        }
    }

    fn overflow_page_mut(&mut self) -> &mut HeapPage<PinnedWritePage, OverflowPageHeader> {
        match &mut self.overflow_page {
            Some(overflow_page) => overflow_page,
            None => panic!(
                "invalid usage of ExclusivePageLockChain - tried to get overflow page when record page is stored instead"
            ),
        }
    }

    fn add_pages(&mut self, pages: Vec<PinnedWritePage>) {
        self.pages.extend(pages);
    }
}

impl<'hf, const BUCKETS_COUNT: usize> PageCollector for ExclusivePageLockChain<'hf, BUCKETS_COUNT> {
    fn consume_heap_page<H: BaseHeapPageHeader>(
        &mut self,
        heap_page: HeapPage<PinnedWritePage, H>,
    ) {
        self.pages.push(heap_page.page.into_page());
    }
}

impl<'hf, const BUCKETS_COUNT: usize> MultiPageOperationChain
    for ExclusivePageLockChain<'hf, BUCKETS_COUNT>
{
    fn drop_pages(self) {
        self.heap_file.cache.drop_write_pages(self.pages);
    }
}

/// Helper struct used for collecting all pages during insert operations.
///
/// Unlike ExclusivePageLockChain, this struct is designed for insert operations where:
/// - We don't need to advance through a chain of existing pages
/// - We may allocate multiple new pages
/// - All modified pages should be collected and dropped together at the end
///
/// This allows insert to properly participate in multi-page WAL operations.
pub(super) struct InsertPageLockChain<'hf, const BUCKETS_COUNT: usize> {
    heap_file: &'hf HeapFile<BUCKETS_COUNT>,
    pages: Vec<PinnedWritePage>,
}

impl<'hf, const BUCKETS_COUNT: usize> InsertPageLockChain<'hf, BUCKETS_COUNT> {
    /// Creates a new empty InsertPageLockChain
    pub fn new(heap_file: &'hf HeapFile<BUCKETS_COUNT>) -> Self {
        InsertPageLockChain {
            heap_file,
            pages: Vec::new(),
        }
    }
}

impl<'hf, const BUCKETS_COUNT: usize> PageCollector for InsertPageLockChain<'hf, BUCKETS_COUNT> {
    fn consume_heap_page<H: BaseHeapPageHeader>(
        &mut self,
        heap_page: HeapPage<PinnedWritePage, H>,
    ) {
        self.pages.push(heap_page.page.into_page());
    }
}

impl<'hf, const BUCKETS_COUNT: usize> MultiPageOperationChain
    for InsertPageLockChain<'hf, BUCKETS_COUNT>
{
    fn drop_pages(self) {
        self.heap_file.cache.drop_write_pages(self.pages);
    }
}

/// Struct that should be used for special operations such as adding/removing column.
/// It works by not dropping any page it acquired and allowing for reusing them as long as this struct lives.
pub(super) struct NoDroppingPageLockChain<'hf, const BUCKETS_COUNT: usize> {
    heap_file: &'hf HeapFile<BUCKETS_COUNT>,
    record_page: Option<(PageId, HeapPage<PinnedWritePage, RecordPageHeader>)>,
    overflow_page: Option<(PageId, HeapPage<PinnedWritePage, OverflowPageHeader>)>,
    previous_record_pages: HashMap<PageId, HeapPage<PinnedWritePage, RecordPageHeader>>,
    previous_overflow_pages: HashMap<PageId, HeapPage<PinnedWritePage, OverflowPageHeader>>,
    /// Additional pages collected during inserts (from [`HeapFile::insert_using_only_overflow_pages`])
    additional_pages: Vec<PinnedWritePage>,
}

impl<'hf, const BUCKETS_COUNT: usize> NoDroppingPageLockChain<'hf, BUCKETS_COUNT> {
    /// Creates new [`NoDroppingPageLockChain`] with empty pages
    pub fn empty(heap_file: &'hf HeapFile<BUCKETS_COUNT>) -> Result<Self, HeapFileError> {
        Ok(NoDroppingPageLockChain {
            heap_file,
            record_page: None,
            overflow_page: None,
            previous_record_pages: HashMap::new(),
            previous_overflow_pages: HashMap::new(),
            additional_pages: Vec::new(),
        })
    }

    /// Read new record page
    pub fn start_from_record_page(&mut self, page_id: PageId) -> Result<(), HeapFileError> {
        if let Some((id, _)) = &self.record_page
            && *id == page_id
        {
            return Ok(());
        }

        let record_page = self.read_record_page(page_id)?;
        if let Some((id, r)) = self.record_page.take() {
            self.previous_record_pages.insert(id, r);
        };
        self.record_page = Some((page_id, record_page));
        Ok(())
    }

    fn read_record_page(
        &mut self,
        page_id: PageId,
    ) -> Result<HeapPage<PinnedWritePage, RecordPageHeader>, HeapFileError> {
        if let Some(r) = self.previous_record_pages.remove(&page_id) {
            return Ok(r);
        };
        self.heap_file.write_record_page(page_id)
    }

    fn read_overflow_page(
        &mut self,
        page_id: PageId,
    ) -> Result<HeapPage<PinnedWritePage, OverflowPageHeader>, HeapFileError> {
        if let Some(o) = self.previous_overflow_pages.remove(&page_id) {
            return Ok(o);
        };
        self.heap_file.write_overflow_page(page_id)
    }

    fn _advance(&mut self, next_page_id: PageId) -> Result<(), HeapFileError> {
        if let Some((id, _)) = &self.overflow_page
            && *id == next_page_id
        {
            return Ok(());
        }
        let overflow_page = self.read_overflow_page(next_page_id)?;
        if let Some((id, o)) = self.overflow_page.take() {
            self.previous_overflow_pages.insert(id, o);
        };
        self.overflow_page = Some((next_page_id, overflow_page));
        Ok(())
    }
}

impl<'hf, const BUCKETS_COUNT: usize> ReadPageLockChain<PinnedWritePage>
    for NoDroppingPageLockChain<'hf, BUCKETS_COUNT>
{
    fn advance(&mut self, next_page_id: PageId) -> Result<(), HeapFileError> {
        self._advance(next_page_id)
    }

    fn record_page(&self) -> &HeapPage<PinnedWritePage, RecordPageHeader> {
        match &self.record_page {
            Some((_, record_page)) => record_page,
            None => panic!(
                "invalid usage of NoDroppingPageLockChain - tried to get record page when overflow page is stored instead"
            ),
        }
    }

    fn overflow_page(&self) -> &HeapPage<PinnedWritePage, OverflowPageHeader> {
        match &self.overflow_page {
            Some((_, overflow_page)) => overflow_page,
            None => panic!(
                "invalid usage of NoDroppingPageLockChain - tried to get overflow page when record page is stored instead"
            ),
        }
    }
}

impl<'a, 'hf, const BUCKETS_COUNT: usize> ReadPageLockChain<PinnedWritePage>
    for &'a mut NoDroppingPageLockChain<'hf, BUCKETS_COUNT>
{
    fn advance(&mut self, next_page_id: PageId) -> Result<(), HeapFileError> {
        (**self)._advance(next_page_id)
    }

    fn record_page(&self) -> &HeapPage<PinnedWritePage, RecordPageHeader> {
        (**self).record_page()
    }

    fn overflow_page(&self) -> &HeapPage<PinnedWritePage, OverflowPageHeader> {
        (**self).overflow_page()
    }
}

impl<'hf, const BUCKETS_COUNT: usize> WritePageLockChain
    for NoDroppingPageLockChain<'hf, BUCKETS_COUNT>
{
    fn advance(&mut self, next_page_id: PageId) -> Result<(), HeapFileError> {
        self._advance(next_page_id)
    }

    fn record_page_mut(&mut self) -> &mut HeapPage<PinnedWritePage, RecordPageHeader> {
        match &mut self.record_page {
            Some((_, record_page)) => record_page,
            None => panic!(
                "invalid usage of NoDroppingPageLockChain - tried to get record page when overflow page is stored instead"
            ),
        }
    }

    fn overflow_page_mut(&mut self) -> &mut HeapPage<PinnedWritePage, OverflowPageHeader> {
        match &mut self.overflow_page {
            Some((_, overflow_page)) => overflow_page,
            None => panic!(
                "invalid usage of NoDroppingPageLockChain - tried to get overflow page when record page is stored instead"
            ),
        }
    }

    fn add_pages(&mut self, pages: Vec<PinnedWritePage>) {
        self.additional_pages.extend(pages);
    }
}

impl<'a, 'hf, const BUCKETS_COUNT: usize> WritePageLockChain
    for &'a mut NoDroppingPageLockChain<'hf, BUCKETS_COUNT>
{
    fn advance(&mut self, next_page_id: PageId) -> Result<(), HeapFileError> {
        (**self)._advance(next_page_id)
    }

    fn record_page_mut(&mut self) -> &mut HeapPage<PinnedWritePage, RecordPageHeader> {
        (**self).record_page_mut()
    }

    fn overflow_page_mut(&mut self) -> &mut HeapPage<PinnedWritePage, OverflowPageHeader> {
        (**self).overflow_page_mut()
    }

    fn add_pages(&mut self, pages: Vec<PinnedWritePage>) {
        self.additional_pages.extend(pages);
    }
}

impl<'hf, const BUCKETS_COUNT: usize> PageCollector
    for NoDroppingPageLockChain<'hf, BUCKETS_COUNT>
{
    fn consume_heap_page<H: BaseHeapPageHeader>(
        &mut self,
        heap_page: HeapPage<PinnedWritePage, H>,
    ) {
        // For NoDroppingPageLockChain, we store additional pages from inserts in a separate vector
        // These are pages that were allocated during insert_using_only_overflow_pages
        self.additional_pages.push(heap_page.page.into_page());
    }
}

impl<'a, 'hf, const BUCKETS_COUNT: usize> PageCollector
    for &'a mut NoDroppingPageLockChain<'hf, BUCKETS_COUNT>
{
    fn consume_heap_page<H: BaseHeapPageHeader>(
        &mut self,
        heap_page: HeapPage<PinnedWritePage, H>,
    ) {
        // Delegate to the actual implementation
        (**self).consume_heap_page(heap_page)
    }
}

impl<'hf, const BUCKETS_COUNT: usize> MultiPageOperationChain
    for NoDroppingPageLockChain<'hf, BUCKETS_COUNT>
{
    fn drop_pages(mut self) {
        let mut pages = Vec::with_capacity(
            2 + self.previous_record_pages.len()
                + self.previous_overflow_pages.len()
                + self.additional_pages.len(),
        );
        if let Some((_, r)) = self.record_page.take() {
            pages.push(r.page.into_page());
        }
        if let Some((_, r)) = self.overflow_page.take() {
            pages.push(r.page.into_page());
        }
        for (_, page) in self.previous_record_pages {
            pages.push(page.page.into_page());
        }
        for (_, page) in self.previous_overflow_pages {
            pages.push(page.page.into_page());
        }
        pages.extend(self.additional_pages);
        self.heap_file.cache.drop_write_pages(pages);
    }
}

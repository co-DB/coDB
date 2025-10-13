use crate::cache::{PageRead, PageWrite};
use crate::paged_file::PAGE_SIZE;
use crate::slotted_page::SlottedPageError::ForbiddenSlotCompaction;
use bitflags::bitflags;
use bytemuck::{Pod, PodCastError, Zeroable};
use std::marker::PhantomData;
use thiserror::Error;

/// Helper trait meant for structs implementing SlottedPageHeader trait. Making implementing it
/// compulsory should help remind to use #[repr(C)] for those structs (there is no way to ensure
/// that it is used otherwise)
pub(crate) unsafe trait ReprC {}

/// Magic number for assuring the page is initialized
pub(crate) const CO_DB_MAGIC_NUMBER: u16 = 0xC0DB;

/// Type alias for clarity
pub(crate) type SlotId = u16;

/// Struct responsible for storing metadata of a free block. Stored at the start of each free
/// block.
///
/// A free block is a way of allowing slotted page to reuse the empty space left after delete or
/// update operations. We store offset to the first free block inside the page header, and then we
/// can follow offsets inside each free block to get to the next ones.
#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub(crate) struct FreeBlock {
    /// Length of the whole block, including the space taken by storing this struct
    len: u16,
    /// Offset inside the page to the next freeblock
    next_block_offset: u16,
    /// Actual offset of the free block struct in the page. May differ from the one in next_block_offset
    /// since we need to align the free block in accordance to its alignment (2 bytes). Thus, when
    /// the actual offset is odd we need to move the free block to the next even offset
    actual_offset: u16,
}

impl FreeBlock {
    /// A minimal size needed for a free block to be created. If there is less empty space leftover
    /// than this value we don't create a free block and don't use this space until it is reclaimed
    /// during record compaction.
    pub const MIN_SIZE: u16 = 16;

    pub const SIZE: usize = size_of::<FreeBlock>();

    pub const ALIGNMENT: u16 = align_of::<FreeBlock>() as u16;
}

/// In the future add B-tree and heap file implementations of this
/// The structs implementing this trait must use #[repr(C)]
pub(crate) trait SlottedPageHeader: Pod + ReprC {
    fn base(&self) -> &SlottedPageBaseHeader;
}

/// Base header of the slotted page, which is the part of header we can access from all slotted
/// page implementations.
#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub(crate) struct SlottedPageBaseHeader {
    /// Magic number that indicates whether the page is initialized for CODB usage
    co_db_magic_number: u16,
    /// Total free space inside slotted page. Is equal to the sum of space inside free blocks and
    /// contiguous free space.
    total_free_space: u16,
    /// Free space between slots directory and record space.
    contiguous_free_space: u16,
    /// Offset of the start of the record area (i.e. the first used byte from the top of the page).
    /// This marks the end of the contiguous free space. All records are stored below this offset.
    record_area_offset: u16,
    /// Total size of the header including the base + custom header.
    header_size: u16,
    /// Offset of the first free block in the free blocks linked list.
    first_free_block_offset: u16,
    /// Index of the first free slot in the slot directory, which we can reuse during inserts.
    first_free_slot: SlotId,
    /// Total number of slots in the slots directory (including used and free slots).
    num_slots: u16,
    /// Page type enum disk representation
    page_type: PageTypeRepr,
    /// Flags for storing nothing (for now)
    flags: SlottedPageHeaderFlags,
}

impl SlottedPageBaseHeader {
    pub const NO_FREE_BLOCKS: u16 = u16::MAX;
    pub const NO_FREE_SLOTS: u16 = u16::MAX;

    /// Function for calculating the offset of the end of the slot directory.
    pub fn free_space_start(&self) -> u16 {
        self.header_size + self.num_slots * Slot::SIZE as u16
    }

    pub fn has_free_slot(&self) -> bool {
        self.first_free_slot != Self::NO_FREE_SLOTS
    }

    pub fn has_free_block(&self) -> bool {
        self.first_free_block_offset != Self::NO_FREE_SLOTS
    }

    pub fn new(header_size: u16, page_type: PageType) -> Self {
        assert!(
            header_size as usize >= size_of::<SlottedPageBaseHeader>(),
            "header_size must be at least {} bytes",
            size_of::<SlottedPageBaseHeader>()
        );
        Self {
            co_db_magic_number: CO_DB_MAGIC_NUMBER,
            total_free_space: PAGE_SIZE as u16,
            contiguous_free_space: PAGE_SIZE as u16 - header_size,
            record_area_offset: PAGE_SIZE as u16,
            header_size,
            first_free_block_offset: SlottedPageBaseHeader::NO_FREE_BLOCKS,
            first_free_slot: SlottedPageBaseHeader::NO_FREE_SLOTS,
            num_slots: 0,
            page_type: PageTypeRepr::from(page_type),
            flags: SlottedPageHeaderFlags::NO_FLAGS,
        }
    }

    pub fn page_type(&self) -> Result<PageType, SlottedPageError> {
        PageType::try_from(self.page_type)
    }
}

impl SlottedPageHeader for SlottedPageBaseHeader {
    fn base(&self) -> &SlottedPageBaseHeader {
        self
    }
}

unsafe impl ReprC for SlottedPageBaseHeader {}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    Generic = 0,
    Heap = 1,
    BTreeLeaf = 2,
    BTreeInternal = 3,
    Overflow = 4,
}

impl PageType {
    /// Returns true if by default given [`PageType`] should allow slot compaction.
    ///
    /// It is used by [`SlottedPage::new`]. If you want to specify this yourself you should use [`SlottedPage::with_compaction`].
    fn allow_slot_compaction(&self) -> bool {
        match self {
            // Only heap files shouldn't allow auto compaction, because [`RecordPtr`] is based on slot index.
            PageType::Heap => false,
            PageType::Overflow => false,
            _ => true,
        }
    }
}

#[repr(transparent)]
#[derive(Copy, Clone, Pod, Zeroable)]
pub struct PageTypeRepr(u8);

impl From<PageType> for PageTypeRepr {
    fn from(pt: PageType) -> Self {
        PageTypeRepr(pt as u8)
    }
}

impl TryFrom<PageTypeRepr> for PageType {
    type Error = SlottedPageError;

    fn try_from(raw: PageTypeRepr) -> Result<Self, Self::Error> {
        match raw.0 {
            0 => Ok(PageType::Generic),
            1 => Ok(PageType::Heap),
            2 => Ok(PageType::BTreeLeaf),
            3 => Ok(PageType::BTreeInternal),
            4 => Ok(PageType::Overflow),
            _ => Err(Self::Error::Corrupted {
                reason: "invalid page was encountered".to_string(),
            }),
        }
    }
}

bitflags! {
    #[repr(transparent)]
    #[derive(Pod, Zeroable, Copy, Clone, Debug)]
    struct SlottedPageHeaderFlags : u8 {
        const NO_FLAGS   = 0b0000_0000;
    }
}

/// Enum representing possible outcomes of an insert operation.
pub(crate) enum InsertResult {
    /// The insert succeeded and id of the slot referencing the inserted record is returned.
    Success(SlotId),
    /// There is not enough space in neither free blocks nor contiguous free space, but if the
    /// page was defragmented the record would fit.
    NeedsDefragmentation,
    /// The page is full and won't fit a record of this length.
    PageFull,
}

pub(crate) enum UpdateResult {
    /// The update succeeded
    Success,
    /// There is not enough space in neither free blocks nor contiguous free space, but if the
    /// page was defragmented the updated record would fit.
    NeedsDefragmentation,
    /// The page is full and won't fit a record of this length.
    PageFull,
}

#[derive(Debug, Error)]
pub enum SlottedPageError {
    #[error("tried to use uninitialized page")]
    UninitializedPage,
    #[error(
        "tried to access slot at index {out_of_bounds_index} while there were only {num_slots} records"
    )]
    SlotIndexOutOfBounds {
        num_slots: u16,
        out_of_bounds_index: SlotId,
    },
    #[error("tried to modify slot at position {position} while there were only {num_slots} slots")]
    InvalidPosition { num_slots: u16, position: SlotId },
    #[error("tried to access deleted record with slot index {slot_index}")]
    ForbiddenDeletedRecordAccess { slot_index: SlotId },
    #[error("tried to compact slots even though allow_slot_compaction was set to false")]
    ForbiddenSlotCompaction,
    #[error("casting data from bytes failed for the following reason: {reason}")]
    CastError { reason: String },
    #[error("page was corrupted, as {reason}")]
    Corrupted { reason: String },
}

impl From<PodCastError> for SlottedPageError {
    fn from(err: PodCastError) -> Self {
        Self::CastError {
            reason: err.to_string(),
        }
    }
}

/// Slot containing data allowing for access to the record it references.
#[derive(Pod, Zeroable, Clone, Copy)]
#[repr(C)]
pub struct Slot {
    /// Offset to record data
    pub offset: u16,
    /// Length of the record
    pub len: u16,
    /// Stores deleted status + next free slot index.
    /// Highest bit = deleted flag, lower 15 bits = next free slot index.
    pub flags: u16,
}

impl Slot {
    const NO_FREE_SLOTS: u16 = 0x7FFF;

    const SLOT_DELETED: u16 = 0x8000;

    const SLOT_INDEX_MASK: u16 = 0x7FFF;

    const SIZE: usize = size_of::<Slot>();

    /// Creates a new active slot with given offset and length.
    pub fn new(offset: u16, len: u16) -> Self {
        Self {
            offset,
            len,
            flags: Self::NO_FREE_SLOTS,
        }
    }

    /// Returns true if this slot is marked as deleted (highest bit set).
    pub fn is_deleted(&self) -> bool {
        self.flags & Self::SLOT_DELETED != 0
    }

    /// Marks this slot as deleted by setting the highest bit.
    pub fn mark_deleted(&mut self) {
        self.flags |= Self::SLOT_DELETED;
    }

    /// Sets the index of the next free slot in the lower 15 bits of `flags`.
    pub fn set_next_free_slot(&mut self, next_free_slot_index: SlotId) {
        let next = next_free_slot_index & Self::SLOT_INDEX_MASK;
        let deleted_flag = self.flags & Self::SLOT_DELETED;
        self.flags = deleted_flag | next;
    }

    /// Returns the index of the next free slot (lower 15 bits of `flags`).
    pub fn next_free_slot(&self) -> SlotId {
        self.flags & Self::SLOT_INDEX_MASK
    }
}

/// A wrapper around a page, that allows higher level abstractions to interact with structured
/// page data without dealing directly with raw bytes. Exposes typical slotted page methods like
/// read,insert,update,delete and also slot/record compaction.
///
/// A visual for how a slotted page looks can be seen in slotted_page.png in docs directory of this
/// crate.
pub(crate) struct SlottedPage<P, H: SlottedPageHeader> {
    /// The underlying page, with which the slotted page interacts.
    page: P,
    /// Controls whether slots within the page can be compacted to reclaim space after deletions.
    /// Safeguards the compact_slots method which should not be called by some higher level abstractions
    /// (e.g. heap file slotted pages).
    allow_slot_compaction: bool,
    /// We need to tie a SlottedPage to specific header type, but since there is no sensible field of
    /// type H that we can create, we need to use PhantomData just to mark that we are using this
    /// type.
    _header_marker: PhantomData<H>,
}

/// Implementation for read-only slotted page
impl<P: PageRead, H: SlottedPageHeader> SlottedPage<P, H> {
    /// Creates a new SlottedPage wrapper around a page with default slot compaction settings based on page type.
    pub fn new(page: P) -> Result<Self, SlottedPageError> {
        let data = page.data();
        Self::validate_magic_number(data)?;

        let mut page = Self {
            page,
            allow_slot_compaction: false,
            _header_marker: PhantomData,
        };

        let base_header = page.get_base_header()?;
        let ty = base_header.page_type()?;
        let allow_slot_compaction = ty.allow_slot_compaction();
        page.allow_slot_compaction = allow_slot_compaction;

        Ok(page)
    }

    /// Creates a new SlottedPage with explicit slot compaction control.
    ///
    /// Use this when you need to override the default compaction behavior for the page type.
    pub fn with_compaction(page: P, allow_slot_compaction: bool) -> Result<Self, SlottedPageError> {
        let data = page.data();
        Self::validate_magic_number(data)?;

        let page = Self {
            page,
            allow_slot_compaction,
            _header_marker: PhantomData,
        };

        Ok(page)
    }

    /// Checks if `data` starts with CODB magic number.
    fn validate_magic_number(data: &[u8]) -> Result<(), SlottedPageError> {
        let magic_number = u16::from_le_bytes([data[0], data[1]]);
        if magic_number != CO_DB_MAGIC_NUMBER {
            return Err(SlottedPageError::UninitializedPage);
        }
        Ok(())
    }

    /// Generic method to cast page header to any type implementing SlottedPageHeader
    ///
    /// Caller must ensure T matches the actual header type stored in the page
    fn get_generic_header<T>(&self) -> Result<&T, SlottedPageError>
    where
        T: SlottedPageHeader,
    {
        Ok(bytemuck::try_from_bytes(
            &self.page.data()[..size_of::<T>()],
        )?)
    }

    /// Gets a reference to the header
    pub fn get_header(&self) -> Result<&H, SlottedPageError> {
        self.get_generic_header::<H>()
    }

    /// Gets a reference to the base header (common to all slotted pages).
    pub fn get_base_header(&self) -> Result<&SlottedPageBaseHeader, SlottedPageError> {
        self.get_generic_header::<SlottedPageBaseHeader>()
    }

    /// Gets a reference to the base header from the given page without creating an instance of
    /// the slotted page struct. Can be used for e.g. getting the page type before creating a
    /// specific slotted page wrapper like B-Tree internal or leaf node.
    pub fn static_get_base_header(page: &P) -> Result<&SlottedPageBaseHeader, SlottedPageError> {
        Ok(bytemuck::try_from_bytes(
            &page.data()[..size_of::<SlottedPageBaseHeader>()],
        )?)
    }

    /// Returns the total number of slots (both used and unused) in this page
    pub fn num_slots(&self) -> Result<u16, SlottedPageError> {
        let header = self.get_base_header()?;
        Ok(header.num_slots)
    }

    /// Returns the total amount of free space available on this page
    /// This includes both contiguous free space and fragmented free blocks
    pub fn free_space(&self) -> Result<u16, SlottedPageError> {
        let header = self.get_base_header()?;
        Ok(header.total_free_space)
    }

    /// Returns a slice containing all slots (cast) in the slot directory
    fn get_slots(&self) -> Result<&[Slot], SlottedPageError> {
        let header = self.get_base_header()?;
        let start = header.header_size as usize;
        let end = start + (header.num_slots as usize * Slot::SIZE);
        Ok(bytemuck::try_cast_slice(&self.page.data()[start..end])?)
    }

    /// Gets a reference to a specific slot by index
    fn get_slot(&self, slot_id: SlotId) -> Result<&Slot, SlottedPageError> {
        let header = self.get_base_header()?;

        if slot_id >= header.num_slots {
            return Err(SlottedPageError::SlotIndexOutOfBounds {
                num_slots: header.num_slots,
                out_of_bounds_index: slot_id,
            });
        }

        let slots = self.get_slots()?;
        Ok(&slots[slot_id as usize])
    }

    /// Reads the record data for a given slot. Checks if the record is deleted . Safe version of
    /// read_record
    pub fn read_record(&self, slot_id: SlotId) -> Result<&[u8], SlottedPageError> {
        let slot = self.get_slot(slot_id)?;

        if slot.is_deleted() {
            return Err(SlottedPageError::ForbiddenDeletedRecordAccess {
                slot_index: slot_id,
            });
        }

        let record_start = slot.offset as usize;
        let record_end = record_start + slot.len as usize;

        Ok(&self.page.data()[record_start..record_end])
    }
}

impl<P: PageWrite + PageRead, H: SlottedPageHeader> SlottedPage<P, H> {
    pub fn initialize_with_header(page: P, allow_slot_compaction: bool, header: H) -> Self {
        let mut page = page;
        page.data_mut()[0..size_of::<H>()].copy_from_slice(bytemuck::bytes_of(&header));
        Self {
            page,
            allow_slot_compaction,
            _header_marker: PhantomData,
        }
    }

    pub fn initialize_default(page: P, allow_slot_compaction: bool) -> Self
    where
        H: Default,
    {
        Self::initialize_with_header(page, allow_slot_compaction, H::default())
    }

    /// Generic method to cast page header to any type implementing SlottedPageHeader
    ///
    /// Caller must ensure T matches the actual header type stored in the page
    fn get_generic_header_mut<T>(&mut self) -> Result<&mut T, SlottedPageError>
    where
        T: SlottedPageHeader,
    {
        Ok(bytemuck::try_from_bytes_mut(
            &mut self.page.data_mut()[..size_of::<T>()],
        )?)
    }

    pub fn get_header_mut(&mut self) -> Result<&mut H, SlottedPageError> {
        self.get_generic_header_mut::<H>()
    }

    /// Gets a mutable reference to base header (common to all slotted pages).
    pub fn get_base_header_mut(&mut self) -> Result<&mut SlottedPageBaseHeader, SlottedPageError> {
        self.get_generic_header_mut::<SlottedPageBaseHeader>()
    }

    /// Updates the record at given position.
    pub fn update(
        &mut self,
        slot_id: SlotId,
        updated_record: &[u8],
    ) -> Result<UpdateResult, SlottedPageError> {
        let slot = self.get_slot(slot_id)?;

        if slot.is_deleted() {
            return Err(SlottedPageError::ForbiddenDeletedRecordAccess {
                slot_index: slot_id,
            });
        }

        let old_len = slot.len;
        let new_len = updated_record.len() as u16;
        let old_offset = slot.offset;

        if old_len >= new_len {
            self.write_record_at(updated_record, old_offset);

            let updated_slot = self.get_slot_mut(slot_id)?;
            updated_slot.len = new_len;

            let leftover = old_len - new_len;
            self.add_freeblock(old_offset + new_len, leftover)?;

            let header = self.get_base_header_mut()?;
            header.total_free_space += leftover;

            return Ok(UpdateResult::Success);
        }

        let additional_space_needed = new_len - old_len;
        let header = self.get_base_header()?;

        if header.total_free_space < additional_space_needed {
            return Ok(UpdateResult::PageFull);
        }

        let offset = match self.get_allocated_space(new_len, false)? {
            Some(offset) => offset,
            None => return Ok(UpdateResult::NeedsDefragmentation),
        };

        self.write_record_at(updated_record, offset);

        // Create free block from old location
        self.add_freeblock(old_offset, old_len)?;
        let header = self.get_base_header_mut()?;
        header.total_free_space += old_len;

        let updated_slot = self.get_slot_mut(slot_id)?;
        updated_slot.offset = offset;
        updated_slot.len = new_len;

        Ok(UpdateResult::Success)
    }

    /// Marks a slot as deleted, compacts records and inserts the updated record. This should
    /// be used when update doesn't succeed with NeedsDefragmentation result.
    pub fn defragment_and_update(
        &mut self,
        slot_id: SlotId,
        updated_record: &[u8],
    ) -> Result<UpdateResult, SlottedPageError> {
        let slot = self.get_slot_mut(slot_id)?;
        if slot.is_deleted() {
            return Err(SlottedPageError::ForbiddenDeletedRecordAccess {
                slot_index: slot_id,
            });
        }

        let old_record_len = slot.len;
        slot.mark_deleted();

        let header = self.get_base_header_mut()?;
        header.total_free_space += old_record_len;

        self.compact_records()?;

        let offset = match self.get_allocated_space(updated_record.len() as u16, false)? {
            Some(offset) => offset,
            None => return Ok(UpdateResult::PageFull),
        };

        self.write_record_at(updated_record, offset);

        let slot = self.get_slot_mut(slot_id)?;
        slot.offset = offset;
        slot.len = updated_record.len() as u16;
        slot.flags = 0;

        Ok(UpdateResult::Success)
    }

    /// Deletes the record from the given position.
    pub fn delete(&mut self, slot_id: SlotId) -> Result<(), SlottedPageError> {
        let next_free_slot = self.get_base_header()?.first_free_slot;
        let slot = self.get_slot_mut(slot_id)?;
        if slot.is_deleted() {
            return Err(SlottedPageError::ForbiddenDeletedRecordAccess {
                slot_index: slot_id,
            });
        }

        slot.mark_deleted();
        slot.set_next_free_slot(next_free_slot);

        // copy slot to handle borrow checker complaints
        let copied_slot = *self.get_slot(slot_id)?;

        let header = self.get_base_header_mut()?;
        header.total_free_space += copied_slot.len;
        header.first_free_slot = slot_id;

        if copied_slot.offset == header.record_area_offset {
            header.contiguous_free_space += copied_slot.len;
            header.record_area_offset += copied_slot.len;
        } else {
            self.add_freeblock(copied_slot.offset, copied_slot.len)?;
        }

        Ok(())
    }

    /// Calculates the offset of the free block, so that it matches the alignment of the struct (2 bytes)
    fn calculate_free_block_aligned_offset(actual_offset: u16) -> u16 {
        actual_offset.next_multiple_of(FreeBlock::ALIGNMENT)
    }
    /// Adds a free block with given length at the given offset and adds it to the free block linked
    /// list.
    fn add_freeblock(&mut self, offset: u16, len: u16) -> Result<(), SlottedPageError> {
        if len < FreeBlock::MIN_SIZE {
            return Ok(());
        }

        let next_block_offset = self.get_base_header()?.first_free_block_offset;

        let page = self.page.data_mut();

        let new_freeblock = FreeBlock {
            len,
            next_block_offset,
            actual_offset: offset,
        };

        let aligned_offset = Self::calculate_free_block_aligned_offset(offset);
        let start = aligned_offset as usize;
        let end = start + FreeBlock::SIZE;
        page[start..end].copy_from_slice(bytemuck::bytes_of(&new_freeblock));

        self.get_base_header_mut()?.first_free_block_offset = aligned_offset;

        Ok(())
    }

    /// Inserts a record at a given slot position.
    ///
    /// Shifts existing slots to the right if necessary, allocates space for the record,
    /// writes it, and updates the header.
    pub fn insert_at(
        &mut self,
        record: &[u8],
        position: SlotId,
    ) -> Result<InsertResult, SlottedPageError> {
        let header = self.get_base_header()?;

        if position > header.num_slots {
            return Err(SlottedPageError::InvalidPosition {
                position,
                num_slots: header.num_slots,
            });
        }

        let needs_new_slot = position == header.num_slots || !self.get_slot(position)?.is_deleted();

        let required_space = record.len() + if needs_new_slot { Slot::SIZE } else { 0 };

        if required_space as u16 > header.total_free_space {
            return Ok(InsertResult::PageFull);
        }

        let offset = match self.get_allocated_space(record.len() as u16, needs_new_slot)? {
            None => return Ok(InsertResult::NeedsDefragmentation),
            Some(offset) => offset,
        };
        self.write_record_at(record, offset);

        if needs_new_slot {
            self.shift_slots_right(position)?;
        } else {
            self.remove_from_free_slot_chain(position)?;
        }

        self.write_slot_at(position, Slot::new(offset, record.len() as u16))?;

        let header_mut = self.get_base_header_mut()?;

        if needs_new_slot {
            header_mut.num_slots += 1;
            header_mut.total_free_space -= Slot::SIZE as u16;
            header_mut.contiguous_free_space -= Slot::SIZE as u16;
        }

        Ok(InsertResult::Success(position))
    }

    /// Writes a slot to the slot directory at a given position.
    fn write_slot_at(&mut self, position: SlotId, slot: Slot) -> Result<(), SlottedPageError> {
        let header_size = self.get_base_header()?.header_size as usize;
        let page = self.page.data_mut();

        let start = header_size + position as usize * Slot::SIZE;
        let end = start + Slot::SIZE;

        page[start..end].copy_from_slice(bytemuck::bytes_of(&slot));

        Ok(())
    }

    /// Shifts slots to the right starting from a given position.
    ///
    /// Used when inserting into the middle of the slot directory.
    fn shift_slots_right(&mut self, position: SlotId) -> Result<(), SlottedPageError> {
        let header = self.get_base_header()?;

        let shifted_slots_num = header.num_slots - position;
        if shifted_slots_num == 0 {
            return Ok(());
        }
        let start = header.header_size as usize + position as usize * Slot::SIZE;
        let end = start + shifted_slots_num as usize * Slot::SIZE;

        let page = self.page.data_mut();

        page.copy_within(start..end, start + Slot::SIZE);

        Ok(())
    }

    /// Walks the free slot linked list and removes slot with slot_id from it, updating the
    /// previous slot's next pointer or the header's first_free_slot.
    fn remove_from_free_slot_chain(&mut self, slot_id: SlotId) -> Result<(), SlottedPageError> {
        let target_slot = self.get_slot(slot_id)?;

        if !target_slot.is_deleted() {
            return Ok(());
        }

        let next_free_slot_id = target_slot.next_free_slot();

        let mut prev = Slot::NO_FREE_SLOTS;
        let mut current_free_slot_id = self.get_base_header()?.first_free_slot;

        while current_free_slot_id != Slot::NO_FREE_SLOTS {
            if current_free_slot_id == slot_id {
                if prev == Slot::NO_FREE_SLOTS {
                    self.get_base_header_mut()?.first_free_slot = next_free_slot_id;
                } else {
                    self.get_slot_mut(prev)?
                        .set_next_free_slot(next_free_slot_id);
                }

                let removed_slot = self.get_slot_mut(slot_id)?;
                removed_slot.set_next_free_slot(Slot::NO_FREE_SLOTS);

                return Ok(());
            }

            let current_slot = self.get_slot(current_free_slot_id)?;
            prev = current_free_slot_id;
            current_free_slot_id = current_slot.next_free_slot();
        }

        Ok(())
    }

    /// Inserts a record at the end of the slot directory.
    ///
    /// Automatically reuses free slots if available, or appends a new one otherwise.
    pub fn insert(&mut self, record: &[u8]) -> Result<InsertResult, SlottedPageError> {
        let header = self.get_base_header()?;

        let needs_new_slot = !header.has_free_slot();

        let required_space = record.len() + if needs_new_slot { Slot::SIZE } else { 0 };

        if required_space as u16 > header.total_free_space {
            return Ok(InsertResult::PageFull);
        }

        let offset = match self.get_allocated_space(record.len() as u16, needs_new_slot)? {
            None => return Ok(InsertResult::NeedsDefragmentation),
            Some(offset) => offset,
        };

        self.write_record_at(record, offset);

        let slot = Slot::new(offset, record.len() as u16);

        // we call reuse or append here instead of just reuse to account for all possible situations.
        let slot_id = self.reuse_or_append_slot(slot)?;
        Ok(InsertResult::Success(slot_id))
    }

    /// Appends a slot at the end of the slot directory.\
    fn append_slot(&mut self, slot: Slot) -> Result<SlotId, SlottedPageError> {
        let num_slots = self.get_base_header()?.num_slots;
        self.write_slot_at(num_slots, slot)?;
        let header = self.get_base_header_mut()?;
        let slot_id = header.num_slots;
        header.num_slots += 1;
        header.contiguous_free_space -= Slot::SIZE as u16;
        header.total_free_space -= Slot::SIZE as u16;
        Ok(slot_id)
    }

    /// If a free slot exists, reuses it. Otherwise, appends a new slot.
    fn reuse_or_append_slot(&mut self, slot: Slot) -> Result<SlotId, SlottedPageError> {
        let header = self.get_base_header()?;

        if !header.has_free_slot() {
            return self.append_slot(slot);
        }

        let free_slot_id = header.first_free_slot;

        let free_slot = self.get_slot(free_slot_id)?;
        let next_free_slot_id = free_slot.next_free_slot();

        self.write_slot_at(free_slot_id, slot)?;

        self.get_base_header_mut()?.first_free_slot = next_free_slot_id;

        Ok(free_slot_id)
    }

    /// Writes record bytes into the page at the given offset.
    fn write_record_at(&mut self, record: &[u8], offset: u16) {
        let start = offset as usize;
        let end = start + record.len();
        self.page.data_mut()[start..end].copy_from_slice(record)
    }

    /// Allocates space for a record of length `record_len`.
    ///
    /// Attempts to place it in contiguous free space first; if not possible,
    /// searches the freeblock chain. May return `None` if defragmentation is required
    fn get_allocated_space(
        &mut self,
        record_len: u16,
        needs_new_slot: bool,
    ) -> Result<Option<u16>, SlottedPageError> {
        let header = self.get_base_header_mut()?;
        let slot_size = if needs_new_slot { Slot::SIZE as u16 } else { 0 };
        let required_space = record_len + slot_size;

        // First we check whether we can fit the record in the contiguous space between slot directory
        // and record space.
        if header.contiguous_free_space >= required_space {
            header.contiguous_free_space -= record_len;
            header.total_free_space -= record_len;
            header.record_area_offset -= record_len;
            return Ok(Some(header.record_area_offset));
        }

        // This check is so we don't run into situations where we have enough space in a free block
        // to insert a record, but we don't have enough space to add a new slot. In that case the
        // insert must fail.
        if needs_new_slot && header.contiguous_free_space < slot_size {
            return Ok(None);
        }

        // If contiguous space allocation failed we search through free blocks.
        if let Some((prev_offset, offset)) = self.find_free_space(record_len)? {
            let block = *bytemuck::try_from_bytes::<FreeBlock>(
                &self.page.data()[offset as usize..offset as usize + FreeBlock::SIZE],
            )?;

            // actual offset is the non-aligned offset that holds the actual start of the free block,
            // meaning the offset at which we want to write records
            let actual_offset = block.actual_offset;

            // If leftover after insertion is too small, consume entire block.
            if block.len.saturating_sub(record_len) < FreeBlock::MIN_SIZE {
                self.get_base_header_mut()?.total_free_space -= block.len;
                self.update_freeblock_chain(prev_offset, block.next_block_offset)?;
                return Ok(Some(actual_offset));
            }

            // Otherwise, split block into record + smaller free block.
            let consumed_space = record_len + FreeBlock::SIZE as u16;
            self.get_base_header_mut()?.total_free_space -= consumed_space;

            let new_freeblock_actual_offset = actual_offset + record_len;

            // aligned offset is used so we can use bytemuck's zero-copy from_bytes (free block
            // struct must be correctly aligned)
            let aligned_offset =
                Self::calculate_free_block_aligned_offset(new_freeblock_actual_offset);

            let new_freeblock = FreeBlock {
                next_block_offset: block.next_block_offset,
                len: block.len - record_len - FreeBlock::SIZE as u16,
                actual_offset: new_freeblock_actual_offset,
            };

            let start = aligned_offset as usize;
            let end = start + FreeBlock::SIZE;
            self.page.data_mut()[start..end].copy_from_slice(bytemuck::bytes_of(&new_freeblock));

            // we store the aligned offset in the free block linked list so we can read the free block
            // without calculating aligned version of it every time.
            self.update_freeblock_chain(prev_offset, aligned_offset)?;
            return Ok(Some(actual_offset));
        }

        Ok(None)
    }

    /// Updates the linked list of free blocks when one is consumed or split.
    fn update_freeblock_chain(
        &mut self,
        prev_offset: u16,
        new_next: u16,
    ) -> Result<(), SlottedPageError> {
        // If prev offset is NO_FREE_BLOCKS then it means it was the first free block in the linked
        // list, and thus we only need to replace the offset in the header (the head of the list)
        if prev_offset == SlottedPageBaseHeader::NO_FREE_BLOCKS {
            self.get_base_header_mut()?.first_free_block_offset = new_next;
        } else {
            // Else we join together the previous block on the list and the next one after the deleted one
            let prev = bytemuck::try_from_bytes_mut::<FreeBlock>(
                &mut self.page.data_mut()
                    [prev_offset as usize..prev_offset as usize + FreeBlock::SIZE],
            )?;
            prev.next_block_offset = new_next;
        }
        Ok(())
    }

    /// Finds the first free block large enough for `record_len`. Returns offset of the free block
    /// previous to the found one (SlottedPageBaseHeader::NO_FREE_BLOCKS in case this one is the
    /// head of the list) and the offset of the found one. The offset are aligned meaning the user
    /// can read the free block metadata from that exact offset, but for writing a new record
    /// the offset is stored in free block's actual_offset field.
    fn find_free_space(&self, record_len: u16) -> Result<Option<(u16, u16)>, SlottedPageError> {
        let header = self.get_base_header()?;

        let mut block_offset = header.first_free_block_offset;
        let mut prev_offset = SlottedPageBaseHeader::NO_FREE_BLOCKS;
        // We go through the linked list of free blocks and read the metadata of each blocks until
        // we encounter either a free block containing enough space or the end of the linked
        // list - NO_FREE_BLOCKS.
        while block_offset != SlottedPageBaseHeader::NO_FREE_BLOCKS {
            let start = block_offset as usize;
            let end = start + FreeBlock::SIZE;
            let freeblock = bytemuck::try_from_bytes::<FreeBlock>(&self.page.data()[start..end])?;
            if freeblock.len >= record_len {
                return Ok(Some((prev_offset, block_offset)));
            }
            prev_offset = block_offset;
            block_offset = freeblock.next_block_offset;
        }
        Ok(None)
    }

    /// Returns a mutable slice of all slots.
    fn get_slots_mut(&mut self) -> Result<&mut [Slot], SlottedPageError> {
        let header = self.get_base_header()?;
        let start = header.header_size as usize;
        let end = start + (header.num_slots as usize * Slot::SIZE);
        Ok(bytemuck::try_cast_slice_mut(
            &mut self.page.data_mut()[start..end],
        )?)
    }

    /// Returns a mutable reference to a specific slot.
    fn get_slot_mut(&mut self, slot_id: SlotId) -> Result<&mut Slot, SlottedPageError> {
        let header = self.get_base_header()?;
        let start = header.header_size as usize + slot_id as usize * Slot::SIZE;
        let end = start + Slot::SIZE;
        Ok(bytemuck::try_from_bytes_mut(
            &mut self.page.data_mut()[start..end],
        )?)
    }

    /// Compacts the slot directory by removing deleted slots and shifting
    /// remaining slots toward the beginning of the slot array.
    ///
    /// After compaction, the slot directory contains only active slots, packed
    /// tightly from the start of the slot area.
    pub fn compact_slots(&mut self) -> Result<(), SlottedPageError> {
        if !self.allow_slot_compaction {
            return Err(ForbiddenSlotCompaction);
        }

        let header_size = self.get_base_header()?.header_size as usize;

        let slots = self.get_slots()?;

        // Collect all indices of non-deleted slots.
        let filled_slots: Vec<usize> = slots
            .iter()
            .enumerate()
            .filter_map(|(idx, slot)| (!slot.is_deleted()).then_some(idx))
            .collect();

        let filled_count = filled_slots.len();
        let deleted_count = slots.len() - filled_count;
        let freed_space = deleted_count * Slot::SIZE;

        let page = self.page.data_mut();

        // Compact slots by moving filled ones toward the start.
        for (dst_i, src_i) in filled_slots.into_iter().enumerate() {
            let copy_location = header_size + dst_i * Slot::SIZE;
            let src_start = header_size + src_i * Slot::SIZE;
            let src_end = src_start + Slot::SIZE;

            page.copy_within(src_start..src_end, copy_location);
        }

        let header = self.get_base_header_mut()?;
        header.num_slots = filled_count as u16;
        header.total_free_space += freed_space as u16;
        header.contiguous_free_space += freed_space as u16;
        header.first_free_slot = SlottedPageBaseHeader::NO_FREE_SLOTS;
        Ok(())
    }

    /// Compacts all non-deleted records in the slotted page by moving them
    /// into a contiguous block at the end of the page.
    ///
    /// After compaction, all free space is guaranteed to be one contiguous
    /// block between the slot directory and the record area.
    pub fn compact_records(&mut self) -> Result<(), SlottedPageError> {
        let mut write_pos = self.page.data().len();

        // Get non-deleted slots (copied) and their indices
        let mut slots: Vec<(usize, Slot)> = self
            .get_slots()?
            .iter()
            .enumerate()
            .filter_map(|(idx, slot)| (!slot.is_deleted()).then_some((idx, *slot)))
            .collect();

        // Sort slots by descending offset to avoid overwriting the records that have not yet been moved.
        slots.sort_by_key(|(_, slot)| -(slot.offset as i32));
        for (idx, slot) in slots {
            write_pos -= slot.len as usize;

            if slot.offset == write_pos as u16 {
                continue;
            }

            let record_range = slot.offset as usize..slot.offset as usize + slot.len as usize;

            // Move record into new compacted position and update its corresponding slot to new offset
            self.page.data_mut().copy_within(record_range, write_pos);
            self.get_slot_mut(idx as u16)?.offset = write_pos as u16;
        }
        let header = self.get_base_header_mut()?;
        header.record_area_offset = write_pos as u16;
        header.contiguous_free_space = write_pos as u16 - header.free_space_start();
        header.first_free_block_offset = SlottedPageBaseHeader::NO_FREE_BLOCKS;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAGE_SIZE: usize = 4096;

    // Simple page for testing with configurable size
    struct TestPage {
        data: Vec<u8>,
    }

    impl TestPage {
        fn new(size: usize) -> Self {
            Self {
                data: vec![0; size],
            }
        }
    }

    impl PageRead for TestPage {
        fn data(&self) -> &[u8] {
            &self.data
        }
    }

    impl PageWrite for TestPage {
        fn data_mut(&mut self) -> &mut [u8] {
            &mut self.data
        }
    }

    // Helper to create initialized slotted page
    fn create_test_page(size: usize) -> SlottedPage<TestPage, SlottedPageBaseHeader> {
        let mut page = TestPage::new(size);

        let header = SlottedPageBaseHeader {
            co_db_magic_number: CO_DB_MAGIC_NUMBER,
            total_free_space: (size - size_of::<SlottedPageBaseHeader>()) as u16,
            contiguous_free_space: (size - size_of::<SlottedPageBaseHeader>()) as u16,
            record_area_offset: size as u16,
            header_size: size_of::<SlottedPageBaseHeader>() as u16,
            first_free_block_offset: SlottedPageBaseHeader::NO_FREE_BLOCKS,
            first_free_slot: SlottedPageBaseHeader::NO_FREE_SLOTS,
            num_slots: 0,
            page_type: PageTypeRepr::from(PageType::Generic),
            flags: SlottedPageHeaderFlags::NO_FLAGS,
        };

        page.data_mut()[..size_of::<SlottedPageBaseHeader>()]
            .copy_from_slice(bytemuck::bytes_of(&header));

        SlottedPage::new(page).unwrap()
    }

    #[test]
    fn test_insert_and_read() {
        let mut page = create_test_page(PAGE_SIZE);

        let data = b"hello world";
        let result = page.insert(data).unwrap();

        assert!(matches!(result, InsertResult::Success(0)));
        assert_eq!(page.read_record(0).unwrap(), data);
        assert_eq!(page.num_slots().unwrap(), 1);
    }

    #[test]
    fn test_multiple_inserts() {
        let mut page = create_test_page(PAGE_SIZE);

        let records = [b"first", b"secon", b"third"];

        for (i, &record) in records.iter().enumerate() {
            let result = page.insert(record).unwrap();
            assert!(matches!(result, InsertResult::Success(_)));
            assert_eq!(page.read_record(i as SlotId).unwrap(), record);
        }

        assert_eq!(page.num_slots().unwrap(), 3);
    }

    #[test]
    fn test_large_record() {
        let mut page = create_test_page(PAGE_SIZE);

        let large_data = vec![42u8; 1000];
        let result = page.insert(&large_data).unwrap();

        assert!(matches!(result, InsertResult::Success(0)));
        assert_eq!(page.read_record(0).unwrap(), large_data.as_slice());
    }

    #[test]
    fn test_insert_at_beginning() {
        let mut page = create_test_page(PAGE_SIZE);

        page.insert(b"second").unwrap();

        let result = page.insert_at(b"first", 0).unwrap();
        assert!(matches!(result, InsertResult::Success(0)));

        assert_eq!(page.read_record(0).unwrap(), b"first");
        assert_eq!(page.read_record(1).unwrap(), b"second");
        assert_eq!(page.num_slots().unwrap(), 2);
    }

    #[test]
    fn test_insert_at_middle() {
        let mut page = create_test_page(PAGE_SIZE);

        page.insert(b"first").unwrap();
        page.insert(b"third").unwrap();

        let result = page.insert_at(b"second", 1).unwrap();
        assert!(matches!(result, InsertResult::Success(1)));

        assert_eq!(page.read_record(0).unwrap(), b"first");
        assert_eq!(page.read_record(1).unwrap(), b"second");
        assert_eq!(page.read_record(2).unwrap(), b"third");
    }

    #[test]
    fn test_insert_at_end() {
        let mut page = create_test_page(PAGE_SIZE);

        page.insert(b"first").unwrap();

        let result = page.insert_at(b"second", 1).unwrap();
        assert!(matches!(result, InsertResult::Success(1)));

        assert_eq!(page.read_record(1).unwrap(), b"second");
    }

    #[test]
    fn test_insert_at_invalid_position() {
        let mut page = create_test_page(PAGE_SIZE);

        let result = page.insert_at(b"data", 1);
        assert!(matches!(
            result,
            Err(SlottedPageError::InvalidPosition { .. })
        ));
    }

    #[test]
    fn test_exact_fit() {
        let page_size = 128;
        let mut page = create_test_page(page_size);

        let header_size = size_of::<SlottedPageBaseHeader>();
        let slot_size = size_of::<Slot>();
        let available_space = page_size - header_size - slot_size;

        let data = vec![42u8; available_space];
        let result = page.insert(&data).unwrap();

        assert!(matches!(result, InsertResult::Success(0)));
        assert_eq!(page.free_space().unwrap(), 0);
    }

    #[test]
    fn test_exact_fit_plus_one_byte() {
        let mut page = create_test_page(PAGE_SIZE);

        let header_size = size_of::<SlottedPageBaseHeader>();
        let slot_size = size_of::<Slot>();
        let available_space = PAGE_SIZE - header_size - slot_size;

        let data = vec![42u8; available_space + 1];
        let result = page.insert(&data).unwrap();

        assert!(matches!(result, InsertResult::PageFull));
    }

    #[test]
    fn test_empty_record() {
        let mut page = create_test_page(PAGE_SIZE);
        let result = page.insert(b"").unwrap();
        assert!(matches!(result, InsertResult::Success(0)));
        assert_eq!(page.read_record(0).unwrap(), b"");
    }

    #[test]
    fn test_page_full() {
        let mut page = create_test_page(64);

        let large_record = vec![0u8; 32];
        page.insert(&large_record).unwrap();

        let result = page.insert(b"too much").unwrap();
        assert!(matches!(result, InsertResult::PageFull));
    }

    #[test]
    fn test_read_out_of_bounds() {
        let page = create_test_page(PAGE_SIZE);

        let result = page.read_record(0);
        assert!(matches!(
            result,
            Err(SlottedPageError::SlotIndexOutOfBounds {
                num_slots: 0,
                out_of_bounds_index: 0
            })
        ));
    }

    #[test]
    fn test_custom_header() {
        #[derive(Pod, Zeroable, Copy, Clone)]
        #[repr(C)]
        struct CustomHeader {
            base: SlottedPageBaseHeader,
            custom_field: u16,
        }

        unsafe impl ReprC for CustomHeader {}

        impl SlottedPageHeader for CustomHeader {
            fn base(&self) -> &SlottedPageBaseHeader {
                &self.base
            }
        }

        impl Default for CustomHeader {
            fn default() -> Self {
                Self {
                    base: SlottedPageBaseHeader {
                        co_db_magic_number: CO_DB_MAGIC_NUMBER,
                        total_free_space: (PAGE_SIZE - size_of::<CustomHeader>()) as u16,
                        contiguous_free_space: (PAGE_SIZE - size_of::<CustomHeader>()) as u16,
                        record_area_offset: PAGE_SIZE as u16,
                        header_size: size_of::<CustomHeader>() as u16,
                        first_free_block_offset: SlottedPageBaseHeader::NO_FREE_BLOCKS,
                        first_free_slot: SlottedPageBaseHeader::NO_FREE_SLOTS,
                        num_slots: 0,
                        page_type: PageTypeRepr::from(PageType::Generic),
                        flags: SlottedPageHeaderFlags::NO_FLAGS,
                    },
                    custom_field: 42,
                }
            }
        }

        let mut page = TestPage::new(PAGE_SIZE);
        let custom_header = CustomHeader::default();

        page.data_mut()[..size_of::<CustomHeader>()]
            .copy_from_slice(bytemuck::bytes_of(&custom_header));

        let slotted_page = SlottedPage::<TestPage, CustomHeader>::new(page).unwrap();

        let header: &CustomHeader = slotted_page.get_generic_header().unwrap();
        assert_eq!(header.custom_field, 42);
    }

    #[test]
    fn test_many_small_records() {
        let mut page = create_test_page(PAGE_SIZE);
        let mut inserted = Vec::new();

        let initial_free_space = page.free_space().unwrap();
        let initial_num_slots = page.num_slots().unwrap();

        // Insert many small records until page is full
        for i in 0..1000 {
            let data = format!("rec{}", i);
            let free_space_before = page.free_space().unwrap();
            let num_slots_before = page.num_slots().unwrap();

            match page.insert(data.as_bytes()).unwrap() {
                InsertResult::Success(slot_id) => {
                    inserted.push((slot_id, data.clone()));

                    let header = page.get_base_header().unwrap();

                    assert_eq!(header.num_slots, num_slots_before + 1);
                    assert_eq!(page.num_slots().unwrap(), num_slots_before + 1);

                    let expected_space_used = data.len() + size_of::<Slot>();
                    assert_eq!(
                        page.free_space().unwrap(),
                        free_space_before - expected_space_used as u16
                    );
                    assert_eq!(
                        header.total_free_space,
                        free_space_before - expected_space_used as u16
                    );

                    if i == 0 {
                        assert_eq!(header.first_free_slot, SlottedPageBaseHeader::NO_FREE_SLOTS);
                    }

                    assert_eq!(
                        header.header_size,
                        size_of::<SlottedPageBaseHeader>() as u16
                    );

                    let expected_free_space_start =
                        header.header_size + header.num_slots * Slot::SIZE as u16;
                    assert_eq!(header.free_space_start(), expected_free_space_start);
                }
                InsertResult::PageFull => {
                    let header = page.get_base_header().unwrap();
                    let required_space = data.len() + size_of::<Slot>();
                    assert!(header.total_free_space < required_space as u16);
                    break;
                }
                InsertResult::NeedsDefragmentation => {
                    let header = page.get_base_header().unwrap();
                    let required_space = data.len() + size_of::<Slot>();
                    assert!(header.total_free_space >= required_space as u16);
                    break;
                }
            }
        }

        let final_header = page.get_base_header().unwrap();
        let total_inserted = inserted.len();

        assert_eq!(
            final_header.num_slots,
            initial_num_slots + total_inserted as u16
        );

        let total_record_bytes: usize = inserted.iter().map(|(_, data)| data.len()).sum();
        let total_slot_bytes = total_inserted * size_of::<Slot>();
        let total_consumed = total_record_bytes + total_slot_bytes;

        assert_eq!(
            final_header.total_free_space,
            initial_free_space - total_consumed as u16
        );

        assert_eq!(
            final_header.header_size,
            size_of::<SlottedPageBaseHeader>() as u16
        );

        let expected_free_space_start =
            final_header.header_size + final_header.num_slots * Slot::SIZE as u16;
        assert_eq!(final_header.free_space_start(), expected_free_space_start);

        for (slot_id, expected_data) in inserted {
            assert_eq!(page.read_record(slot_id).unwrap(), expected_data.as_bytes());
        }
    }
    #[test]
    fn test_delete_single_record() {
        let mut page = create_test_page(PAGE_SIZE);
        let insert_value = b"record";
        page.insert(insert_value).unwrap();

        let header_before = page.get_base_header().unwrap();
        let free_space = header_before.total_free_space;
        assert_eq!(header_before.num_slots, 1);
        assert!(page.delete(0).is_ok());

        let header_after = page.get_base_header().unwrap();
        // checking if the logic for consuming free space if it is continuous works
        assert_eq!(
            header_after.first_free_block_offset,
            SlottedPageBaseHeader::NO_FREE_BLOCKS
        );

        assert_eq!(header_after.first_free_slot, 0);
        assert_eq!(
            header_after.total_free_space,
            free_space + insert_value.len() as u16
        );
        let slot = page.get_slot(0).unwrap();
        assert!(slot.is_deleted());
    }
    #[test]
    fn test_delete_multiple_records() {
        let mut page = create_test_page(PAGE_SIZE);
        page.insert(b"first").unwrap();
        page.insert(b"secondsecondsecondsecond").unwrap();
        page.insert(b"third").unwrap();

        page.delete(1).unwrap();

        let slot = page.get_slot(1).unwrap();
        assert!(slot.is_deleted());

        let header = page.get_base_header().unwrap();
        let expected_free_space = PAGE_SIZE as u16
            - header.header_size
            - header.num_slots * Slot::SIZE as u16
            - b"first".len() as u16
            - b"third".len() as u16;
        assert_eq!(header.total_free_space, expected_free_space);
        assert_ne!(
            header.first_free_block_offset,
            SlottedPageBaseHeader::NO_FREE_BLOCKS
        );
        assert_eq!(header.first_free_slot, 1);
    }

    #[test]
    fn test_delete_out_of_bounds() {
        let mut page = create_test_page(PAGE_SIZE);

        page.insert(b"first").unwrap();
        page.insert(b"second").unwrap();

        let result = page.delete(2);
        assert!(result.is_err());
        assert!(matches!(
            result.err().unwrap(),
            SlottedPageError::SlotIndexOutOfBounds {
                num_slots: 2,
                out_of_bounds_index: 2
            }
        ));
    }

    #[test]
    fn test_delete_already_deleted_record() {
        let mut page = create_test_page(64);
        page.insert(b"first").unwrap();
        page.insert(b"secondsecondseco").unwrap();
        page.insert(b"third").unwrap();

        page.delete(1).unwrap();

        let slot = page.get_slot(1).unwrap();
        assert!(slot.is_deleted());

        let result = page.delete(1);
        assert!(result.is_err());
        assert!(matches!(
            result.err().unwrap(),
            SlottedPageError::ForbiddenDeletedRecordAccess { slot_index: 1 }
        ));
    }

    #[test]
    fn test_delete_correctly_updates_free_blocks_and_free_slots() {
        let mut page = create_test_page(PAGE_SIZE);
        // Make the deleted records longer to trigger adding free blocks e.g. > 16 bytes
        page.insert(b"firstfirstfirstfirst").unwrap();
        page.insert(b"seconds").unwrap();
        page.insert(b"thirdthirdthirdthird").unwrap();
        page.insert(b"fourth").unwrap();
        // We delete the in between records so the page looks like free space -> fourth -> free block
        // -> second -> free block |.
        page.delete(0).unwrap();
        page.delete(2).unwrap();

        let deleted_slot_1 = page.get_slot(0).unwrap();
        let deleted_slot_2 = page.get_slot(2).unwrap();

        let header = page.get_base_header().unwrap();

        // Checking free slot list - should look like header -> 2 -> 0
        assert_eq!(header.first_free_slot, 2);
        assert_eq!(deleted_slot_2.next_free_slot(), 0);

        // Checking free block list - should look like header -> slot id 2 offset -> slot id 0 offset
        assert_eq!(
            header.first_free_block_offset,
            SlottedPage::<TestPage, SlottedPageBaseHeader>::calculate_free_block_aligned_offset(
                deleted_slot_2.offset
            )
        );

        let range = header.first_free_block_offset as usize
            ..header.first_free_block_offset as usize + FreeBlock::SIZE;
        let first_free_block = &page.page.data()[range];
        let free_block = bytemuck::from_bytes::<FreeBlock>(first_free_block);
        assert_eq!(
            free_block.next_block_offset,
            SlottedPage::<TestPage, SlottedPageBaseHeader>::calculate_free_block_aligned_offset(
                deleted_slot_1.offset
            )
        );
    }

    #[test]
    fn test_record_defragmentation() {
        let mut page = create_test_page(132);

        let record_1 = [1u8; 50];
        let record_2 = [2u8; 50];

        page.insert(&record_1).expect("Insert should succeed");
        page.insert(&record_2).expect("Insert should succeed");

        page.delete(0).unwrap();

        let header = page.get_base_header().unwrap();
        let before_defrag_continuous_space = header.contiguous_free_space;
        let before_defrag_free_space = header.total_free_space;

        assert!(before_defrag_free_space > 51);
        assert!(before_defrag_continuous_space < 51);

        let record_3 = [3u8; 51];

        let insert_result = page.insert(&record_3).unwrap();
        assert!(matches!(insert_result, InsertResult::NeedsDefragmentation));

        page.compact_records().unwrap();

        let header = page.get_base_header().unwrap();
        let continuous_space = header.contiguous_free_space;
        let free_space = header.total_free_space;

        assert_eq!(free_space, before_defrag_free_space);
        assert_eq!(continuous_space, free_space);

        let insert_result = page.insert(&record_3).unwrap();
        assert!(matches!(insert_result, InsertResult::Success(_)));
    }

    #[test]
    fn test_defragmentation_multiple_fragments() {
        let mut page = create_test_page(512);

        for i in 0..6 {
            let data = vec![i as u8; 30];
            page.insert(&data).unwrap();
        }

        page.delete(1).unwrap();
        page.delete(3).unwrap();
        page.delete(4).unwrap();

        let header_before = *page.get_base_header().unwrap();
        assert_ne!(
            header_before.first_free_block_offset,
            SlottedPageBaseHeader::NO_FREE_BLOCKS
        );

        page.compact_records().unwrap();

        let header_after = *page.get_base_header().unwrap();
        assert_eq!(
            header_after.first_free_block_offset,
            SlottedPageBaseHeader::NO_FREE_BLOCKS
        );
        assert_eq!(
            header_after.contiguous_free_space,
            header_after.total_free_space
        );
    }

    #[test]
    fn test_defragmentation_preserves_record_order() {
        let mut page = create_test_page(512);

        let records = [b"firstt", b"second", b"thirdt", b"fourth"];
        for record in &records {
            page.insert(*record).unwrap();
        }

        page.delete(1).unwrap();
        page.delete(2).unwrap();

        page.compact_records().unwrap();

        assert_eq!(page.read_record(0).unwrap(), b"firstt");
        assert_eq!(page.read_record(3).unwrap(), b"fourth");

        assert!(page.read_record(1).is_err());
        assert!(page.read_record(2).is_err());
    }

    #[test]
    fn test_defragmentation_all_records_deleted() {
        let mut page = create_test_page(512);

        page.insert(b"record1").unwrap();
        page.insert(b"record2").unwrap();

        page.delete(0).unwrap();
        page.delete(1).unwrap();

        page.compact_records().unwrap();

        let header = page.get_base_header().unwrap();
        let expected_free = 512 - header.header_size - (2 * size_of::<Slot>() as u16);
        assert_eq!(header.contiguous_free_space, expected_free);
    }

    #[test]
    fn test_defragmentation_no_deleted_records() {
        let mut page = create_test_page(512);

        page.insert(b"record1").unwrap();
        page.insert(b"record2").unwrap();

        let header_before = *page.get_base_header().unwrap();
        page.compact_records().unwrap();
        let header_after = *page.get_base_header().unwrap();

        assert_eq!(
            header_before.contiguous_free_space,
            header_after.contiguous_free_space
        );
        assert_eq!(
            header_before.record_area_offset,
            header_after.record_area_offset
        );
    }

    #[test]
    fn test_slot_compaction_removes_deleted_slots() {
        let mut page = create_test_page(PAGE_SIZE);

        page.insert(b"keep1").unwrap(); // slot 0
        page.insert(b"delete1").unwrap(); // slot 1
        page.insert(b"keep2").unwrap(); // slot 2
        page.insert(b"delete2").unwrap(); // slot 3
        page.insert(b"keep3").unwrap(); // slot 4

        page.delete(1).unwrap();
        page.delete(3).unwrap();

        assert_eq!(page.num_slots().unwrap(), 5);

        page.compact_slots().unwrap();

        assert_eq!(page.num_slots().unwrap(), 3);

        assert_eq!(page.read_record(0).unwrap(), b"keep1");
        assert_eq!(page.read_record(1).unwrap(), b"keep2");
        assert_eq!(page.read_record(2).unwrap(), b"keep3");
    }

    #[test]
    fn test_slot_compaction_reclaims_slot_space() {
        let mut page = create_test_page(PAGE_SIZE);

        for i in 0..10 {
            let data = format!("record{}", i);
            page.insert(data.as_bytes()).unwrap();
        }

        // Delete every other slot
        for i in (1..10).step_by(2) {
            page.delete(i).unwrap();
        }

        let free_space_before = page.free_space().unwrap();
        let slots_before = page.num_slots().unwrap();

        page.compact_slots().unwrap();

        let free_space_after = page.free_space().unwrap();
        let slots_after = page.num_slots().unwrap();

        let reclaimed_space = (slots_before - slots_after) * size_of::<Slot>() as u16;
        assert_eq!(free_space_after, free_space_before + reclaimed_space);

        let header = page.get_base_header().unwrap();
        assert_eq!(header.first_free_slot, SlottedPageBaseHeader::NO_FREE_SLOTS);
    }

    #[test]
    fn test_slot_compaction_preserves_relative_order() {
        let mut page = create_test_page(PAGE_SIZE);

        let records = [b"first2", b"second", b"third2", b"fourth", b"fifth2"];
        for record in &records {
            page.insert(*record).unwrap();
        }

        page.delete(1).unwrap();
        page.delete(3).unwrap();

        page.compact_slots().unwrap();

        assert_eq!(page.read_record(0).unwrap(), b"first2");
        assert_eq!(page.read_record(1).unwrap(), b"third2");
        assert_eq!(page.read_record(2).unwrap(), b"fifth2");
    }

    #[test]
    fn test_update_record_will_not_fit() {
        let mut page = create_test_page(64);
        page.insert(b"tiny").unwrap();

        let result = page.update(0, &[0u8; 100]).unwrap();
        assert!(matches!(result, UpdateResult::PageFull));
    }

    #[test]
    fn test_update_smaller_record() {
        let mut page = create_test_page(PAGE_SIZE);
        page.insert(b"original").unwrap();

        let old_total_free_space = page.free_space().unwrap();

        let result = page.update(0, b"small").unwrap();
        assert!(matches!(result, UpdateResult::Success));

        let slot = page.get_slot(0).unwrap();
        assert_eq!(slot.len, b"small".len() as u16);

        let record = page.read_record(0).unwrap();
        assert_eq!(record, b"small");

        let expected_free_space =
            old_total_free_space + (b"original".len() as u16 - b"small".len() as u16);
        assert_eq!(page.free_space().unwrap(), expected_free_space);
    }

    #[test]
    fn test_update_equal_size() {
        let mut page = create_test_page(PAGE_SIZE);
        page.insert(b"original").unwrap();

        let old_total_free_space = page.free_space().unwrap();

        let result = page.update(0, b"0riginal").unwrap();
        assert!(matches!(result, UpdateResult::Success));

        let slot = page.get_slot(0).unwrap();
        assert_eq!(slot.len, b"0riginal".len() as u16);

        let record = page.read_record(0).unwrap();
        assert_eq!(record, b"0riginal");

        assert_eq!(page.free_space().unwrap(), old_total_free_space);
    }

    #[test]
    fn test_update_shrink_creates_free_block() {
        let mut page = create_test_page(PAGE_SIZE);

        page.insert(b"very_long_record_that_will_create_free_block")
            .unwrap();
        let original_offset = page.get_slot(0).unwrap().offset;
        let free_space_before = page.free_space().unwrap();

        let result = page.update(0, b"short").unwrap();
        assert!(matches!(result, UpdateResult::Success));

        let header = page.get_base_header().unwrap();
        assert_ne!(
            header.first_free_block_offset,
            SlottedPageBaseHeader::NO_FREE_BLOCKS
        );

        let slot = page.get_slot(0).unwrap();
        assert_eq!(slot.offset, original_offset);
        assert_eq!(slot.len, 5);

        assert!(page.free_space().unwrap() > free_space_before);
    }

    #[test]
    fn test_update_grow_with_available_space() {
        let mut page = create_test_page(PAGE_SIZE);

        page.insert(&[1u8; 17]).unwrap();
        let original_slot = *page.get_slot(0).unwrap();

        let new_record = b"much_longer_record_that_needs_more_space_and_will_create_a_free_block";
        let result = page.update(0, new_record).unwrap();
        assert!(matches!(result, UpdateResult::Success));

        let updated_slot = *page.get_slot(0).unwrap();
        assert_ne!(updated_slot.offset, original_slot.offset);
        assert_eq!(updated_slot.len, new_record.len() as u16);

        let header = page.get_base_header().unwrap();
        assert_ne!(
            header.first_free_block_offset,
            SlottedPageBaseHeader::NO_FREE_BLOCKS
        );

        assert_eq!(page.read_record(0).unwrap(), new_record);
    }

    #[test]
    fn test_update_needs_defragmentation() {
        let mut page = create_test_page(160);

        page.insert(b"record1").unwrap();
        page.insert(b"large_record_to_create_fragment").unwrap();
        page.insert(b"record3").unwrap();

        page.delete(1).unwrap();

        let large_update = vec![b'x'; 100];
        let result = page.update(0, &large_update).unwrap();

        assert!(matches!(result, UpdateResult::NeedsDefragmentation));

        assert_eq!(page.read_record(0).unwrap(), b"record1");
    }
    #[test]
    fn test_update_deleted_record_error() {
        let mut page = create_test_page(PAGE_SIZE);

        page.insert(b"test_record").unwrap();
        page.delete(0).unwrap();

        let result = page.update(0, b"new_data");
        assert!(matches!(
            result,
            Err(SlottedPageError::ForbiddenDeletedRecordAccess { slot_index: 0 })
        ));
    }

    #[test]
    fn test_defragment_and_update_actually_needs_defrag() {
        let mut page = create_test_page(290); // Much smaller page

        // Fill most of the page with records
        let records = [
            vec![1u8; 60], // slot 0
            vec![2u8; 60], // slot 1
            vec![3u8; 60], // slot 2
            vec![4u8; 60], // slot 3
        ];

        for record in &records {
            page.insert(record).unwrap();
        }

        // Delete middle records to create 120 bytes of fragmented space
        page.delete(1).unwrap();
        page.delete(2).unwrap();

        // Try to update slot 0 to something that needs fragmented space
        let large_update = vec![b'X'; 100]; // Needs more than contiguous space

        // Regular update should fail
        let update_result = page.update(0, &large_update).unwrap();
        assert!(matches!(update_result, UpdateResult::NeedsDefragmentation));

        // Defragment and update should succeed
        let defrag_result = page.defragment_and_update(0, &large_update).unwrap();
        assert!(matches!(defrag_result, UpdateResult::Success));
    }
}

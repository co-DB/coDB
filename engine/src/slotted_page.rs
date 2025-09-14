use crate::cache::{PageRead, PageWrite};
use crate::slotted_page::SlottedPageError::ForbiddenSlotCompaction;
use bytemuck::{Pod, PodCastError, Zeroable};
use thiserror::Error;

/// Helper trait meant for structs implementing SlottedPageHeader trait. Making implementing it
/// compulsory should help remind to use #[repr(C)] for those structs (there is no way to ensure
/// that it is used otherwise)
unsafe trait ReprC {}

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
}
impl FreeBlock {
    /// A minimal size needed for a free block to be created. If there is less empty space leftover
    /// than this value we don't create a free block and don't use this space until it is reclaimed
    /// during record compaction.
    pub const MIN_SIZE: u16 = 16;

    pub const SIZE: usize = size_of::<FreeBlock>();
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
    first_free_slot: u16,
    /// Total number of slots in the slots directory (including used and free slots).
    num_slots: u16,
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
}

impl SlottedPageHeader for SlottedPageBaseHeader {
    fn base(&self) -> &SlottedPageBaseHeader {
        self
    }
}

unsafe impl ReprC for SlottedPageBaseHeader {}

/// Enum representing possible outcomes of an insert operation.
pub(crate) enum InsertResult {
    /// The insert succeeded and id of the slot referencing the inserted record is returned.
    Success(u16),
    /// There is not enough space in neither free blocks nor contiguous free space, but if the
    /// page was defragmented the record would fit.
    NeedsDefragmentation,
    /// The page is full and won't fit a record of this length.
    PageFull,
}

#[derive(Debug, Error)]
pub enum SlottedPageError {
    #[error(
        "tried to access record at index {out_of_bounds_index} while there were only {num_slots} records"
    )]
    RecordIndexOutOfBounds {
        num_slots: u16,
        out_of_bounds_index: u16,
    },
    #[error("tried to modify slot at position {position} while there were only {num_slots} slots")]
    InvalidPosition { num_slots: u16, position: u16 },
    #[error("tried to access deleted record with slot index {slot_index}")]
    TriedToAccessDeletedRecord { slot_index: u16 },
    #[error("tried to compact slots even though allow_slot_compaction was set to false")]
    ForbiddenSlotCompaction,
    #[error("casting data from bytes failed for the following reason: {reason}")]
    CastError { reason: String },
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
    pub fn set_next_free_slot(&mut self, next_free_slot_index: u16) {
        // Ox7FFF here is the max value of the last 15 bits used for holding slot index
        let next = next_free_slot_index & 0x7FFF;
        let deleted_flag = self.flags & Self::SLOT_DELETED;
        self.flags = deleted_flag | next;
    }

    /// Returns the index of the next free slot (lower 15 bits of `flags`).
    pub fn next_free_slot(&self) -> u16 {
        // Ox7FFF here is the max value of the last 15 bits used for holding slot index
        self.flags & 0x7FFF
    }
}

/// A wrapper around a page, that allows higher level abstractions to interact with structured
/// page data without dealing directly with raw bytes. Exposes typical slotted page methods like
/// read,insert,update,delete and also slot/record compaction.
///
/// A visual for how a slotted page looks can be seen in slotted_page.png in docs directory of this
/// crate.
pub(crate) struct SlottedPage<P> {
    /// The underlying page, with which the slotted page interacts.
    page: P,
    /// Controls whether slots within the page can be compacted to reclaim space after deletions.
    /// Safeguards the compact_slots method which should not be called by some higher level abstractions
    /// (e.g. heap file slotted pages).
    allow_slot_compaction: bool,
}

/// Implementation for read-only slotted page
impl<P: PageRead> SlottedPage<P> {
    /// Creates a new SlottedPage wrapper around a page
    pub fn new(page: P, allow_slot_compaction: bool) -> Self {
        Self {
            page,
            allow_slot_compaction,
        }
    }

    /// Generic method to cast page header to any type implementing SlottedPageHeader
    ///
    /// Caller must ensure T matches the actual header type stored in the page
    pub fn get_header<T>(&self) -> Result<&T, SlottedPageError>
    where
        T: SlottedPageHeader,
    {
        Ok(bytemuck::try_from_bytes(
            &self.page.data()[..size_of::<T>()],
        )?)
    }

    /// Gets a reference to the base header (common to all slotted pages).
    fn get_base_header(&self) -> Result<&SlottedPageBaseHeader, SlottedPageError> {
        self.get_header::<SlottedPageBaseHeader>()
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
    fn get_slot(&self, slot_idx: u16) -> Result<&Slot, SlottedPageError> {
        let header = self.get_base_header()?;

        if slot_idx >= header.num_slots {
            return Err(SlottedPageError::RecordIndexOutOfBounds {
                num_slots: header.num_slots,
                out_of_bounds_index: slot_idx,
            });
        }

        let slots = self.get_slots()?;
        Ok(&slots[slot_idx as usize])
    }

    /// Reads the record data for a given slot. Doesn't check if the record is deleted (since
    /// we use deleted records in binary search for b-tree). For safe access use read_valid_record
    pub fn read_record(&self, slot_idx: u16) -> Result<&[u8], SlottedPageError> {
        let slot = self.get_slot(slot_idx)?;

        let record_start = slot.offset as usize;
        let record_end = record_start + slot.len as usize;

        Ok(&self.page.data()[record_start..record_end])
    }

    /// Reads the record data for a given slot. Checks if the record is deleted . Safe version of
    /// read_record
    pub fn read_valid_record(&self, slot_idx: u16) -> Result<&[u8], SlottedPageError> {
        let slot = self.get_slot(slot_idx)?;

        if slot.is_deleted() {
            return Err(SlottedPageError::TriedToAccessDeletedRecord {
                slot_index: slot_idx,
            });
        }

        let record_start = slot.offset as usize;
        let record_end = record_start + slot.len as usize;

        Ok(&self.page.data()[record_start..record_end])
    }
}

impl<P: PageWrite + PageRead> SlottedPage<P> {
    /// Generic method to cast page header to any type implementing SlottedPageHeader
    ///
    /// Caller must ensure T matches the actual header type stored in the page
    pub fn get_header_mut<T>(&mut self) -> Result<&mut T, SlottedPageError>
    where
        T: SlottedPageHeader,
    {
        Ok(bytemuck::try_from_bytes_mut(
            &mut self.page.data_mut()[..size_of::<T>()],
        )?)
    }

    /// Gets a mutable reference to base header (common to all slotted pages).
    fn get_base_header_mut(&mut self) -> Result<&mut SlottedPageBaseHeader, SlottedPageError> {
        self.get_header_mut::<SlottedPageBaseHeader>()
    }

    /// Inserts a record at a given slot position.
    ///
    /// Shifts existing slots to the right if necessary, allocates space for the record,
    /// writes it, and updates the header.
    pub fn insert_at(
        &mut self,
        record: &[u8],
        position: u16,
    ) -> Result<InsertResult, SlottedPageError> {
        let header = self.get_base_header()?;

        if position > header.num_slots {
            return Err(SlottedPageError::InvalidPosition {
                position,
                num_slots: header.num_slots,
            });
        }

        let required_space = record.len() + Slot::SIZE;

        if required_space as u16 > header.total_free_space {
            return Ok(InsertResult::PageFull);
        }

        let offset = match self.get_allocated_space(record.len() as u16, true)? {
            None => return Ok(InsertResult::NeedsDefragmentation),
            Some(offset) => offset,
        };

        self.write_record_at(record, offset);

        self.shift_slots_right(position)?;

        self.write_slot_at(position, Slot::new(offset, record.len() as u16))?;

        let header_mut = self.get_base_header_mut()?;
        header_mut.num_slots += 1;
        header_mut.total_free_space -= Slot::SIZE as u16;
        header_mut.contiguous_free_space -= Slot::SIZE as u16;

        Ok(InsertResult::Success(position))
    }

    /// Writes a slot to the slot directory at a given position.
    fn write_slot_at(&mut self, position: u16, slot: Slot) -> Result<(), SlottedPageError> {
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
    fn shift_slots_right(&mut self, position: u16) -> Result<(), SlottedPageError> {
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
    fn append_slot(&mut self, slot: Slot) -> Result<u16, SlottedPageError> {
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
    fn reuse_or_append_slot(&mut self, slot: Slot) -> Result<u16, SlottedPageError> {
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

            // If leftover after insertion is too small, consume entire block.
            if block.len.saturating_sub(record_len) < FreeBlock::MIN_SIZE {
                self.get_base_header_mut()?.total_free_space -= block.len;
                self.update_freeblock_chain(prev_offset, block.next_block_offset)?;
                return Ok(Some(offset));
            }

            // Otherwise, split block into record + smaller free block.
            let consumed_space = record_len + FreeBlock::SIZE as u16;
            self.get_base_header_mut()?.total_free_space -= consumed_space;

            let new_freeblock_offset = offset + record_len;
            let new_freeblock = FreeBlock {
                next_block_offset: block.next_block_offset,
                len: block.len - record_len - FreeBlock::SIZE as u16,
            };
            self.page.data_mut()
                [new_freeblock_offset as usize..new_freeblock_offset as usize + FreeBlock::SIZE]
                .copy_from_slice(bytemuck::bytes_of(&new_freeblock));

            self.update_freeblock_chain(prev_offset, new_freeblock_offset)?;
            return Ok(Some(offset));
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

    /// Finds the first free block large enough for `record_len`.
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
    fn get_slot_mut(&mut self, slot_idx: u16) -> Result<&mut Slot, SlottedPageError> {
        let header = self.get_base_header()?;
        let start = header.header_size as usize + slot_idx as usize * Slot::SIZE;
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
        header.first_free_slot = Slot::NO_FREE_SLOTS;
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
    fn create_test_page(size: usize) -> SlottedPage<TestPage> {
        let mut page = TestPage::new(size);

        let header = SlottedPageBaseHeader {
            total_free_space: (size - size_of::<SlottedPageBaseHeader>()) as u16,
            contiguous_free_space: (size - size_of::<SlottedPageBaseHeader>()) as u16,
            record_area_offset: size as u16,
            header_size: size_of::<SlottedPageBaseHeader>() as u16,
            first_free_block_offset: SlottedPageBaseHeader::NO_FREE_BLOCKS,
            first_free_slot: SlottedPageBaseHeader::NO_FREE_SLOTS,
            num_slots: 0,
        };

        page.data_mut()[..size_of::<SlottedPageBaseHeader>()]
            .copy_from_slice(bytemuck::bytes_of(&header));

        SlottedPage::new(page, true)
    }

    #[test]
    fn test_insert_and_read() {
        let mut page = create_test_page(PAGE_SIZE);

        let data = b"hello world";
        let result = page.insert(data).unwrap();

        assert!(matches!(result, InsertResult::Success(0)));
        assert_eq!(page.read_valid_record(0).unwrap(), data);
        assert_eq!(page.num_slots().unwrap(), 1);
    }

    #[test]
    fn test_multiple_inserts() {
        let mut page = create_test_page(PAGE_SIZE);

        let records = [b"first", b"secon", b"third"];

        for (i, record) in records.iter().enumerate() {
            let result = page.insert(*record).unwrap();
            assert!(matches!(result, InsertResult::Success(_)));
            assert_eq!(page.read_valid_record(i as u16).unwrap(), *record);
        }

        assert_eq!(page.num_slots().unwrap(), 3);
    }

    #[test]
    fn test_large_record() {
        let mut page = create_test_page(PAGE_SIZE);

        let large_data = vec![42u8; 1000];
        let result = page.insert(&large_data).unwrap();

        assert!(matches!(result, InsertResult::Success(0)));
        assert_eq!(page.read_valid_record(0).unwrap(), large_data.as_slice());
    }

    #[test]
    fn test_insert_at_beginning() {
        let mut page = create_test_page(PAGE_SIZE);

        page.insert(b"second").unwrap();

        let result = page.insert_at(b"first", 0).unwrap();
        assert!(matches!(result, InsertResult::Success(0)));

        assert_eq!(page.read_valid_record(0).unwrap(), b"first");
        assert_eq!(page.read_valid_record(1).unwrap(), b"second");
        assert_eq!(page.num_slots().unwrap(), 2);
    }

    #[test]
    fn test_insert_at_middle() {
        let mut page = create_test_page(PAGE_SIZE);

        page.insert(b"first").unwrap();
        page.insert(b"third").unwrap();

        let result = page.insert_at(b"second", 1).unwrap();
        assert!(matches!(result, InsertResult::Success(1)));

        assert_eq!(page.read_valid_record(0).unwrap(), b"first");
        assert_eq!(page.read_valid_record(1).unwrap(), b"second");
        assert_eq!(page.read_valid_record(2).unwrap(), b"third");
    }

    #[test]
    fn test_insert_at_end() {
        let mut page = create_test_page(PAGE_SIZE);

        page.insert(b"first").unwrap();

        let result = page.insert_at(b"second", 1).unwrap();
        assert!(matches!(result, InsertResult::Success(1)));

        assert_eq!(page.read_valid_record(1).unwrap(), b"second");
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
        let mut page = create_test_page(4096);
        let result = page.insert(b"").unwrap();
        assert!(matches!(result, InsertResult::Success(0)));
        assert_eq!(page.read_valid_record(0).unwrap(), b"");
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
        let page = create_test_page(4096);

        let result = page.read_valid_record(0);
        assert!(matches!(
            result,
            Err(SlottedPageError::RecordIndexOutOfBounds {
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

        let mut page = TestPage::new(4096);
        let custom_header = CustomHeader {
            base: SlottedPageBaseHeader {
                total_free_space: (4096 - size_of::<CustomHeader>()) as u16,
                contiguous_free_space: (4096 - size_of::<CustomHeader>()) as u16,
                record_area_offset: 4096,
                header_size: size_of::<CustomHeader>() as u16,
                first_free_block_offset: SlottedPageBaseHeader::NO_FREE_BLOCKS,
                first_free_slot: SlottedPageBaseHeader::NO_FREE_SLOTS,
                num_slots: 0,
            },
            custom_field: 42,
        };

        page.data_mut()[..size_of::<CustomHeader>()]
            .copy_from_slice(bytemuck::bytes_of(&custom_header));

        let slotted_page = SlottedPage::new(page, true);

        let header: &CustomHeader = slotted_page.get_header().unwrap();
        assert_eq!(header.custom_field, 42);
    }

    #[test]
    fn test_many_small_records() {
        let mut page = create_test_page(4096);
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
            assert_eq!(
                page.read_valid_record(slot_id).unwrap(),
                expected_data.as_bytes()
            );
        }
    }
}

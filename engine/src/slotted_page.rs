use crate::cache::{PageRead, PageWrite};
use bytemuck::{Pod, PodCastError, Zeroable};
use thiserror::Error;

/// Struct responsible for storing metadata of a free block. We store it at the start of each free
/// block.
///
/// A free block is a way of allowing slotted page to reuse the empty space left after delete or
/// update operations. We store offset to the first free block inside the page header, and then we
/// can follow offsets inside each free block to get to the next ones.
#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub(crate) struct FreeBlock {
    /// Length of the whole block, including both the empty space and the space taken by storing this
    /// struct
    len: u16,
    /// Offset inside the page to the next freeblock
    next_block_offset: u16,
}
impl FreeBlock {
    /// A minimal size needed for a free block to be created. If the empty space is less than this
    /// value we don't create a free block and don't use this space until it is reclaimed during
    /// record compacting.
    pub const MIN_SIZE: u16 = 16;
}

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub(crate) struct SlottedPageBaseHeader {
    free_space_size: u16,
    free_contiguous_size: u16,
    free_space_offset: u16,
    header_size: u16,
    first_freeblock_offset: u16,
    first_free_slot: u16,
    num_slots: u16,
}

impl SlottedPageBaseHeader {
    pub fn free_space_start(&self) -> u16 {
        self.header_size + self.num_slots * size_of::<Slot>() as u16
    }
}
pub(crate) enum InsertResult {
    Success(u16),
    NeedsDefragmentation,
    PageFull,
}

#[derive(Debug, Error)]
pub enum SlottedPageError {
    #[error(
        "tried to access record at index: {out_of_bounds_index} while there were only {num_slots} records"
    )]
    IndexOutOfBounds {
        num_slots: u16,
        out_of_bounds_index: u16,
    },
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
pub struct SlottedPage<P> {
    page: P,
}

impl<P: PageRead> SlottedPage<P> {
    pub fn new(page: P) -> Self {
        Self { page }
    }

    pub fn get_header<T>(&self) -> Result<&T, SlottedPageError>
    where
        T: SlottedPageHeader,
    {
        Ok(bytemuck::try_from_bytes(
            &self.page.data()[..size_of::<T>()],
        )?)
    }
    fn get_base_header(&self) -> Result<&SlottedPageBaseHeader, SlottedPageError> {
        self.get_header::<SlottedPageBaseHeader>()
    }

    pub fn num_slots(&self) -> Result<u16, SlottedPageError> {
        let header = self.get_base_header()?;
        Ok(header.num_slots)
    }

    pub fn is_empty(&self) -> Result<bool, SlottedPageError> {
        Ok(self.num_slots()?.eq(&0))
    }

    pub fn free_space(&self) -> Result<u16, SlottedPageError> {
        let header = self.get_base_header()?;
        Ok(header.free_space_size)
    }
    fn get_slots(&self) -> Result<&[Slot], SlottedPageError> {
        let header = self.get_base_header()?;
        let start = header.header_size as usize;
        let end = start + (header.num_slots as usize * size_of::<Slot>());
        Ok(bytemuck::try_cast_slice(&self.page.data()[start..end])?)
    }

    fn get_slot(&self, slot_idx: u16) -> Result<&Slot, SlottedPageError> {
        let header = self.get_base_header()?;
        if header.num_slots > slot_idx {
            return Err(SlottedPageError::IndexOutOfBounds {
                num_slots: header.num_slots,
                out_of_bounds_index: slot_idx,
            });
        }
        let start = header.header_size as usize;
        let end = start + (header.num_slots as usize * size_of::<Slot>());
        Ok(bytemuck::from_bytes(&self.page.data()[start..end]))
    }
    pub fn read_record(&self, slot_idx: u16) -> Result<&[u8], SlottedPageError> {
        let num_slots = self.num_slots()?;
        if slot_idx >= num_slots {
            return Err(SlottedPageError::IndexOutOfBounds {
                num_slots,
                out_of_bounds_index: slot_idx,
            });
        }
        let slot = &self.get_slots()?[slot_idx as usize];
        Ok(&self.page.data()[slot.offset as usize..slot.offset as usize + slot.len as usize])
    }
}

#[derive(Pod, Zeroable, Clone, Copy)]
#[repr(C)]
pub struct Slot {
    pub offset: u16,
    pub len: u16,
    /// This is for storing information about deleted status + free slot offset for heap files
    pub flags: u16,
}

impl Slot {
    const NO_FREE_SLOTS: u16 = 0x7FFF;
    pub fn new(offset: u16, len: u16) -> Self {
        Self {
            offset,
            len,
            flags: Self::NO_FREE_SLOTS,
        }
    }

    pub fn is_deleted(&self) -> bool {
        self.flags & 0x8000 != 0
    }

    pub fn mark_deleted(&mut self) {
        self.flags |= 0x8000;
    }

    pub fn set_next_free_slot(&mut self, next_free_slot_offset: u16) {
        let next = next_free_slot_offset & 0x7FFF;
        let deleted_flag = self.flags & 0x8000;
        self.flags = deleted_flag | next;
    }

    pub fn next_free_slot(&self) -> u16 {
        self.flags & 0x7FFF
    }
}

impl<P: PageWrite + PageRead> SlottedPage<P> {
    pub fn get_header_mut<T>(&mut self) -> Result<&mut T, SlottedPageError>
    where
        T: SlottedPageHeader + Pod,
    {
        Ok(bytemuck::try_from_bytes_mut(
            &mut self.page.data_mut()[..size_of::<T>()],
        )?)
    }

    fn get_base_header_mut(&mut self) -> Result<&mut SlottedPageBaseHeader, SlottedPageError> {
        self.get_header_mut::<SlottedPageBaseHeader>()
    }
    pub fn insert_record(&mut self, record: &[u8]) -> Result<InsertResult, SlottedPageError> {
        let header = self.get_base_header()?;

        let needs_new_slot = header.first_free_slot == u16::MAX;

        let required_space = record.len() + if needs_new_slot { size_of::<Slot>() } else { 0 };

        if required_space as u16 > header.free_space_size {
            return Ok(InsertResult::PageFull);
        }

        let offset = match self.get_allocated_space(record.len() as u16, needs_new_slot)? {
            None => return Ok(InsertResult::NeedsDefragmentation),
            Some(offset) => offset,
        };

        self.write_record_at(record, offset);

        let slot = Slot::new(offset, record.len() as u16);

        let slot_id = self.reuse_or_append_slot(slot)?;
        Ok(InsertResult::Success(slot_id))
    }

    fn append_slot(&mut self, slot: Slot) -> Result<u16, SlottedPageError> {
        let free_space_start = self.get_base_header()?.free_space_start();
        self.page.data_mut()
            [free_space_start as usize..free_space_start as usize + size_of::<Slot>()]
            .copy_from_slice(bytemuck::bytes_of(&slot));
        let header = self.get_base_header_mut()?;
        let slot_id = header.num_slots;
        header.num_slots += 1;
        header.free_contiguous_size -= size_of::<Slot>() as u16;
        header.free_space_size -= size_of::<Slot>() as u16;
        Ok(slot_id)
    }

    fn reuse_or_append_slot(&mut self, slot: Slot) -> Result<u16, SlottedPageError> {
        let header = self.get_base_header()?;
        let free_slot_id = header.first_free_slot;

        if free_slot_id == u16::MAX {
            return self.append_slot(slot);
        }

        let free_slot = self.get_slot(free_slot_id)?;
        let next_free_slot_id = free_slot.next_free_slot();

        let start = header.header_size as usize + free_slot_id as usize * size_of::<Slot>();
        let end = start + size_of::<Slot>();
        self.page.data_mut()[start..end].copy_from_slice(bytemuck::bytes_of(&slot));

        self.get_base_header_mut()?.first_free_slot = next_free_slot_id;

        Ok(free_slot_id)
    }

    fn write_record_at(&mut self, record: &[u8], offset: u16) {
        let start = offset as usize;
        let end = start + record.len();
        self.page.data_mut()[start..end].copy_from_slice(record)
    }

    fn get_allocated_space(
        &mut self,
        record_len: u16,
        needs_new_slot: bool,
    ) -> Result<Option<u16>, SlottedPageError> {
        let header = self.get_base_header_mut()?;
        let slot_size = if needs_new_slot {
            size_of::<Slot>() as u16
        } else {
            0
        };

        if header.free_contiguous_size >= record_len + slot_size {
            header.free_contiguous_size -= record_len;
            header.free_space_size -= record_len;
            header.free_space_offset -= record_len;
            return Ok(Some(header.free_space_offset));
        }

        if needs_new_slot && header.free_contiguous_size < slot_size {
            return Ok(None);
        }

        if let Some((prev_offset, offset)) = self.find_free_space(record_len)? {
            let block = *bytemuck::try_from_bytes::<FreeBlock>(
                &self.page.data()[offset as usize..offset as usize + size_of::<FreeBlock>()],
            )?;

            if block.len.saturating_sub(record_len) < FreeBlock::MIN_SIZE {
                self.get_base_header_mut()?.free_space_size -= block.len;
                self.update_freeblock_chain(prev_offset, block.next_block_offset)?;
                return Ok(Some(offset));
            }

            // Here we handle the case where the freeblock has at least 16 bytes of space
            // leftover after inserting the record, so we add another free block
            let consumed_space = record_len + size_of::<FreeBlock>() as u16;
            self.get_base_header_mut()?.free_space_size -= consumed_space;

            let new_freeblock_offset = offset + record_len;
            let new_freeblock = FreeBlock {
                next_block_offset: block.next_block_offset,
                len: block.len - record_len - size_of::<FreeBlock>() as u16,
            };
            self.page.data_mut()[new_freeblock_offset as usize
                ..new_freeblock_offset as usize + size_of::<FreeBlock>()]
                .copy_from_slice(bytemuck::bytes_of(&new_freeblock));

            self.update_freeblock_chain(prev_offset, new_freeblock_offset)?;
            return Ok(Some(offset));
        }

        Ok(None)
    }

    fn update_freeblock_chain(
        &mut self,
        prev_offset: u16,
        new_next: u16,
    ) -> Result<(), SlottedPageError> {
        if prev_offset == u16::MAX {
            self.get_base_header_mut()?.first_freeblock_offset = new_next;
        } else {
            let prev = bytemuck::try_from_bytes_mut::<FreeBlock>(
                &mut self.page.data_mut()
                    [prev_offset as usize..prev_offset as usize + size_of::<FreeBlock>()],
            )?;
            prev.next_block_offset = new_next;
        }
        Ok(())
    }
    fn find_free_space(&self, record_len: u16) -> Result<Option<(u16, u16)>, SlottedPageError> {
        let header = self.get_base_header()?;

        let mut block_offset = header.first_freeblock_offset;
        let mut prev_offset = u16::MAX;
        while block_offset != u16::MAX {
            let start = block_offset as usize;
            let end = start + size_of::<FreeBlock>();
            let freeblock = bytemuck::try_from_bytes::<FreeBlock>(&self.page.data()[start..end])?;
            if freeblock.len >= record_len {
                return Ok(Some((prev_offset, block_offset)));
            }
            prev_offset = block_offset;
            block_offset = freeblock.next_block_offset;
        }
        Ok(None)
    }

    fn get_slots_mut(&mut self) -> Result<&mut [Slot], SlottedPageError> {
        let header = self.get_base_header()?;
        let start = header.header_size as usize;
        let end = start + (header.num_slots as usize * size_of::<Slot>());
        Ok(bytemuck::try_cast_slice_mut(
            &mut self.page.data_mut()[start..end],
        )?)
    }

    fn get_slot_mut(&mut self, slot_idx: usize) -> Result<&mut Slot, SlottedPageError> {
        let header = self.get_base_header()?;
        let start = header.header_size as usize + slot_idx as usize * size_of::<Slot>();
        let end = start + size_of::<Slot>();
        Ok(bytemuck::try_from_bytes_mut(
            &mut self.page.data_mut()[start..end],
        )?)
    }

    /// Removes deleted slots and pushes the filled ones together. Keeps the order of slots intact and
    /// frees space.
    ///
    /// This is for B-Tree nodes only as there slots placement doesn't matter since they are never
    /// referenced from outside the node. For heap file pages the slot ids matter and compacting
    /// would change and thus invalidate the external references to them.
    pub fn compact_slots(&mut self) -> Result<(), SlottedPageError> {
        let header_size = self.get_base_header()?.header_size as usize;

        let slots = self.get_slots()?;
        let filled_slots: Vec<usize> = slots
            .iter()
            .enumerate()
            .filter_map(|(idx, slot)| (!slot.is_deleted()).then_some(idx))
            .collect();

        let filled_count = filled_slots.len();
        let freed_space = (slots.len() - filled_count) * size_of::<Slot>();

        let page = self.page.data_mut();

        for (dst_i, src_i) in filled_slots.into_iter().enumerate() {
            let copy_location = header_size + dst_i * size_of::<Slot>();
            let src_start = header_size + src_i * size_of::<Slot>();
            let src_end = src_start + size_of::<Slot>();

            page.copy_within(src_start..src_end, copy_location);
        }

        let header = self.get_base_header_mut()?;
        header.num_slots = filled_count as u16;
        header.free_space_size += freed_space as u16;
        header.free_contiguous_size += freed_space as u16;
        header.first_free_slot = Slot::NO_FREE_SLOTS;
        Ok(())
    }

    pub fn compact_records(&mut self) -> Result<(), SlottedPageError> {
        let mut write_pos = self.page.data().len();

        let mut slots: Vec<(usize, Slot)> = self
            .get_slots()?
            .iter()
            .enumerate()
            .filter_map(|(idx, slot)| (!slot.is_deleted()).then_some((idx, *slot)))
            .collect();

        slots.sort_by_key(|(_, slot)| -(slot.offset as i32));
        for (idx, slot) in slots {
            write_pos -= slot.len as usize;
            let record_range = slot.offset as usize..slot.offset as usize + slot.len as usize;
            self.page.data_mut().copy_within(record_range, write_pos);
            self.get_slot_mut(idx)?.offset = write_pos as u16;
        }
        let header = self.get_base_header_mut()?;
        header.free_space_offset = write_pos as u16;
        header.free_contiguous_size = write_pos as u16 - header.free_space_start();
        header.first_freeblock_offset = u16::MAX;
        Ok(())
    }
}

/// In the future add b-tree and heap file implementations of this
/// The structs implementing this trait must use #[repr(C)]
pub(crate) trait SlottedPageHeader: Pod {
    fn base(&self) -> &SlottedPageBaseHeader;
}

impl SlottedPageHeader for SlottedPageBaseHeader {
    fn base(&self) -> &SlottedPageBaseHeader {
        self
    }
}

use crate::cache::{PageRead, PageWrite};
use crate::slotted_page::SlottedPageError::IndexOutOfBounds;
use bytemuck::{Contiguous, Pod, PodCastError, Zeroable};
use std::ops::Sub;
use thiserror::Error;

#[derive(Pod, Zeroable, Copy, Clone)]
#[repr(C)]
pub(crate) struct SlottedPageBaseHeader {
    free_space_size: u16,
    free_contiguous_size: u16,
    free_space_offset: u16,
    header_size: u16,
    num_slots: u16,
}

impl SlottedPageBaseHeader {
    pub fn free_space_start(&self) -> u16 {
        self.header_size + self.num_slots * size_of::<Slot>() as u16
    }
}
pub(crate) enum InsertResult {
    Success { slot_idx: u16 },
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
            return Err(IndexOutOfBounds {
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
            return Err(IndexOutOfBounds {
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
}

impl Slot {
    pub fn new(offset: u16, len: u16) -> Self {
        Self { offset, len }
    }

    pub fn is_deleted(&self) -> bool {
        self.offset == u16::MAX_VALUE
    }

    pub fn mark_deleted(&mut self) {
        self.offset = u16::MAX_VALUE;
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
        if (header.free_space_size as usize).saturating_sub(size_of::<Slot>()) < record.len() {
            return Ok(InsertResult::PageFull);
        } else if (header.free_contiguous_size as usize).saturating_sub(size_of::<Slot>())
            < record.len()
        {
            return Ok(InsertResult::NeedsDefragmentation);
        }
        Ok(InsertResult::Success { slot_idx: 1 })
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
    fn append_record_at_end(&mut self, record: &[u8], record_start_pos: u16) {
        self.page.data_mut()[record_start_pos as usize..record_start_pos as usize + record.len()]
            .copy_from_slice(record);
    }

    /// Removes deleted slots and pushes the filled ones together. Keeps the order of slots intact and
    /// frees space.
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
        Ok(())
    }

    pub fn defragment(&mut self) -> Result<(), SlottedPageError> {
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

// TODO: move to wal.rs once its created

use std::collections::BTreeMap;

/// Structure containing list of diffs applied to a page.
///
/// Invariants:
/// - Each diff is stored as its start offset and new bytes.
/// - Diffs are disjoint.
#[derive(Default)]
pub(crate) struct PageDiff {
    diffs: BTreeMap<u16, Vec<u8>>,
}

impl PageDiff {
    /// Adds new diff to [`PageDiff`].
    pub(crate) fn write_at(&mut self, offset: u16, data: Vec<u8>) {
        let mut new_start = offset;
        let mut new_data = data;
        let new_end = offset + new_data.len() as u16;

        let mut to_remove = vec![];

        // Iterate over all diffs that start before end of new diff
        for (&diff_start, diff_data) in self.diffs.range(..=new_end) {
            let diff_end = diff_start + diff_data.len() as u16;

            // Skip element that ends before new diff starts
            if diff_end < offset {
                continue;
            }

            // Merge left (there is at most one element that match this criteria)
            if diff_start < new_start {
                let prefix_len = (new_start - diff_start) as usize;
                let mut merged = diff_data[..prefix_len].to_vec();
                merged.append(&mut new_data);
                new_data = merged;
                new_start = diff_start;
            }

            // Merge right (there is at most one element that match this criteria)
            if diff_end > new_end {
                let suffix_start = (new_end - diff_start) as usize;
                new_data.extend_from_slice(&diff_data[suffix_start..]);
            }

            to_remove.push(diff_start);
        }

        for r in to_remove {
            self.diffs.remove(&r);
        }

        self.diffs.insert(new_start, new_data);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_non_overlapping_writes() {
        let mut diff = PageDiff::default();

        diff.write_at(0, vec![0, 1, 2]);
        diff.write_at(10, vec![10, 11, 12]);
        diff.write_at(20, vec![20, 21]);

        assert_eq!(diff.diffs.len(), 3);
        assert_eq!(diff.diffs.get(&0), Some(&vec![0, 1, 2]));
        assert_eq!(diff.diffs.get(&10), Some(&vec![10, 11, 12]));
        assert_eq!(diff.diffs.get(&20), Some(&vec![20, 21]));
    }

    #[test]
    fn test_overlapping_writes_later_wins() {
        let mut diff = PageDiff::default();

        // Write [2, 2, 2] at positions 1, 2, 3
        diff.write_at(1, vec![2, 2, 2]);

        // Write [4, 4, 4] at positions 3, 4, 5 - overlaps at position 3
        diff.write_at(3, vec![4, 4, 4]);

        // Should merge into single range: 1..=5 -> [2, 2, 4, 4, 4]
        assert_eq!(diff.diffs.len(), 1);
        assert_eq!(diff.diffs.get(&1), Some(&vec![2, 2, 4, 4, 4]));
    }

    #[test]
    fn test_fully_overlapping_write() {
        let mut diff = PageDiff::default();

        // Write [1, 1, 1, 1, 1] at offset 10..=14
        diff.write_at(10, vec![1, 1, 1, 1, 1]);

        // Completely overwrite middle with [9, 9, 9] at offset 11..=13
        diff.write_at(11, vec![9, 9, 9]);

        // Should merge: 10..=14 -> [1, 9, 9, 9, 1]
        assert_eq!(diff.diffs.len(), 1);
        assert_eq!(diff.diffs.get(&10), Some(&vec![1, 9, 9, 9, 1]));
    }

    #[test]
    fn test_adjacent_writes() {
        let mut diff = PageDiff::default();

        diff.write_at(0, vec![1, 2, 3]);
        diff.write_at(3, vec![4, 5, 6]);

        // Should merge adjacent writes: 0..=5 -> [1, 2, 3, 4, 5, 6]
        assert_eq!(diff.diffs.len(), 1);
        assert_eq!(diff.diffs.get(&0), Some(&vec![1, 2, 3, 4, 5, 6]));
    }

    #[test]
    fn test_partial_overlap_at_start() {
        let mut diff = PageDiff::default();

        // Write [5, 5, 5, 5, 5] at offset 10..=14
        diff.write_at(10, vec![5, 5, 5, 5, 5]);

        // Overwrite beginning with [7, 7] at offset 10..=11
        diff.write_at(10, vec![7, 7]);

        // Should merge: 10..=14 -> [7, 7, 5, 5, 5]
        assert_eq!(diff.diffs.len(), 1);
        assert_eq!(diff.diffs.get(&10), Some(&vec![7, 7, 5, 5, 5]));
    }

    #[test]
    fn test_partial_overlap_at_end() {
        let mut diff = PageDiff::default();

        // Write [3, 3, 3, 3] at offset 20..=23
        diff.write_at(20, vec![3, 3, 3, 3]);

        // Overwrite end with [8, 8, 8] at offset 22..=24
        diff.write_at(22, vec![8, 8, 8]);

        // Should merge: 20..=24 -> [3, 3, 8, 8, 8]
        assert_eq!(diff.diffs.len(), 1);
        assert_eq!(diff.diffs.get(&20), Some(&vec![3, 3, 8, 8, 8]));
    }

    #[test]
    fn test_multiple_overwrites_same_position() {
        let mut diff = PageDiff::default();

        diff.write_at(100, vec![1, 1]);
        diff.write_at(100, vec![2, 2]);
        diff.write_at(100, vec![3, 3]);

        assert_eq!(diff.diffs.len(), 1);
        assert_eq!(diff.diffs.get(&100), Some(&vec![3, 3]));
    }

    #[test]
    fn test_empty_diff() {
        let diff = PageDiff::default();
        assert_eq!(diff.diffs.len(), 0);
    }

    #[test]
    fn test_merge_three_ranges() {
        let mut diff = PageDiff::default();

        // Create three separate ranges
        diff.write_at(0, vec![1, 1]); // 0..=1
        diff.write_at(5, vec![5, 5]); // 5..=6
        diff.write_at(10, vec![10, 10]); // 10..=11

        assert_eq!(diff.diffs.len(), 3);

        // Write that bridges all three
        diff.write_at(2, vec![2, 2, 2, 2, 2, 2, 2, 2]); // 2..=9

        // Should merge all into one: 0..=11 -> [1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 10, 10]
        assert_eq!(diff.diffs.len(), 1);
        assert_eq!(
            diff.diffs.get(&0),
            Some(&vec![1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 10, 10])
        );
    }

    #[test]
    fn test_write_with_gap() {
        let mut diff = PageDiff::default();

        diff.write_at(0, vec![1, 2, 3]);
        diff.write_at(10, vec![10, 11, 12]);

        // Write that doesn't touch existing ranges
        diff.write_at(5, vec![5, 6]);

        // Should have 3 separate ranges
        assert_eq!(diff.diffs.len(), 3);
        assert_eq!(diff.diffs.get(&0), Some(&vec![1, 2, 3]));
        assert_eq!(diff.diffs.get(&5), Some(&vec![5, 6]));
        assert_eq!(diff.diffs.get(&10), Some(&vec![10, 11, 12]));
    }

    #[test]
    fn test_maximize_merge_sequential_writes() {
        let mut diff = PageDiff::default();

        // Simulate sequential writes that should all merge
        for i in 0..10 {
            diff.write_at(i, vec![i as u8]);
        }

        // All should merge into one contiguous range
        assert_eq!(diff.diffs.len(), 1);
        assert_eq!(
            diff.diffs.get(&0),
            Some(&vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
        );
    }
}

use super::{CellRange, UnboundedRange};

use std::cmp;

// Helper internal trait. Can consider externalizing it if necessary.
pub(super) trait Range {
    fn merge_range(&self, other: &Self) -> Self;
}

impl Range for CellRange {
    fn merge_range(&self, other: &CellRange) -> CellRange {
        let start_row = cmp::min(self.start().row(), other.start().row());
        let start_col = cmp::min(self.start().col(), other.start().col());

        let end_row = cmp::max(self.end().row(), other.end().row());
        let end_col = cmp::max(self.end().col(), other.end().col());

        CellRange::new(start_row, end_row, start_col, end_col).unwrap_or_else(|_| {
            panic!(
                "Merged CellRanges should be valid. Current: {:#?}, other: {:#?}",
                self, other
            )
        })
    }
}

impl Range for UnboundedRange {
    fn merge_range(&self, other: &UnboundedRange) -> UnboundedRange {
        (cmp::min(self.0, other.0), cmp::max(self.1, other.1))
    }
}

pub fn merge_range(
    range_1: Option<UnboundedRange>,
    range_2: Option<UnboundedRange>,
) -> Option<UnboundedRange> {
    match (range_1, range_2) {
        (None, None) => None,
        (None, Some(range)) | (Some(range), None) => Some(range),
        (Some(range_1), Some(range_2)) => Some(range_1.merge_range(&range_2)),
    }
}

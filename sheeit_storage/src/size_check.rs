use crate::StorageErrorKind;
use std::isize;

/// A trait to ensure that the operations performed on the Sheet coordinates are valid.
/// Sheet coordinates are only valid if:
/// 1) They're 0 or more.
/// 2) They're not larger than MAX size.
///
/// MAX size is (around) half of the max isize because of some weird decision I made back then.
/// I think it's fine to make it isize::MAX, as we just want to ensure that the coordinate system
/// is fully representable by a signed integer, but never negative.
///
/// This is because Refs can be negative, and therefore need to be signed integers.
pub trait NonNegativeIsize {
    fn ensure(&self) -> Result<isize, StorageErrorKind>;
    fn max_size() -> Self;
}

const MAX: isize = ((isize::MAX - 1) / 2) - 1;

impl NonNegativeIsize for isize {
    fn ensure(&self) -> Result<isize, StorageErrorKind> {
        if *self < 0 {
            Err(StorageErrorKind::InvalidParameter)
        } else {
            Ok(*self)
        }
    }

    fn max_size() -> isize {
        MAX
    }
}

impl NonNegativeIsize for usize {
    fn ensure(&self) -> Result<isize, StorageErrorKind> {
        if *self >= ((isize::MAX - 1) / 2) as usize {
            Err(StorageErrorKind::InvalidParameter)
        } else {
            Ok(*self as isize)
        }
    }

    fn max_size() -> usize {
        MAX as usize
    }
}

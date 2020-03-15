//! The storage module for Sheeit.
//!
//! Provides everything to deal with data in the spreadsheet, from performing read/write operations
//! to data structures to deal with locations and helpers to print the sheet.

pub mod location;
pub mod size_check;
pub mod util;

mod document;
mod shifter;

pub use crate::document::*;
pub use sheeit_parser::*;

use thiserror::Error;

/// The different error types that storage operations can result in.
/// Current, it is very bare-bones and not very informative.
///
/// More work needs to be done here to have a richer error API.
#[derive(Error, Debug, Clone)]
// TODO: Better error handling.
pub enum StorageErrorKind {
    #[error("Invalid Parameter")]
    InvalidParameter,

    #[error("Parse Error")]
    ParseError,

    #[error("Transform Error")]
    TransformError,

    #[error("Not Found Error")]
    NotFoundError,

    #[error("Attempting to persist a value with an invalid sequence")]
    InvalidatedSequence,

    #[error("Found a bug in the program. Did not panic because it is recoverable.")]
    RecoverableUnexpectedError,

    #[error("Got errors: {:#?}", .0)]
    Compound(Vec<StorageErrorKind>),
}

/// The Sequence type is an atomically increasing unsigned integer which is incremented every time
/// the Document is updated.
pub type Sequence = u64;

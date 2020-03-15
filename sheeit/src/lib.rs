//! `sheeit` is the main module which users interface with.
//!
//! A quick example of how to interface with Sheeit's API:
//!
//!
//! ```rust
//! use sheeit::storage::{Coordinate, SheetCoordinate, Storage, Value};
//!
//! let storage = Storage::obtain();
//! let uuid = storage.add_ledger();
//!
//! let coordinate = Coordinate::new(0, 0, 0).unwrap();
//! let (write_result, _) = storage
//!                     .transact_write(uuid, |document| {
//!                         // Within the transact_write block,
//!                         // no other writes can happen until this returns.
//!                         document.add_sheet();
//!
//!                         document.insert_cell_facts(
//!                             &coordinate,
//!                             vec![vec![Some("=SUM(1, 2) - 4".to_string())]],
//!                         )
//!                         .expect("Inserting should be successful");
//!                 })
//!                     .expect("Writing to doc should be successful");
//!
//! // Perform Evaluation
//! write_result.eval_handle.join().unwrap();
//!
//! // Read flow
//! // `read_document` is an owned `Document` struct that users can read *and* mutate, but the
//! // mutation will not be reflected in storage.
//! // Any mutation must be performed via `transact_write`.
//! let read_document = storage.read(&uuid).unwrap();
//!
//! assert_eq!(read_document
//!     .sheet_at(0)
//!     .unwrap()
//!     .cell_at(coordinate.sheet_coord())
//!     .unwrap()
//!     .value(),
//!     &Value::Number(-1.0)
//! );
//!
//! ```
//!
//! Currently, `sheeit` contains only the `storage` module which provides an interface to store data
//! in memory and perform read-write operations. Evaluation will always be done concurrently in the background,
//! eventually reaching a consistent state.
//!
//!
mod dependency;
mod evaluation;
mod model;
mod shifter;
pub mod storage;

use thiserror::Error;

// TODO: Better error handling.
/// Currently this is a mixed-bag of errors that is used internally as well as errors that will be surfaced by the user.
/// Better error handling and API is needed.
#[derive(Error, Debug, Clone)]
pub enum ErrorKind {
    #[error("Invalid Parameter")]
    InvalidParameter,
    #[error("Parse Error")]
    ParseError,

    #[error("Transform Error")]
    TransformError,

    #[error("Not Found Error")]
    NotFoundError,

    // TODO: Expand on this.
    #[error("Error during writing eval")]
    WriteEvalError,

    // TODO: Expand on this.
    #[error("Error when cleaning node")]
    WriteCleanNodeError,

    #[error("Source Node was dirtied. This eval is no longer valid.")]
    SourceNodeDirtied,

    #[error("Precedent is still dirty. Try again soon.")]
    PrecedentStillDirty,

    #[error("Read is sequence is stale. You need to get a later version of the Document.")]
    StaleRead,
}

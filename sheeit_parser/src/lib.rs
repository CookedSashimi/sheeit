//! This crate provides functionality to parse Excel expressions and formulas.
//! The goal of this crate is to provide an accurate AST representation for Excel formulas.
//!
//! Unlike other Sheeit-related crates, this can be used very much independently for any project
//! that requires parsing of Excel functions and formulas. However, the API currently is rather crude
//! and not very intuitive and user-friendly.
//!
//! ## Example
//! ```
//! use sheeit_parser::raw_parser::*;
//!
//! let parsed = parse_cell_content("=SUM(1, 2) + 3");
//!
//! assert_eq!(parsed.unwrap(),
//!     Expression::Add(
//!             Box::new(
//!                 Expression::Fn(
//!                     "SUM".to_string(),
//!                     vec![
//!                         Expression::ValueNum(1.0),
//!                         Expression::ValueNum(2.0)
//!                     ]
//!                 )
//!             ),
//!             Box::new(Expression::ValueNum(3.0))
//!         )
//! );
//! ```

pub mod raw_parser;
pub mod transformer;

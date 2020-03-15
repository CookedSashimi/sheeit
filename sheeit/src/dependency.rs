mod coordinate;
mod dependency_table;
mod nodes;

pub use coordinate::*;
pub use dependency_table::*;
pub use nodes::*;

use crate::storage::raw_parser::Expression;

use crate::storage::location::{Coordinate, RefersToLocation};
use std::collections::HashSet;

pub fn extract_precedents(expr: &Expression, coord: &Coordinate) -> HashSet<NodeType> {
    let mut result = HashSet::new();

    extract_precedents_inner(expr, coord, &mut result);

    result
}

fn extract_precedents_inner(expr: &Expression, coord: &Coordinate, result: &mut HashSet<NodeType>) {
    match expr {
        Expression::ValueString(_)
        | Expression::ValueNum(_)
        | Expression::ValueBool(_)
        | Expression::Err(_) => {}
        Expression::Ref(ref_expr) => {
            if let Ok(coords) = ref_expr.refers_to(coord.sheet_coord()) {
                coords.into_iter().for_each(|range| {
                    result.insert(range.into());
                });
            };
        }
        Expression::RefA1(_) => panic!("Unexpected"),
        Expression::Percent(expr) | Expression::Parens(expr) | Expression::Negate(expr) => {
            extract_precedents_inner(expr, coord, result)
        }
        Expression::Add(expr1, expr2)
        | Expression::Subtract(expr1, expr2)
        | Expression::Multiply(expr1, expr2)
        | Expression::Divide(expr1, expr2)
        | Expression::Exponentiate(expr1, expr2)
        | Expression::Eq(expr1, expr2)
        | Expression::NotEq(expr1, expr2)
        | Expression::Gt(expr1, expr2)
        | Expression::Lt(expr1, expr2)
        | Expression::Gte(expr1, expr2)
        | Expression::Lte(expr1, expr2)
        | Expression::Concat(expr1, expr2) => {
            extract_precedents_inner(expr1, coord, result);
            extract_precedents_inner(expr2, coord, result);
        }
        Expression::Fn(_name, exprs) => exprs
            .iter()
            .for_each(|expr| extract_precedents_inner(expr, coord, result)),
    }
}

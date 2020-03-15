use crate::Sheet;
use prettytable::{Row, Table};
use std::cmp;

/// A utility to print the Sheet.
/// WARNING: Not intended to have a stable output. Use for debugging purposes only.
// Can print to something other than stdout.
pub fn print_sheet(sheet: &Sheet) {
    let mut table = Table::new();
    let max_rows = sheet
        .columns()
        .iter()
        .fold(0, |acc, col| cmp::max(acc, col.cells().len()));

    for i in 0..max_rows {
        let mut cells = Vec::new();
        for column in sheet.columns() {
            cells.push(
                column
                    .cells()
                    .get(i)
                    .map(|cell| {
                        let content = match cell.formula() {
                            None => format!("{}", cell.value()),
                            Some(formula) => format!("{}  :  ={}", cell.value(), formula.parsed()),
                        };

                        prettytable::Cell::new(&content)
                    }) // TODO: Better formatting for formula.
                    .unwrap_or_else(|| prettytable::Cell::new("")),
            )
        }

        table.add_row(Row::new(cells));
    }

    table.printstd();
}

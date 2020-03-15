use crate::location::{CellRange, CellRangeView, Coordinate, SheetCoordinate};
use crate::size_check::NonNegativeIsize;
use crate::{shifter, Sequence, StorageErrorKind};
use im::vector::Focus;
use im::Vector;
use lazy_static::lazy_static;
use rayon::prelude::*;
use sheeit_parser::raw_parser;
use sheeit_parser::raw_parser::{ExprErr, Expression, Ref};
use sheeit_parser::transformer;
use std::borrow::BorrowMut;
use std::cmp;
use std::collections::HashSet;
use std::fmt::{Debug, Display, Error, Formatter};
use std::isize;

/// A Document represents the entire stored data for a, well, document.
/// You can think of this like an Excel file. However, it's more like a *subset* of the file.
/// The subset which is the data that is
/// 1) Persistent. Not ephemerally generated and discarded. (Therefore, this should be serializable. Serialization is a TODO)
/// 2) Efficiently cloneable. We heavily rely on persistent data structures, and it's expected that this Document is cloned and passed along frequently.
#[derive(Clone, Debug)]
pub struct CoreDocument {
    sheets: Vector<Sheet>,
    sequence: Sequence,
}

/// A Sheet represents a, well, sheet.
/// Must be efficiently Cloneable, with the same reason as Document.
#[derive(Clone, Debug)]
pub struct Sheet {
    columns: Vector<Column>,
}

/// In general, users should not bother themselves with the Column structure.
/// This is an internal data structure.
#[derive(Clone, Debug)]
pub struct Column {
    cells: Vector<Cell>,
}

/// A trait that allows users to define a custom Value type.
/// This is currently under-tested in terms of use case.
pub trait CustomValue: CustomValueClone + Debug + Send + Sync {
    fn formatted_value(&self) -> String;
    fn eq(&self, other: &Value) -> bool;
}

/// Errors types that can be a result from evaluating a Formula.
/// ExpressionErr can be represented in an Expression. However, certain errors cannot, and are separated.
#[derive(Clone, Debug, PartialEq)]
pub enum EvalErrorVal {
    ExpressionErr(ExprErr),
    Invalid,
    CyclicDependency,
    UnsupportedExpression,
}

// Currently, the data model of Spill is not ideal. It uses N more memory, where N is the number of cells in Spill.
// However, the current implementation makes it really easy to evaluate.
/// The Value that is stored in every Cell. Uses Box references to optimize for memory usage.
#[derive(Clone, Debug)]
#[allow(clippy::box_vec)]
pub enum Value {
    EvalError(EvalErrorVal),
    Blank,
    Bool(bool),
    Number(f64),
    Integer(i64),
    String(Box<str>),
    Ref(Box<Ref>),               // We might not need this?
    Spill(Box<Vec<Vec<Value>>>), //Column-major order: [col][row].
    SpillRef(SheetCoordinate),
    Custom(Box<dyn CustomValue>),
}

// The size of this is very important. We need to keep it as low as possible.
// Size is guaranteed via unit tests.
// Current size is 24 bytes(Value) + 8 bytes(Box) = 40 bytes.
/// A typical cell in a spreadsheet.
/// dirtied_at should always be populated whenever the Cell is mutated from a non-evaluation call.
#[derive(Default, Clone, Debug, PartialEq)]
pub struct Cell {
    value: Value,
    dirtied_at: Sequence,
    formula: Option<Box<Formula>>, // Depending on how our dependency graph goes, we might want to make this an Arc
}

// The size of this is may not be very important, depending on the characteristics that we want to target.
// In general, we should treat formula as 2 step less in the order of magnitude compared to cells,
// 1 step less in the order of magnitude compared to rows.
// Size is guaranteed via unit tests.
// Current size is 24 bytes (String) + 56 bytes(Expression) = 80 bytes (can we get this lower??)
/// A formula that is attached to a Cell.
/// eval_at should be populated when the formula is evaluated.
#[derive(Clone, Debug, PartialEq)]
pub struct Formula {
    parsed: Expression,
    eval_at: Sequence,
}

impl Default for CoreDocument {
    fn default() -> Self {
        CoreDocument {
            sheets: Vector::new(),
            sequence: 0,
        }
    }
}

// TODO: All these vector collections of sheets/columns/cells can be DRY-ed with a macro that implements the same thing.
// Document-specific operations.
impl CoreDocument {
    pub fn new() -> CoreDocument {
        Default::default()
    }

    pub fn add_sheet(&mut self) {
        self.sheets.push_back(Sheet::new());
    }

    pub fn insert_sheet(&mut self, index: usize) -> Result<(), StorageErrorKind> {
        let _ = index.ensure()?;
        self.sheets.insert(index, Sheet::new());

        Ok(())
    }

    pub fn replace_sheet_at(
        &mut self,
        sheet: Sheet,
        index: usize,
    ) -> Result<Sheet, StorageErrorKind> {
        let _ = index.ensure()?;

        if index >= self.sheets.len() {
            return Err(StorageErrorKind::InvalidParameter);
        }

        Ok(self.sheets.set(index, sheet))
    }

    pub fn remove_sheet_at(&mut self, index: usize) -> Result<Sheet, StorageErrorKind> {
        let _ = index.ensure()?;

        if index >= self.sheets.len() {
            return Err(StorageErrorKind::InvalidParameter);
        }

        Ok(self.sheets.remove(index))
    }

    pub fn sheet_at(&self, index: usize) -> Option<&Sheet> {
        if index.ensure().is_err() {
            return None;
        }

        self.sheets.get(index)
    }

    pub fn sheets(&self) -> Vector<Sheet> {
        self.sheets.clone()
    }

    pub fn sequence(&self) -> Sequence {
        self.sequence
    }

    // TODO: Figure out how to prevent users from calling this.
    pub fn set_sequence(&mut self, seq: Sequence) {
        self.sequence = seq;
    }
}

// Operations that pass-through to Sheet
impl CoreDocument {
    pub fn get_sheet_mut(&mut self, sheet_index: usize) -> Result<&mut Sheet, StorageErrorKind> {
        self.sheets
            .get_mut(sheet_index)
            .ok_or_else(|| StorageErrorKind::InvalidParameter)
    }

    pub fn insert_cell_facts(
        &mut self,
        start_coord: &Coordinate,
        facts: Vec<Vec<Option<String>>>,
    ) -> Result<(), StorageErrorKind> {
        let sequence = self.sequence;
        let sheet = self.get_sheet_mut(start_coord.sheet())?;

        sheet.insert_cell_facts(&start_coord, sequence, facts)
    }

    pub fn insert_cells(
        &mut self,
        start_coord: &Coordinate,
        cells: Vec<Vec<Option<Cell>>>,
    ) -> Result<(), StorageErrorKind> {
        let sequence = self.sequence;
        let sheet = self.get_sheet_mut(start_coord.sheet())?;

        sheet.insert_cells(cells, &start_coord, sequence)
    }

    pub fn add_cells_at_column(
        &mut self,
        sheet_index: usize,
        col_index: usize,
        cells: Vec<Cell>,
    ) -> Result<(), StorageErrorKind> {
        let sheet = self.get_sheet_mut(sheet_index)?;

        let column = sheet
            .column_at_mut(col_index)
            .ok_or_else(|| StorageErrorKind::InvalidParameter)?;

        column.add_cells(cells)
    }

    pub fn insert_row_at(
        &mut self,
        sheet_index: usize,
        row_index: usize,
    ) -> Result<(), StorageErrorKind> {
        let sequence = self.sequence;
        let sheet = self.get_sheet_mut(sheet_index)?;

        sheet.insert_row_at(row_index, sequence)
    }

    pub fn insert_row_at_with_hint(
        &mut self,
        sheet_index: usize,
        row_index: usize,
        formula_cell_locations: HashSet<SheetCoordinate>,
    ) -> Result<(), StorageErrorKind> {
        let sequence = self.sequence;
        let sheet = self.get_sheet_mut(sheet_index)?;

        sheet.insert_row_at_with_hint(row_index, sequence, formula_cell_locations)
    }

    pub fn delete_row_at(
        &mut self,
        sheet_index: usize,
        row_index: usize,
    ) -> Result<Vec<Option<Cell>>, StorageErrorKind> {
        let sheet = self.get_sheet_mut(sheet_index)?;

        sheet.delete_row_at(row_index)
    }

    pub fn delete_row_at_with_hint(
        &mut self,
        sheet_index: usize,
        row_index: usize,
        formula_cell_locations: HashSet<SheetCoordinate>,
    ) -> Result<Vec<Option<Cell>>, StorageErrorKind> {
        let sheet = self.get_sheet_mut(sheet_index)?;

        sheet.delete_row_at_with_hint(row_index, formula_cell_locations)
    }

    pub fn add_column(&mut self, sheet_index: usize) -> Result<(), StorageErrorKind> {
        let sheet = self.get_sheet_mut(sheet_index)?;

        sheet.add_column();
        Ok(())
    }

    pub fn insert_column_at(
        &mut self,
        sheet_index: usize,
        col_index: usize,
    ) -> Result<(), StorageErrorKind> {
        let sheet = self.get_sheet_mut(sheet_index)?;

        sheet.insert_column_at(col_index)
    }

    pub fn insert_column_at_with_hint(
        &mut self,
        sheet_index: usize,
        col_index: usize,
        formula_cell_locations: HashSet<SheetCoordinate>,
    ) -> Result<(), StorageErrorKind> {
        let sheet = self.get_sheet_mut(sheet_index)?;

        sheet.insert_column_at_with_hint(col_index, formula_cell_locations)
    }

    pub fn delete_column_at(
        &mut self,
        sheet_index: usize,
        col_index: usize,
    ) -> Result<Vector<Cell>, StorageErrorKind> {
        let sheet = self.get_sheet_mut(sheet_index)?;

        sheet.delete_column_at(col_index)
    }

    pub fn delete_column_at_with_hint(
        &mut self,
        sheet_index: usize,
        col_index: usize,
        formula_cell_locations: HashSet<SheetCoordinate>,
    ) -> Result<Vector<Cell>, StorageErrorKind> {
        let sheet = self.get_sheet_mut(sheet_index)?;

        sheet.delete_column_at_with_hint(col_index, formula_cell_locations)
    }

    pub fn persist_evaluated_value(
        &mut self,
        coord: &Coordinate,
        value: Value,
    ) -> Result<(), StorageErrorKind> {
        let sheet = self
            .sheets
            .get_mut(coord.sheet())
            .ok_or_else(|| StorageErrorKind::NotFoundError)?;
        sheet.persist_evaluated_value(&coord, self.sequence, value)
    }
}

lazy_static! {
    static ref EMPTY_CELL_VECTOR: Vector<Cell> = Vector::new();
}

impl Default for Sheet {
    fn default() -> Self {
        Sheet {
            columns: Vector::new(),
        }
    }
}

// The main user interaction with the sheet should be here.
impl Sheet {
    pub fn new() -> Sheet {
        Default::default()
    }

    fn insert_cell_facts(
        &mut self,
        start_location: &Coordinate,
        sequence: Sequence,
        facts: Vec<Vec<Option<String>>>,
    ) -> Result<(), StorageErrorKind> {
        let cells = facts
            .into_par_iter()
            .enumerate()
            .try_fold(
                || vec![],
                |mut acc, (col_index, rows)| {
                    let col_index = start_location.col() + col_index;

                    let rows = rows
                        .into_par_iter()
                        .enumerate()
                        .try_fold(
                            || vec![],
                            |mut acc, (row_index, fact_opt)| {
                                let row_index = start_location.row() + row_index;

                                let cell_opt = fact_opt
                                    .map(|fact| {
                                        Cell::with_fact(
                                            fact,
                                            &Coordinate::new(start_location.sheet(), row_index, col_index).expect("Previous row index and column index should be valid."),

                                            sequence,
                                        )
                                    })
                                    .transpose()?;

                                acc.push(cell_opt);
                                Ok(acc)
                            },
                        )
                        .try_reduce(
                            || vec![],
                            |mut acc, mut item| {
                                acc.append(&mut item);
                                Ok(acc)
                            },
                        )?;

                    acc.push(rows);
                    Ok(acc)
                },
            )
            .try_reduce(
                || vec![],
                |mut acc, mut item| {
                    acc.append(&mut item);
                    Ok(acc)
                },
            )?;

        self.insert_cells(cells, start_location, sequence)
    }

    // Cells are in col-major order [col][row].
    // TODO: RELEASE-BLOCKER: Check for SpillRefs in the cell where the Spill is not part of the inserted cell range and invalidate the operation.
    fn insert_cells(
        &mut self,
        mut cells: Vec<Vec<Option<Cell>>>,
        start_location: &Coordinate,
        sequence: Sequence,
    ) -> Result<(), StorageErrorKind> {
        let start_location = &start_location.sheet_coord();
        let no_of_cols_to_insert = cells.len();
        let last_col_index = no_of_cols_to_insert + start_location.col() - 1;

        while self.columns.len() <= last_col_index {
            self.columns.push_back(Column::new());
        }

        let cols_borrow: Vec<_> = self
            .columns
            .iter_mut()
            .enumerate()
            .skip(start_location.col())
            .take(no_of_cols_to_insert)
            .map(|(col_index, col)| (col_index, col.borrow_mut()))
            .collect();

        assert_eq!(
            no_of_cols_to_insert,
            cols_borrow.len(),
            "List of mutable columns must be the same as cells to insert"
        );

        let mut cols: Vec<_> = cols_borrow.into_iter().zip(cells.drain(..)).collect();
        cols.par_iter_mut().for_each(|((col_index, col), rows)| {
            let start_row = start_location.row();

            rows.drain(..)
                .enumerate()
                .for_each(|(row_index, cell_opt)| {
                    let row_index = start_row + row_index;

                    match cell_opt {
                        None => {}
                        Some(cell) => {
                            col.set_cell(*col_index, cell, row_index, sequence)
                                .expect("Row index of rows already in storage should be valid");
                        }
                    }
                });
        });

        Ok(())
    }

    fn add_column(&mut self) {
        self.columns.push_back(Column::new());
    }

    fn insert_column_at(&mut self, col_index: usize) -> Result<(), StorageErrorKind> {
        self.insert_column_at_inner(col_index, None)
    }

    fn insert_column_at_with_hint(
        &mut self,
        col_index: usize,
        formula_cell_locations: HashSet<SheetCoordinate>,
    ) -> Result<(), StorageErrorKind> {
        self.insert_column_at_inner(col_index, Some(formula_cell_locations))
    }

    fn insert_column_at_inner(
        &mut self,
        inserted_index: usize,
        formula_cell_locations: Option<HashSet<SheetCoordinate>>,
    ) -> Result<(), StorageErrorKind> {
        let inserted_index_isize = inserted_index.ensure()?;

        self.columns.insert(inserted_index, Column::new());

        self.shift_formulas(
            inserted_index_isize,
            1,
            formula_cell_locations,
            |cell_expr: &mut Expression,
             op_index: isize,
             num_of_rows_or_cols: isize,
             cell_coord: &SheetCoordinate| {
                shifter::shift_expression_col_on_insert(
                    cell_expr,
                    cell_coord.col() as isize,
                    op_index,
                    num_of_rows_or_cols,
                );
            },
        )
    }

    fn delete_column_at(&mut self, col_index: usize) -> Result<Vector<Cell>, StorageErrorKind> {
        self.delete_column_at_inner(col_index, None)
    }

    fn delete_column_at_with_hint(
        &mut self,
        col_index: usize,
        formula_cell_locations: HashSet<SheetCoordinate>,
    ) -> Result<Vector<Cell>, StorageErrorKind> {
        self.delete_column_at_inner(col_index, Some(formula_cell_locations))
    }

    fn delete_column_at_inner(
        &mut self,
        col_index: usize,
        formula_cell_locations: Option<HashSet<SheetCoordinate>>,
    ) -> Result<Vector<Cell>, StorageErrorKind> {
        let col_index_isize = col_index.ensure()?;

        if col_index >= self.columns.len() {
            return Err(StorageErrorKind::InvalidParameter);
        }

        let removed_cells = self.columns.remove(col_index).cells;

        // TODO: Figure out how not to silently fail.
        self.shift_formulas(
            col_index_isize,
            1,
            formula_cell_locations,
            |cell_expr: &mut Expression,
             op_index: isize,
             num_of_rows_or_cols: isize,
             cell_coord: &SheetCoordinate| {
                shifter::shift_expression_col_on_delete(
                    cell_expr,
                    cell_coord.col() as isize,
                    op_index,
                    num_of_rows_or_cols,
                );
            },
        )?;

        Ok(removed_cells)
    }

    // This assumes that the cells passed in do not have to be re-written.
    // TODO: Implement inserting multiple rows.
    fn insert_row_at(
        &mut self,
        row_index: usize,
        sequence: Sequence,
    ) -> Result<(), StorageErrorKind> {
        self.insert_row_inner(row_index, sequence, None)
    }

    fn insert_row_at_with_hint(
        &mut self,
        row_index: usize,
        sequence: Sequence,
        formula_cell_locations: HashSet<SheetCoordinate>,
    ) -> Result<(), StorageErrorKind> {
        self.insert_row_inner(row_index, sequence, Some(formula_cell_locations))
    }

    fn insert_row_inner(
        &mut self,
        row_index: usize,
        sequence: Sequence,
        formula_cell_locations: Option<HashSet<SheetCoordinate>>,
    ) -> Result<(), StorageErrorKind> {
        let row_index_isize = row_index.ensure()?;

        self.columns
            .par_iter_mut()
            .enumerate()
            .for_each(|(col_index, column)| {
                let cells_len = column.cells.len();

                if cells_len > row_index {
                    column
                        .insert_cell(col_index, Cell::new_blank(sequence), row_index, sequence)
                        .ok()
                        .unwrap();
                }
            });

        self.shift_formulas(
            row_index_isize,
            1,
            formula_cell_locations,
            |cell_expr: &mut Expression,
             op_index: isize,
             num_of_rows_or_cols: isize,
             cell_coord: &SheetCoordinate| {
                shifter::shift_expression_row_on_insert(
                    cell_expr,
                    cell_coord.row() as isize,
                    op_index,
                    num_of_rows_or_cols,
                );
            },
        )
    }

    // TODO: Implement deleting multiple rows.
    fn delete_row_at(&mut self, row_index: usize) -> Result<Vec<Option<Cell>>, StorageErrorKind> {
        self.delete_row_at_inner(row_index, None)
    }
    fn delete_row_at_with_hint(
        &mut self,
        row_index: usize,
        formula_cell_locations: HashSet<SheetCoordinate>,
    ) -> Result<Vec<Option<Cell>>, StorageErrorKind> {
        self.delete_row_at_inner(row_index, Some(formula_cell_locations))
    }

    fn delete_row_at_inner(
        &mut self,
        row_index: usize,
        formula_cell_locations: Option<HashSet<SheetCoordinate>>,
    ) -> Result<Vec<Option<Cell>>, StorageErrorKind> {
        let row_index_isize = row_index.ensure()?;

        let result: Vec<Option<Cell>> = self
            .columns
            .par_iter_mut()
            .fold_with(vec![], |mut acc, col| {
                let item = if col.cells.len() <= row_index {
                    None
                } else {
                    Some(col.cells.remove(row_index))
                };

                acc.push(item);
                acc
            })
            .flatten()
            .collect();

        if result.is_empty() {
            Err(StorageErrorKind::InvalidParameter)
        } else {
            // TODO: Don't silently swallow errors.
            self.shift_formulas(
                row_index_isize,
                1,
                formula_cell_locations,
                |cell_expr: &mut Expression,
                 op_index: isize,
                 num_of_rows_or_cols: isize,
                 cell_coord: &SheetCoordinate| {
                    shifter::shift_expression_row_on_delete(
                        cell_expr,
                        cell_coord.row() as isize,
                        op_index,
                        num_of_rows_or_cols,
                    );
                },
            )?;

            Ok(result)
        }
    }

    fn persist_evaluated_value(
        &mut self,
        coord: &Coordinate,
        sequence: Sequence,
        value: Value,
    ) -> Result<(), StorageErrorKind> {
        let mut_cell = if let Value::Spill(spill_array) = &value {
            self.check_valid_persist(&coord, sequence)?;

            let curr_cell = self
                .cell_at(&coord.sheet_coord())
                .ok_or_else(|| StorageErrorKind::RecoverableUnexpectedError)?;

            let ref_spill_cells: Vec<_> = spill_array
                .iter()
                .enumerate()
                .map(|(col_index, rows)| {
                    rows.iter()
                        .enumerate()
                        .map(|(row_index, _)| {
                            let cell = if col_index == 0 && row_index == 0 {
                                curr_cell.clone()
                            } else {
                                Cell::with_value(
                                    Value::SpillRef(coord.sheet_coord().clone()),
                                    sequence,
                                )
                            };

                            Some(cell)
                        })
                        .collect()
                })
                .collect();

            self.insert_cells(ref_spill_cells, coord, sequence)?;

            self.cell_at_mut(&coord.sheet_coord())
                .ok_or_else(|| StorageErrorKind::RecoverableUnexpectedError)?
        } else {
            self.check_valid_persist(&coord, sequence)?
        };

        let formula = mut_cell
            .formula
            .as_mut()
            .ok_or_else(|| StorageErrorKind::RecoverableUnexpectedError)?;
        formula.eval_at = sequence;
        mut_cell.value = value;

        Ok(())
    }

    fn check_valid_persist(
        &mut self,
        coord: &Coordinate,
        sequence: Sequence,
    ) -> Result<&mut Cell, StorageErrorKind> {
        let cell = match self.cell_at(&coord.sheet_coord()) {
            // I don't know how to satisfy the borrow checker here with cell_at_mut
            Some(_cell) => self.cell_at_mut(&coord.sheet_coord()).unwrap(),
            None => {
                self.insert_cells(
                    vec![vec![Some(Cell::new_blank(sequence))]],
                    &coord,
                    sequence,
                )?;
                self.cell_at_mut(&coord.sheet_coord())
                    .ok_or_else(|| StorageErrorKind::RecoverableUnexpectedError)?
            }
        };

        // This is probably not needed, because we always check if the node was dirtied first.
        if cell.dirtied_at > sequence {
            return Err(StorageErrorKind::InvalidatedSequence);
        }

        let formula = cell
            .formula
            .as_ref()
            .ok_or_else(|| StorageErrorKind::NotFoundError)?;

        if formula.eval_at > sequence {
            return Err(StorageErrorKind::InvalidatedSequence);
        }

        Ok(cell)
    }

    fn shift_formulas<F: Fn(&mut Expression, isize, isize, &SheetCoordinate) -> () + Sync>(
        &mut self,
        start_op_index: isize, // Start row or col index that was inserted/deleted.
        num_of_ops: isize,     // Number of insert or delete
        formula_cell_locations: Option<HashSet<SheetCoordinate>>,
        shift_op: F,
    ) -> Result<(), StorageErrorKind> {
        let _ = start_op_index.ensure()?;
        let _ = num_of_ops.ensure()?;

        let compound_errors = vec![];
        match formula_cell_locations {
            None => {
                self.columns
                    .par_iter_mut()
                    .enumerate()
                    .for_each(|(col_index, col)| {
                        let formulas: Vec<_> = col
                            .cells
                            .par_iter()
                            .enumerate()
                            .filter_map(|(row_index, cell)| match &cell.formula {
                                Some(_formula) => Some(row_index),
                                None => None,
                            })
                            .collect();

                        // Parallelizing this will be tricky because we need to borrow cells as mutable,
                        // but we don't want to borrow ALL of them, as `im` has a huge performance penalty if
                        // we perform a mutable borrow on ALL cells.
                        // Ideally, we would like to have a mutable borrow on only the formula cells, but that
                        // seems to be a difficult thing to do in Rust...
                        for row_index in formulas {
                            let cell = col.cells.get_mut(row_index).unwrap();
                            let cell_expr = cell
                                .formula
                                .as_mut()
                                .map(|formula| &mut formula.parsed)
                                .expect("Shift of pre-checked formula cells should succeed.");

                            shift_op(
                                cell_expr,
                                start_op_index,
                                num_of_ops,
                                &SheetCoordinate::new(row_index, col_index)
                                    .expect("Formula coordinate must be valid"),
                            );
                        }
                    })
            }
            Some(locations) => {
                for location in locations {
                    match self.cell_at_mut(&location) {
                        None => {
                            // Negative case, nothing to do.
                        }
                        Some(cell) => {
                            let cell_expr =
                                match cell.formula.as_mut().map(|formula| &mut formula.parsed) {
                                    None => {
                                        // TODO: Figure out a better error handling strategy.
                                        //                                        compound_errors.push(ErrorKind::NotFoundError);
                                        continue;
                                    }
                                    Some(expr) => expr,
                                };

                            shift_op(cell_expr, start_op_index, num_of_ops, &location);
                        }
                    }
                }
            }
        };

        match compound_errors.len() {
            0 => Ok(()),
            _ => Err(StorageErrorKind::Compound(compound_errors)),
        }
    }

    pub fn row_at(&self, row_index: usize) -> Result<Vec<Option<&Cell>>, StorageErrorKind> {
        let _ = row_index.ensure()?;
        let mut result = Vec::with_capacity(self.columns.len());

        for column in self.columns.iter() {
            result.push(column.cell_at(row_index));
        }

        Ok(result)
    }

    pub fn cell_at(&self, coord: &SheetCoordinate) -> Option<&Cell> {
        self.columns
            .get(coord.col())
            .and_then(|col| col.cell_at(coord.row()))
    }

    pub fn cell_at_mut(&mut self, coord: &SheetCoordinate) -> Option<&mut Cell> {
        self.columns
            .get_mut(coord.col())
            .and_then(|col| col.cell_at_mut(coord.row()))
    }

    pub fn cells_in_range(&self, range: &CellRange) -> CellRangeView {
        let mut result = Vector::new();

        for col_index in range.start().col()..=range.end().col() {
            let col_cells = self
                .column_at(col_index)
                .map(|col| col.cells_view())
                .unwrap_or_else(|| EMPTY_CELL_VECTOR.focus());

            let col_cells_len = col_cells.len();

            let start_row = range.start().row();
            if col_cells.is_empty() || start_row >= col_cells_len {
                result.push_back(EMPTY_CELL_VECTOR.focus());
            } else {
                let end_row = cmp::min(range.end().row() + 1, col_cells_len);

                result.push_back(col_cells.narrow(start_row..end_row));
            }
        }

        CellRangeView::new(result, range.clone())
    }

    // These owned* methods are not effcient.
    // TODO: Implement iterators for these methods.
    pub fn owned_cells_in_range(&self, range: CellRange) -> Vec<Vec<Option<Cell>>> {
        let mut view = self.cells_in_range(&range);

        let mut result = vec![];
        for col in view.values().iter_mut() {
            let mut rows = vec![];

            for row_index in 0..col.len() {
                rows.push(col.get(row_index).cloned());
            }

            result.push(rows);
        }

        result
    }

    pub fn owned_formulas_in_range(&self, range: CellRange) -> Vec<Vec<Option<Expression>>> {
        let mut view = self.cells_in_range(&range);

        let mut result = vec![];
        for col in view.values().iter_mut() {
            let mut rows = vec![];

            for row_index in 0..col.len() {
                let formula = col
                    .get(row_index)
                    .and_then(|cell| cell.formula.as_ref())
                    .map(|formula| formula.parsed.clone());

                rows.push(formula);
            }

            result.push(rows);
        }

        result
    }

    pub fn column_at(&self, index: usize) -> Option<&Column> {
        if index.ensure().is_err() {
            return None;
        }

        self.columns.get(index)
    }

    pub fn column_at_mut(&mut self, index: usize) -> Option<&mut Column> {
        if index.ensure().is_err() {
            return None;
        }

        self.columns.get_mut(index)
    }

    pub fn columns(&self) -> Vector<Column> {
        self.columns.clone()
    }

    pub fn columns_len(&self) -> usize {
        self.columns.len()
    }

    pub fn cells_len(&self) -> usize {
        self.columns.iter().fold(0, |mut acc, col| {
            acc += col.cells.len();
            acc
        })
    }
}

impl Default for Column {
    fn default() -> Self {
        Column {
            cells: Vector::new(),
        }
    }
}

impl Column {
    pub fn new() -> Column {
        Default::default()
    }

    fn add_cells(&mut self, cells: Vec<Cell>) -> Result<(), StorageErrorKind> {
        let _ = cells.len().ensure()?;

        for cell in cells {
            self.cells.push_back(cell);
        }

        Ok(())
    }

    fn insert_cell(
        &mut self,
        _col_index: usize,
        cell: Cell,
        index: usize,
        seq: Sequence,
    ) -> Result<(), StorageErrorKind> {
        let _ = index.ensure()?;

        while index > self.cells.len() {
            self.cells.push_back(Cell::new_blank(seq))
        }

        self.cells.insert(index, cell);

        Ok(())
    }

    fn set_cell(
        &mut self,
        _col_index: usize,
        cell: Cell,
        index: usize,
        seq: Sequence,
    ) -> Result<(), StorageErrorKind> {
        let _ = index.ensure()?;

        while index > self.cells.len() {
            self.cells.push_back(Cell::new_blank(seq))
        }

        if index == self.cells.len() {
            self.cells.push_back(cell);
        } else {
            self.cells.set(index, cell);
        }

        Ok(())
    }

    pub fn cell_at(&self, index: usize) -> Option<&Cell> {
        if index.ensure().is_err() || index >= self.cells.len() {
            return None;
        }

        self.cells.get(index)
    }

    pub fn cell_at_mut(&mut self, index: usize) -> Option<&mut Cell> {
        if index.ensure().is_err() || index >= self.cells.len() {
            return None;
        }

        self.cells.get_mut(index)
    }

    pub fn cells_view(&self) -> Focus<Cell> {
        self.cells.focus()
    }

    pub fn cells(&self) -> Vector<Cell> {
        self.cells.clone()
    }

    pub fn cells_len(&self) -> usize {
        self.cells.len()
    }
}

impl Cell {
    pub fn new_blank(seq: Sequence) -> Cell {
        Cell::with_value(Value::Blank, seq)
    }

    pub fn with_value(value: Value, seq: Sequence) -> Cell {
        Cell {
            value,
            dirtied_at: seq,
            formula: Default::default(),
        }
    }

    pub fn with_fact(
        fact: String,
        coord: &Coordinate,
        seq: Sequence,
    ) -> Result<Cell, StorageErrorKind> {
        let parsed =
            raw_parser::parse_cell_content(&fact).map_err(|_e| StorageErrorKind::ParseError)?;

        match parsed {
            Expression::ValueString(str_val) => Ok(Cell::with_value(
                Value::String(str_val.into_boxed_str()),
                seq,
            )),
            Expression::ValueNum(num_val) => Ok(Cell::with_value(Value::Number(num_val), seq)),
            Expression::ValueBool(bool_val) => Ok(Cell::with_value(Value::Bool(bool_val), seq)),
            Expression::Err(expr_err) => Ok(Cell::with_value(
                Value::EvalError(EvalErrorVal::ExpressionErr(expr_err)),
                seq,
            )),
            _ => Ok(Cell::with_parsed_formula(
                Formula::with_expression(parsed, &coord)?,
                seq,
            )),
        }
    }

    pub fn with_parsed_formula(formula: Formula, seq: Sequence) -> Cell {
        Cell {
            value: Value::Blank,
            dirtied_at: seq,
            formula: Some(Box::new(formula)),
        }
    }

    pub fn value(&self) -> &Value {
        &self.value
    }

    pub fn dirtied_at(&self) -> Sequence {
        self.dirtied_at
    }

    pub fn formula(&self) -> Option<&Formula> {
        self.formula.as_ref().map(|formula| formula.as_ref())
    }

    pub fn update_value(&mut self, new_val: Value) {
        self.value = new_val;
    }

    pub fn update_dirtied_at(&mut self, new_dirtied_at: Sequence) {
        self.dirtied_at = new_dirtied_at;
    }

    pub fn update_formula(&mut self, new_formula: Formula) {
        self.formula = Some(Box::new(new_formula));
    }
}

impl Formula {
    pub fn with_expression(
        mut expr: Expression,
        coord: &Coordinate,
    ) -> Result<Formula, StorageErrorKind> {
        transformer::transform_to_expr_ref(&mut expr, &coord.to_transform_context()?)
            .map_err(|_e| StorageErrorKind::TransformError)?;

        Ok(Formula {
            parsed: expr,
            eval_at: 0,
        })
    }

    pub fn parsed(&self) -> &Expression {
        &self.parsed
    }

    pub fn eval_at(&self) -> Sequence {
        self.eval_at
    }
}

// TODO: Unit test these equals.
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            // Trivial Equals
            (Value::Blank, Value::Blank) => true,
            (Value::Bool(self_bool), Value::Bool(other_bool)) => self_bool.eq(other_bool),
            (Value::Number(self_num), Value::Number(other_num)) => self_num.eq(other_num),
            (Value::Integer(self_int), Value::Integer(other_int)) => self_int.eq(other_int),
            (Value::String(self_str), Value::String(other_str)) => self_str.eq(other_str),
            (Value::EvalError(self_err), Value::EvalError(other_err)) => self_err.eq(other_err),
            (Value::Ref(self_ref), Value::Ref(other_ref)) => self_ref.eq(other_ref),
            (Value::Spill(self_ref), Value::Spill(other_ref)) => self_ref.eq(other_ref),
            (Value::SpillRef(self_ref), Value::SpillRef(other_ref)) => self_ref.eq(other_ref),

            // Handling Custom
            (Value::Custom(self_custom), _) => self_custom.eq(other),
            (_, Value::Custom(other_custom)) => other_custom.eq(self),

            // Handling Blanks
            (Value::String(self_str), Value::Blank) => self_str.is_empty(),
            (Value::Blank, Value::String(other_str)) => other_str.is_empty(),
            (Value::Spill(spill), Value::Blank) => spill.is_empty(),
            (Value::Blank, Value::Spill(spill)) => spill.is_empty(),

            // Handling Numbers
            (Value::Number(self_num), Value::Integer(other_int)) => {
                (*other_int as f64).eq(self_num)
            }
            (Value::Integer(self_int), Value::Number(other_num)) => {
                (*self_int as f64).eq(other_num)
            }
            (Value::String(self_str), Value::Number(other_num)) => self_str
                .parse::<f64>()
                .map(|self_num| self_num.eq(other_num))
                .unwrap_or(false),
            (Value::String(self_str), Value::Integer(other_num)) => self_str
                .parse::<i64>()
                .map(|self_num| self_num.eq(other_num))
                .unwrap_or(false),

            // The rest are false, do not match (_, _) to ensure we cover the cases.
            (Value::EvalError(_), _) | (_, Value::EvalError(_)) => false,
            (Value::Blank, _) | (_, Value::Blank) => false,
            (Value::Bool(_), _) | (_, Value::Bool(_)) => false,
            (Value::Number(_), _) | (_, Value::Number(_)) => false,
            (Value::Integer(_), _) | (_, Value::Integer(_)) => false,
            (Value::String(_), _) | (_, Value::String(_)) => false,
            (Value::Ref(_), _) | (_, Value::Ref(_)) => false,
            (Value::Spill(_), _) | (_, Value::Spill(_)) => false,
            // To avoid warning, but we should be cognizant that this comparison happens implicitly above.
            // (Value::Spill(_), _) | (_, Value::SpillRef(_)) => false,
        }
    }
}

impl Display for Cell {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{}", self.value)
    }
}

impl Display for EvalErrorVal {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            EvalErrorVal::Invalid => write!(f, "#INVALID"),
            EvalErrorVal::CyclicDependency => write!(f, "#CYCLE"),
            EvalErrorVal::UnsupportedExpression => write!(f, "#EXPR"),
            EvalErrorVal::ExpressionErr(expr_err) => write!(f, "{}", expr_err),
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            Value::Blank => write!(f, "<BLANK>"),
            Value::Bool(bool_val) => write!(f, "{}", if *bool_val { "TRUE" } else { "FALSE" }),
            Value::Number(num) => write!(f, "{}", num),
            Value::Integer(num) => write!(f, "{}", num),
            Value::String(str_value) => write!(f, "{}", str_value),
            Value::Ref(ref_val) => write!(f, "{}", ref_val),
            Value::EvalError(err_val) => write!(f, "{}", err_val),
            Value::Spill(_spill_val) => write!(f, "TODO: Spill val fmt"),
            Value::SpillRef(coord) => write!(f, "SpillFrom:{}", coord),
            Value::Custom(custom_val) => write!(f, "{}", custom_val.formatted_value()),
        }
    }
}

// Default implemenation to ensure CustomValues can be cloned.
pub trait CustomValueClone {
    fn box_clone(&self) -> Box<dyn CustomValue>;
}

impl<T> CustomValueClone for T
where
    T: 'static + CustomValue + Clone,
{
    fn box_clone(&self) -> Box<dyn CustomValue> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn CustomValue> {
    fn clone(&self) -> Box<dyn CustomValue> {
        self.box_clone()
    }
}

#[cfg(test)]
mod tests {
    mod document_structure_test {
        use crate::document::{Cell, CoreDocument, Formula, Sheet, Value};
        use pretty_assertions::assert_eq;
        use sheeit_parser::raw_parser::{CellRef, ExprErr, Expression, Ref, RefA1, RefType};
        use sheeit_parser::transformer::TransformContext;
        use sheeit_parser::{raw_parser, transformer};

        use crate::location::{CellRange, Coordinate, SheetCoordinate};
        use std::collections::HashSet;
        use std::mem;

        fn to_exprs_opt(
            cells: Vec<Vec<Option<&str>>>,
            start_point: &SheetCoordinate,
        ) -> Vec<Vec<Option<Expression>>> {
            cells
                .iter()
                .enumerate()
                .map(|(col_index, rows)| {
                    let col_index = start_point.col() + col_index;

                    rows.iter()
                        .enumerate()
                        .map(|(row_index, fact)| {
                            let row_index = start_point.row() + row_index;

                            fact.map(|fact| {
                                let mut parsed = raw_parser::parse_cell_content(fact).unwrap();

                                transformer::transform_to_expr_ref(
                                    &mut parsed,
                                    &TransformContext {
                                        sheet_index: 0,
                                        row_index: row_index as isize,
                                        col_index: col_index as isize,
                                    },
                                )
                                .unwrap();

                                parsed
                            })
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        }

        #[test]
        fn test_memory_size() {
            let value_size = mem::size_of::<Value>();
            let expression_size = mem::size_of::<Expression>();
            let cell_size = mem::size_of::<Cell>();
            let formula_size = mem::size_of::<Formula>();
            let ref_size = mem::size_of::<Ref>();
            let refa1_size = mem::size_of::<RefA1>();
            let cell_ref_size = mem::size_of::<CellRef>();

            // With boxed ref and Fn, we can expect size of expression to go down to 32bytes. Much more reasonable.
            println!(
                "Size of value: {}, formula: {}, expression: {}, ref: {}, cell_ref: {}, refa1: {}, cell: {}",
                value_size, formula_size, expression_size, ref_size, cell_ref_size, refa1_size, cell_size
            );
            assert_eq!(value_size, 24);
            assert_eq!(cell_size, 40);
            assert_eq!(formula_size, 64);
        }

        #[test]
        fn test_lifecycle() {
            let mut document = CoreDocument::new();
            let cells = vec![
                Some(Cell::with_value(
                    Value::String(String::from("hello").into_boxed_str()),
                    0,
                )),
                Some(Cell::with_value(Value::Integer(1), 0)),
                Some(Cell::with_value(Value::Number(2.0), 0)),
                Some(
                    Cell::with_fact(
                        String::from("=NOW()"),
                        &Coordinate::new(0, 3, 0).unwrap(),
                        0,
                    )
                    .ok()
                    .unwrap(),
                ),
                Some(Cell::with_value(Value::Number(60.0), 0)),
            ];

            document.add_sheet();
            document.add_sheet();

            document.add_column(0).unwrap();
            document.add_column(0).unwrap();

            document
                .insert_cells(&Coordinate::new(0, 0, 0).unwrap(), vec![cells.clone()])
                .ok()
                .unwrap();

            document.add_column(1).unwrap();

            assert_eq!(document.sheets.len(), 2);

            let populated_sheet = document.sheet_at(0).unwrap();
            assert_eq!(populated_sheet.columns.len(), 2);

            let populated_column_index = 0;
            let populated_column = populated_sheet.column_at(populated_column_index).unwrap();
            assert_eq!(populated_column.cells.len(), cells.len());

            for (index, cell) in populated_column.cells.iter().enumerate() {
                let expected_cell = cells[index].clone();

                if let Some(ref mut formula) = expected_cell.clone().unwrap().formula {
                    transformer::transform_to_expr_ref(
                        &mut formula.parsed,
                        &TransformContext::new(0, index as isize, populated_column_index as isize),
                    )
                    .ok()
                    .unwrap();
                }

                assert_eq!(*cell, expected_cell.clone().unwrap());
            }
        }

        #[test]
        fn test_insert_row_at_empty_row() {
            let col1_rows = vec![
                Some(Cell::with_value(
                    Value::String(String::from("hello").into_boxed_str()),
                    0,
                )),
                Some(Cell::new_blank(0)),
                Some(Cell::with_value(Value::Integer(1), 0)),
                Some(Cell::with_value(Value::Number(2.0), 0)),
            ];
            let col3_rows = vec![
                Some(Cell::new_blank(0)),
                Some(Cell::with_value(
                    Value::String(String::from("world").into_boxed_str()),
                    0,
                )),
                Some(Cell::new_blank(0)),
                Some(Cell::with_value(Value::Number(222.0), 0)),
            ];

            let mut sheet = Sheet::new();
            sheet.add_column();
            sheet.add_column();
            sheet.add_column();

            sheet
                .insert_cells(
                    vec![col1_rows.clone()],
                    &Coordinate::new(0, 0, 0).unwrap(),
                    1,
                )
                .ok()
                .unwrap();
            sheet
                .insert_cells(
                    vec![col3_rows.clone()],
                    &Coordinate::new(0, 0, 2).unwrap(),
                    1,
                )
                .ok()
                .unwrap();

            sheet.insert_row_at(2, 0).ok().unwrap();

            let row1 = sheet.row_at(0).ok().unwrap();
            let row2 = sheet.row_at(1).ok().unwrap();
            let row3_inserted = sheet.row_at(2).ok().unwrap();
            let row4 = sheet.row_at(3).ok().unwrap();
            let row5 = sheet.row_at(4).ok().unwrap();

            assert_eq!(
                row1,
                vec![col1_rows[0].as_ref(), None, col3_rows[0].as_ref()]
            );
            assert_eq!(
                row2,
                vec![col1_rows[1].as_ref(), None, col3_rows[1].as_ref()]
            );
            assert_eq!(
                row3_inserted,
                vec![Some(&Cell::new_blank(0)), None, Some(&Cell::new_blank(0))]
            );

            assert_eq!(
                row4,
                vec![col1_rows[2].as_ref(), None, col3_rows[2].as_ref()]
            );
            assert_eq!(
                row5,
                vec![col1_rows[3].as_ref(), None, col3_rows[3].as_ref()]
            );
        }

        #[test]
        fn test_get_cell_in_range() {
            let mut sheet = Sheet::new();

            let column1_cells = vec![
                Some(Cell::with_value(
                    Value::String(String::from("hello").into_boxed_str()),
                    0,
                )),
                Some(Cell::with_value(
                    Value::String(String::from("world").into_boxed_str()),
                    0,
                )),
                Some(Cell::new_blank(0)),
            ];

            sheet.add_column();
            sheet.add_column();
            sheet
                .insert_cells(
                    vec![column1_cells.clone()],
                    &Coordinate::new(0, 0, 0).unwrap(),
                    1,
                )
                .ok()
                .unwrap();

            let mut view = sheet.cells_in_range(&CellRange::new(0, 2, 0, 1).ok().unwrap());
            let mut column1_return = view.values().get(0).unwrap().clone();
            let mut column2_return = view.values().get(1).unwrap().clone();

            assert_eq!(column1_return.get(0), column1_cells[0].as_ref());
            assert_eq!(column1_return.get(1), column1_cells[1].as_ref());
            assert_eq!(column1_return.get(2), column1_cells[2].as_ref());
            assert!(column1_return.get(3).is_none());

            assert!(column2_return.get(0).is_none());

            assert!(view.values().get(2).is_none());
        }

        // TODO: Port/add test to shifter.
        #[test]
        fn test_insert_row_formula_shifting() {
            // Between insert_row_at and insert_row_at_with_hint, same inputs/outputs otherwise.
            let cases: Vec<Box<dyn Fn(&mut Sheet) -> ()>> = vec![
                Box::new(|sheet: &mut Sheet| {
                    sheet
                        .insert_row_at(2, 0)
                        .expect("Should succeed insert row");
                }),
                Box::new(|sheet: &mut Sheet| {
                    let mut coords = HashSet::new();
                    for col in 0..3 {
                        for row in 0..5 {
                            coords.insert(SheetCoordinate::new(row, col).unwrap());
                        }
                    }

                    sheet.insert_row_at_with_hint(2, 0, coords).unwrap();
                }),
            ];

            for case in cases {
                let mut sheet = Sheet::new();
                sheet
                    .insert_cell_facts(
                        &Coordinate::new(0, 0, 0).unwrap(),
                        0,
                        vec![vec![
                            Some("11".to_string()),
                            Some("21".to_string()),
                            Some("31".to_string()),
                            Some("41".to_string()),
                        ]],
                    )
                    .ok()
                    .unwrap();

                sheet
                    .insert_cell_facts(
                        &Coordinate::new(0, 0, 1).unwrap(),
                        0,
                        vec![vec![
                            Some("=SUM($A$2, $A$3, $A1, $A2, $A3)".to_string()),
                            Some("=SUM($A$2, $A$3, $A2, $A3, $A4)".to_string()),
                            Some("=SUM($A$2, $A$3, $A1, $A2, $A3)".to_string()),
                            Some("=SUM($A$2, $A$3, $A2, $A3, $A4)".to_string()),
                        ]],
                    )
                    .ok()
                    .unwrap();

                sheet
                    .insert_cell_facts(
                        &Coordinate::new(0, 0, 2).unwrap(),
                        0,
                        vec![vec![
                            Some("=SUM($2:$3, 1:2:3:$4)".to_string()),
                            Some("=SUM($2:$3, 2:3:4:5)".to_string()),
                            Some("=SUM($2:$3, 1:2,3:4)".to_string()),
                            Some("=SUM($2:$3, 2:3 4:$5)".to_string()),
                        ]],
                    )
                    .ok()
                    .unwrap();

                case(&mut sheet);

                let b1 = sheet.row_at(0).ok().unwrap()[1].unwrap();
                assert_eq!(
                    b1.formula.as_ref().unwrap().parsed,
                    Expression::Fn(
                        "SUM".to_string(),
                        vec![
                            Expression::Ref(Ref::CellRef(CellRef::ARAC(1, 0))),
                            Expression::Ref(Ref::CellRef(CellRef::ARAC(3, 0))),
                            Expression::Ref(Ref::CellRef(CellRef::RRAC(0, 0))),
                            Expression::Ref(Ref::CellRef(CellRef::RRAC(1, 0))),
                            Expression::Ref(Ref::CellRef(CellRef::RRAC(3, 0)))
                        ]
                    )
                );

                let b2 = sheet.row_at(1).ok().unwrap()[1].unwrap();
                assert_eq!(
                    b2.formula.as_ref().unwrap().parsed,
                    Expression::Fn(
                        "SUM".to_string(),
                        vec![
                            Expression::Ref(Ref::CellRef(CellRef::ARAC(1, 0))),
                            Expression::Ref(Ref::CellRef(CellRef::ARAC(3, 0))),
                            Expression::Ref(Ref::CellRef(CellRef::RRAC(0, 0))),
                            Expression::Ref(Ref::CellRef(CellRef::RRAC(2, 0))),
                            Expression::Ref(Ref::CellRef(CellRef::RRAC(3, 0)))
                        ]
                    )
                );

                let b3_inserted = sheet.row_at(2).ok().unwrap()[1].unwrap();
                assert_eq!(*b3_inserted, Cell::new_blank(0));

                let b4 = sheet.row_at(3).ok().unwrap()[1].unwrap();
                assert_eq!(
                    b4.formula.as_ref().unwrap().parsed,
                    Expression::Fn(
                        "SUM".to_string(),
                        vec![
                            Expression::Ref(Ref::CellRef(CellRef::ARAC(1, 0))),
                            Expression::Ref(Ref::CellRef(CellRef::ARAC(3, 0))),
                            Expression::Ref(Ref::CellRef(CellRef::RRAC(-3, 0))),
                            Expression::Ref(Ref::CellRef(CellRef::RRAC(-2, 0))),
                            Expression::Ref(Ref::CellRef(CellRef::RRAC(0, 0)))
                        ]
                    )
                );

                let b5 = sheet.row_at(4).ok().unwrap()[1].unwrap();
                assert_eq!(
                    b5.formula.as_ref().unwrap().parsed,
                    Expression::Fn(
                        "SUM".to_string(),
                        vec![
                            Expression::Ref(Ref::CellRef(CellRef::ARAC(1, 0))),
                            Expression::Ref(Ref::CellRef(CellRef::ARAC(3, 0))),
                            Expression::Ref(Ref::CellRef(CellRef::RRAC(-3, 0))),
                            Expression::Ref(Ref::CellRef(CellRef::RRAC(-1, 0))),
                            Expression::Ref(Ref::CellRef(CellRef::RRAC(0, 0)))
                        ]
                    )
                );

                let c1 = sheet.row_at(0).ok().unwrap()[2].unwrap();
                assert_eq!(
                    c1.formula.as_ref().unwrap().parsed,
                    Expression::Fn(
                        "SUM".to_string(),
                        vec![
                            Expression::Ref(Ref::RowRangeRef(
                                (RefType::Absolute, 1),
                                (RefType::Absolute, 3)
                            )),
                            Expression::Ref(Ref::RangeRef(
                                Box::new(Ref::RowRangeRef(
                                    (RefType::Relative, 0),
                                    (RefType::Relative, 1)
                                )),
                                Box::new(Ref::RowRangeRef(
                                    (RefType::Relative, 3),
                                    (RefType::Absolute, 4)
                                ))
                            )),
                        ]
                    )
                );

                let c2 = sheet.row_at(1).ok().unwrap()[2].unwrap();
                assert_eq!(
                    c2.formula.as_ref().unwrap().parsed,
                    Expression::Fn(
                        "SUM".to_string(),
                        vec![
                            Expression::Ref(Ref::RowRangeRef(
                                (RefType::Absolute, 1),
                                (RefType::Absolute, 3)
                            )),
                            Expression::Ref(Ref::RangeRef(
                                Box::new(Ref::RowRangeRef(
                                    (RefType::Relative, 0),
                                    (RefType::Relative, 2)
                                )),
                                Box::new(Ref::RowRangeRef(
                                    (RefType::Relative, 3),
                                    (RefType::Relative, 4)
                                ))
                            )),
                        ]
                    )
                );

                // This should be empty.
                let c3 = sheet.row_at(2).ok().unwrap()[2].unwrap();
                assert_eq!(c3.formula, None);

                let c4 = sheet.row_at(3).ok().unwrap()[2].unwrap();
                assert_eq!(
                    c4.formula.as_ref().unwrap().parsed,
                    Expression::Fn(
                        "SUM".to_string(),
                        vec![
                            Expression::Ref(Ref::RowRangeRef(
                                (RefType::Absolute, 1),
                                (RefType::Absolute, 3)
                            )),
                            Expression::Ref(Ref::UnionRef(
                                Box::new(Ref::RowRangeRef(
                                    (RefType::Relative, -3),
                                    (RefType::Relative, -2)
                                )),
                                Box::new(Ref::RowRangeRef(
                                    (RefType::Relative, 0),
                                    (RefType::Relative, 1)
                                ))
                            )),
                        ]
                    )
                );

                let c5 = sheet.row_at(4).ok().unwrap()[2].unwrap();
                assert_eq!(
                    c5.formula.as_ref().unwrap().parsed,
                    Expression::Fn(
                        "SUM".to_string(),
                        vec![
                            Expression::Ref(Ref::RowRangeRef(
                                (RefType::Absolute, 1),
                                (RefType::Absolute, 3)
                            )),
                            Expression::Ref(Ref::IntersectRef(
                                Box::new(Ref::RowRangeRef(
                                    (RefType::Relative, -3),
                                    (RefType::Relative, -1)
                                )),
                                Box::new(Ref::RowRangeRef(
                                    (RefType::Relative, 0),
                                    (RefType::Absolute, 5)
                                ))
                            )),
                        ]
                    )
                );
            }
        }

        #[test]
        fn test_delete_row_at() {
            let mut sheet = Sheet::new();
            let col1_rows = vec![
                Some(Cell::with_value(
                    Value::String(String::from("hello").into_boxed_str()),
                    0,
                )),
                Some(Cell::new_blank(0)),
                Some(Cell::with_value(Value::Integer(1), 0)),
                Some(Cell::with_value(Value::Number(2.0), 0)),
            ];
            let col3_rows = vec![
                Some(Cell::new_blank(0)),
                Some(Cell::with_value(
                    Value::String(String::from("world").into_boxed_str()),
                    0,
                )),
                Some(Cell::new_blank(0)),
                Some(Cell::with_value(Value::Number(222.0), 0)),
            ];

            sheet.add_column();
            sheet.add_column();
            sheet.add_column();

            sheet
                .insert_cells(
                    vec![col1_rows.clone()],
                    &Coordinate::new(0, 0, 0).unwrap(),
                    1,
                )
                .ok()
                .unwrap();
            sheet
                .insert_cells(
                    vec![col3_rows.clone()],
                    &Coordinate::new(0, 0, 2).unwrap(),
                    1,
                )
                .ok()
                .unwrap();

            let deleted = sheet.delete_row_at(2).ok().unwrap();

            assert_eq!(
                deleted,
                vec![
                    Some(col1_rows[2].clone().unwrap()),
                    None,
                    Some(col3_rows[2].clone().unwrap())
                ]
            );
        }

        #[test]
        fn test_delete_row_formula_shifting() {
            let cases: Vec<Box<dyn Fn(&mut Sheet) -> ()>> = vec![
                Box::new(|sheet: &mut Sheet| {
                    sheet.delete_row_at(2).ok().unwrap();
                }),
                Box::new(|sheet: &mut Sheet| {
                    let mut coords = HashSet::new();
                    for col in 0..3 {
                        for row in 0..5 {
                            coords.insert(SheetCoordinate::new(row, col).unwrap());
                        }
                    }

                    sheet.delete_row_at_with_hint(2, coords).unwrap();
                }),
            ];

            for case in cases {
                let mut sheet = Sheet::new();

                sheet
                    .insert_cell_facts(
                        &Coordinate::new(0, 0, 0).unwrap(),
                        0,
                        vec![vec![
                            Some("11".to_string()),
                            Some("21".to_string()),
                            Some("31".to_string()),
                            Some("41".to_string()),
                        ]],
                    )
                    .ok()
                    .unwrap();

                sheet
                    .insert_cell_facts(
                        &Coordinate::new(0, 0, 1).unwrap(),
                        0,
                        vec![vec![
                            Some("=SUM($A$2, $A$4, $A1, $A2, $A4)".to_string()),
                            Some("=SUM($A$2, $A$4, $A2, $A3, $A5)".to_string()),
                            Some("50".to_string()),
                            Some("=SUM($A$2, $A$4, $A1, $A2, $A4)".to_string()),
                            Some("=SUM($A$2, $A$4, $A2, $A3, $A5)".to_string()),
                        ]],
                    )
                    .ok()
                    .unwrap();

                sheet
                    .insert_cell_facts(
                        &Coordinate::new(0, 0, 2).unwrap(),
                        0,
                        vec![vec![
                            Some("=$2:$4 + 1:2:3:$4".to_string()),
                            Some("=-$2:$4".to_string()),
                            Some("222".to_string()),
                            Some("=SUM($2:$4, 2)".to_string()),
                            Some("=SUM($2:$4, \"hello\")".to_string()),
                        ]],
                    )
                    .ok()
                    .unwrap();

                case(&mut sheet);

                let b1 = sheet.row_at(0).ok().unwrap()[1].unwrap();
                assert_eq!(
                    b1.formula.as_ref().unwrap().parsed,
                    Expression::Fn(
                        "SUM".to_string(),
                        vec![
                            Expression::Ref(Ref::CellRef(CellRef::ARAC(1, 0))),
                            Expression::Ref(Ref::CellRef(CellRef::ARAC(2, 0))),
                            Expression::Ref(Ref::CellRef(CellRef::RRAC(0, 0))),
                            Expression::Ref(Ref::CellRef(CellRef::RRAC(1, 0))),
                            Expression::Ref(Ref::CellRef(CellRef::RRAC(2, 0)))
                        ]
                    )
                );

                let b2 = sheet.row_at(1).ok().unwrap()[1].unwrap();
                assert_eq!(
                    b2.formula.as_ref().unwrap().parsed,
                    Expression::Fn(
                        "SUM".to_string(),
                        vec![
                            Expression::Ref(Ref::CellRef(CellRef::ARAC(1, 0))),
                            Expression::Ref(Ref::CellRef(CellRef::ARAC(2, 0))),
                            Expression::Ref(Ref::CellRef(CellRef::RRAC(0, 0))),
                            Expression::Err(ExprErr::RefErr),
                            Expression::Ref(Ref::CellRef(CellRef::RRAC(2, 0)))
                        ]
                    )
                );

                let b3 = sheet.row_at(2).ok().unwrap()[1].unwrap();
                assert_eq!(
                    b3.formula.as_ref().unwrap().parsed,
                    Expression::Fn(
                        "SUM".to_string(),
                        vec![
                            Expression::Ref(Ref::CellRef(CellRef::ARAC(1, 0))),
                            Expression::Ref(Ref::CellRef(CellRef::ARAC(2, 0))),
                            Expression::Ref(Ref::CellRef(CellRef::RRAC(-2, 0))),
                            Expression::Ref(Ref::CellRef(CellRef::RRAC(-1, 0))),
                            Expression::Ref(Ref::CellRef(CellRef::RRAC(0, 0)))
                        ]
                    )
                );

                let b4 = sheet.row_at(3).ok().unwrap()[1].unwrap();
                assert_eq!(
                    b4.formula.as_ref().unwrap().parsed,
                    Expression::Fn(
                        "SUM".to_string(),
                        vec![
                            Expression::Ref(Ref::CellRef(CellRef::ARAC(1, 0))),
                            Expression::Ref(Ref::CellRef(CellRef::ARAC(2, 0))),
                            Expression::Ref(Ref::CellRef(CellRef::RRAC(-2, 0))),
                            Expression::Err(ExprErr::RefErr),
                            Expression::Ref(Ref::CellRef(CellRef::RRAC(0, 0)))
                        ]
                    )
                );

                let c1 = sheet.row_at(0).ok().unwrap()[2].unwrap();
                assert_eq!(
                    c1.formula.as_ref().unwrap().parsed,
                    Expression::Add(
                        Box::new(Expression::Ref(Ref::RowRangeRef(
                            (RefType::Absolute, 1),
                            (RefType::Absolute, 2)
                        ))),
                        Box::new(Expression::Err(ExprErr::RefErr))
                    )
                );

                let c2 = sheet.row_at(1).ok().unwrap()[2].unwrap();
                assert_eq!(
                    c2.formula.as_ref().unwrap().parsed,
                    Expression::Negate(Box::new(Expression::Ref(Ref::RowRangeRef(
                        (RefType::Absolute, 1),
                        (RefType::Absolute, 2)
                    ))))
                );

                let c3 = sheet.row_at(2).ok().unwrap()[2].unwrap();
                assert_eq!(
                    c3.formula.as_ref().unwrap().parsed,
                    Expression::Fn(
                        "SUM".to_string(),
                        vec![
                            Expression::Ref(Ref::RowRangeRef(
                                (RefType::Absolute, 1),
                                (RefType::Absolute, 2)
                            )),
                            Expression::ValueNum(2.0)
                        ]
                    )
                );

                let c4 = sheet.row_at(3).ok().unwrap()[2].unwrap();
                assert_eq!(
                    c4.formula.as_ref().unwrap().parsed,
                    Expression::Fn(
                        "SUM".to_string(),
                        vec![
                            Expression::Ref(Ref::RowRangeRef(
                                (RefType::Absolute, 1),
                                (RefType::Absolute, 2)
                            )),
                            Expression::ValueString("hello".to_string())
                        ]
                    )
                );
            }
        }

        #[test]
        fn test_insert_column_at_with_formula_shifting() {
            let col_a = vec![
                Some("11".to_string()),
                Some("=$B$1 + $C$1 + A$1 + B$1 + C$1".to_string()),
            ];
            let col_b = vec![
                Some("21".to_string()),
                Some("=$B$1 + $C$1 + B$1 + C$1 + D$1".to_string()),
            ];
            let col_c = vec![
                Some("31".to_string()),
                Some("=$B$1 + $C$1 + A$1 + B$1 + C$1".to_string()),
            ];
            let col_d = vec![
                Some("41".to_string()),
                Some("=$B$1 + $C$1 + B$1 + C$1 + D$1".to_string()),
            ];

            let mut document = CoreDocument::new();
            document.add_sheet();

            document
                .insert_cell_facts(
                    &Coordinate::new(0, 0, 0).unwrap(),
                    vec![col_a, col_b, col_c, col_d],
                )
                .unwrap();

            document.insert_column_at(0, 2).unwrap();

            let sheet = document.sheet_at(0).unwrap();

            // Assert column is inserted.
            assert_eq!(sheet.columns.get(2).unwrap().cells.len(), 0);

            // Assert shifting.
            let exprs_result = sheet.owned_formulas_in_range(CellRange::new(1, 1, 0, 4).unwrap());
            assert_eq!(
                exprs_result,
                to_exprs_opt(
                    vec![
                        vec![Some("=$B$1 + $D$1 + A$1 + B$1 + D$1")],
                        vec![Some("=$B$1 + $D$1 + B$1 + D$1 + E$1")],
                        vec![],
                        vec![Some("=$B$1 + $D$1 + A$1 + B$1 + D$1")],
                        vec![Some("=$B$1 + $D$1 + B$1 + D$1 + E$1")]
                    ],
                    &SheetCoordinate::new(0, 0).unwrap()
                )
            );
        }

        #[test]
        fn test_delete_column_at_with_formula_shifting() {
            let col_a = vec![
                Some("11".to_string()),
                Some("=$B$1 + $D$1 + A$1 + B$1 + D$1".to_string()),
            ];
            let col_b = vec![
                Some("21".to_string()),
                Some("=$B$1 + $D$1 + B$1 + C$1 + E$1".to_string()),
            ];

            let col_c = vec![];

            let col_d = vec![
                Some("31".to_string()),
                Some("=$B$1 + $D$1 + A$1 + B$1 + D$1".to_string()),
            ];
            let col_e = vec![
                Some("41".to_string()),
                Some("=$B$1 + $D$1 + B$1 + C$1 + E$1".to_string()),
            ];

            let mut document = CoreDocument::new();
            document.add_sheet();

            document
                .insert_cell_facts(
                    &Coordinate::new(0, 0, 0).unwrap(),
                    vec![col_a, col_b, col_c, col_d, col_e],
                )
                .unwrap();

            document.delete_column_at(0, 2).unwrap();

            let sheet = document.sheet_at(0).unwrap();

            // Assert column is deleted.
            assert_eq!(sheet.columns.get(2).unwrap().cells.len(), 2);

            // Assert shifting.
            let exprs_result = sheet.owned_formulas_in_range(CellRange::new(1, 1, 0, 3).unwrap());
            assert_eq!(
                exprs_result,
                to_exprs_opt(
                    vec![
                        vec![Some("=$B$1 + $C$1 + A$1 + B$1 + C$1")],
                        vec![Some("=$B$1 + $C$1 + B$1 +#REF! + D$1")],
                        vec![Some("=$B$1 + $C$1 + A$1 + B$1 + C$1")],
                        vec![Some("=$B$1 + $C$1 + B$1 +#REF! + D$1")],
                    ],
                    &SheetCoordinate::new(0, 0).unwrap()
                )
            );
        }

        #[test]
        fn test_insert_row_with_range_formula_shifting() {
            let col_a = vec![Some("=1".to_string()), Some("=2".to_string())];
            let col_b = vec![Some("=SUM(A1:A2)".to_string())];

            let mut document = CoreDocument::new();
            document.add_sheet();

            document
                .insert_cell_facts(&Coordinate::new(0, 0, 0).unwrap(), vec![col_a, col_b])
                .unwrap();

            document.insert_row_at(0, 1).unwrap();

            assert_eq!(
                document
                    .sheet_at(0)
                    .unwrap()
                    .owned_formulas_in_range(CellRange::new(0, 0, 1, 1).unwrap()),
                to_exprs_opt(
                    vec![vec![Some("=SUM(A1:A3)")]],
                    &SheetCoordinate::new(0, 1).unwrap()
                )
            )
        }

        #[test]
        fn test_insert_cells_disjoint() {
            let col1_cells = vec![vec![
                Some(Cell::with_value(Value::Number(12.2), 0)),
                Some(Cell::with_value(
                    Value::String("hello".to_string().into_boxed_str()),
                    0,
                )),
            ]];

            let mut sheet = Sheet::new();
            sheet
                .insert_cells(col1_cells.clone(), &Coordinate::new(0, 0, 0).unwrap(), 0)
                .ok()
                .expect("Should succeed");

            let c3_d4 = vec![
                vec![
                    Some(Cell::with_value(Value::Number(22.3), 0)),
                    Some(Cell::with_value(
                        Value::String("world".to_string().into_boxed_str()),
                        0,
                    )),
                ],
                vec![
                    None,
                    Some(
                        Cell::with_fact("=$A$1".to_string(), &Coordinate::new(0, 3, 3).unwrap(), 0)
                            .ok()
                            .unwrap(),
                    ),
                ],
            ];

            sheet
                .insert_cells(c3_d4.clone(), &Coordinate::new(0, 2, 2).unwrap(), 0)
                .ok()
                .expect("Should succeed");

            let values = sheet.owned_cells_in_range(CellRange::new(0, 3, 0, 3).ok().unwrap());

            assert_eq!(
                values,
                vec![
                    col1_cells[0].clone(),
                    vec![],
                    vec![
                        Some(Cell::new_blank(0)),
                        Some(Cell::new_blank(0)),
                        c3_d4[0][0].clone(),
                        c3_d4[0][1].clone(),
                    ],
                    vec![
                        Some(Cell::new_blank(0)),
                        Some(Cell::new_blank(0)),
                        Some(Cell::new_blank(0)),
                        Some(Cell::with_parsed_formula(
                            Formula::with_expression(
                                c3_d4[1][1].clone().unwrap().formula.unwrap().parsed,
                                &Coordinate::new(0, 3, 3).unwrap()
                            )
                            .ok()
                            .unwrap(),
                            0
                        )),
                    ]
                ]
            )
        }

        #[test]
        fn test_insert_cells_overlaps() {
            let col1_cells = vec![vec![
                Some(Cell::with_value(Value::Number(12.2), 0)),
                Some(Cell::with_value(
                    Value::String("hello".to_string().into_boxed_str()),
                    0,
                )),
            ]];

            let mut sheet = Sheet::new();
            sheet
                .insert_cells(col1_cells.clone(), &Coordinate::new(0, 0, 0).unwrap(), 0)
                .ok()
                .expect("Should succeed");

            let a2_b3 = vec![
                vec![
                    Some(Cell::with_value(Value::Number(22.3), 0)),
                    Some(Cell::with_value(
                        Value::String("world".to_string().into_boxed_str()),
                        0,
                    )),
                ],
                vec![None, Some(Cell::with_value(Value::Number(223.4), 0))],
            ];

            sheet
                .insert_cells(a2_b3.clone(), &Coordinate::new(0, 1, 0).unwrap(), 0)
                .ok()
                .expect("Should succeed");

            let values = sheet.owned_cells_in_range(CellRange::new(0, 2, 0, 1).ok().unwrap());
            assert_eq!(
                values,
                vec![
                    vec![
                        col1_cells[0][0].clone(),
                        a2_b3[0][0].clone(),
                        a2_b3[0][1].clone()
                    ],
                    vec![
                        Some(Cell::new_blank(0)),
                        Some(Cell::new_blank(0)),
                        a2_b3[1][1].clone()
                    ]
                ]
            )
        }
    }
}

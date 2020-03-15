use crate::dependency::{DependencyTable, NodeLocation, NodeType, ToEnvelope};
use crate::{dependency, shifter};

use crate::storage::location::{
    CellRange, Coordinate, IntoIteratorWith, SheetCoordinate, SheetRange,
};
use crate::storage::raw_parser::Expression;
use crate::storage::size_check::NonNegativeIsize;
use crate::storage::{Cell, CoreDocument, Sequence, Sheet, StorageErrorKind, Value};

use im::Vector;
use lazy_static::lazy_static;
use shifter::{StructuralMutationOp, StructuralMutationType};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::{cmp, mem};

// Will be cleared on each write, thus cloning efficiency is not an important consideration.
// To be used by library only, any updates by users will be cleared away.
#[derive(Clone)]
pub struct EphemeralDocumentStore {
    pub(crate) added_dependencies: HashMap<NodeLocation, HashSet<NodeLocation>>,
    pub(crate) updated_formula_cells: HashSet<Coordinate>,
    pub(crate) dirtied_nodes: HashSet<NodeLocation>,
}

// This is a shim for the actual storage model of Document.
// IMPORTANT: Should not implement Clone. If any write-state needs to implement Clone,
// then it should probably be another struct and live either inside here, or inside WriteState.
pub struct Document {
    pub(crate) ephemeral_store: EphemeralDocumentStore,
    pub(crate) volatiles: HashSet<Coordinate>,
    pub(crate) dependency_table: Arc<RwLock<DependencyTable>>,
    pub(crate) doc: CoreDocument,
}

lazy_static! {
    static ref VOLATILE_FUNCTIONS: HashSet<&'static str> = {
        let mut volatiles = HashSet::new();
        volatiles.insert("NOW");
        volatiles.insert("TODAY");
        volatiles.insert("RAND");

        volatiles
    };
}

impl Document {
    pub fn new(dependency_table: Arc<RwLock<DependencyTable>>) -> Document {
        Document {
            dependency_table,
            ephemeral_store: EphemeralDocumentStore::new(),
            volatiles: HashSet::new(),
            doc: CoreDocument::new(),
        }
    }

    pub fn add_sheet(&mut self) {
        self.doc.add_sheet()
    }

    pub fn insert_sheet(&mut self, index: usize) -> Result<(), StorageErrorKind> {
        self.doc.insert_sheet(index)
    }

    pub fn replace_sheet_at(
        &mut self,
        sheet: Sheet,
        index: usize,
    ) -> Result<Sheet, StorageErrorKind> {
        self.doc.replace_sheet_at(sheet, index)
    }

    pub fn remove_sheet_at(&mut self, index: usize) -> Result<Sheet, StorageErrorKind> {
        self.doc.remove_sheet_at(index)
    }

    pub fn sheet_at(&self, index: usize) -> Option<&Sheet> {
        self.doc.sheet_at(index)
    }

    pub fn sheets(&self) -> Vector<Sheet> {
        self.doc.sheets()
    }

    // TODO: Figure out how to hide this from user.
    pub fn ephemeral_store_mut(&mut self) -> &mut EphemeralDocumentStore {
        &mut self.ephemeral_store
    }

    pub fn ephemeral_store(&self) -> &EphemeralDocumentStore {
        &self.ephemeral_store
    }

    pub fn drain_ephemeral_store(&mut self) -> EphemeralDocumentStore {
        let mut new = EphemeralDocumentStore::new();
        mem::swap(&mut self.ephemeral_store, &mut new);

        // New is now prev
        new
    }

    pub fn sequence(&self) -> Sequence {
        self.doc.sequence()
    }

    // TODO: Figure out how to prevent users from calling this.
    pub fn set_sequence(&mut self, seq: Sequence) {
        self.doc.set_sequence(seq)
    }
}

impl Default for EphemeralDocumentStore {
    fn default() -> Self {
        EphemeralDocumentStore {
            added_dependencies: HashMap::new(),
            updated_formula_cells: HashSet::new(),
            dirtied_nodes: HashSet::new(),
        }
    }
}

impl EphemeralDocumentStore {
    pub fn new() -> EphemeralDocumentStore {
        Default::default()
    }
}

impl Document {
    pub fn insert_cell_facts(
        &mut self,
        start_coord: &Coordinate,
        facts: Vec<Vec<Option<String>>>,
    ) -> Result<(), StorageErrorKind> {
        let max_rows = facts.iter().fold(0, |acc, col| cmp::max(acc, col.len()));
        let col_len = facts.len();

        self.doc.insert_cell_facts(start_coord, facts)?;

        self.after_insert_cells(start_coord, max_rows, col_len)
    }

    pub fn insert_cells(
        &mut self,
        start_coord: &Coordinate,
        cells: Vec<Vec<Option<Cell>>>,
    ) -> Result<(), StorageErrorKind> {
        let max_rows = cells.iter().fold(0, |acc, col| cmp::max(acc, col.len()));
        let col_len = cells.len();

        self.doc.insert_cells(start_coord, cells)?;

        self.after_insert_cells(start_coord, max_rows, col_len)
    }

    fn after_insert_cells(
        &mut self,
        start_coord: &Coordinate,
        rows_added: usize,
        cols_added: usize,
    ) -> Result<(), StorageErrorKind> {
        let cell_range = Document::construct_cell_range(start_coord, rows_added, cols_added)?;

        self.populate_updated_cells(start_coord.sheet(), &cell_range)?;
        self.populate_volatiles(start_coord.sheet(), &cell_range)?;
        self.extract_and_add_dependencies(start_coord, &cell_range)
    }

    pub fn add_cells_at_column(
        &mut self,
        sheet_index: usize,
        col_index: usize,
        cells: Vec<Cell>,
    ) -> Result<(), StorageErrorKind> {
        self.doc.add_cells_at_column(sheet_index, col_index, cells)
    }

    // TODO: Use insert with hint
    pub fn insert_row_at(
        &mut self,
        sheet_index: usize,
        row_index: usize,
    ) -> Result<(), StorageErrorKind> {
        let num_ops = 1;
        let mutation_op = StructuralMutationOp::Insert;
        let mutation_type = StructuralMutationType::Row;

        // TODO: We need to make a formal guarantee that insert does not happen if an error occurred.
        match self.shifting_hints(
            sheet_index,
            num_ops,
            row_index,
            &mutation_op,
            &mutation_type,
        )? {
            None => {
                self.doc.insert_row_at(sheet_index, row_index)?;
            }
            Some(formula_cells) => {
                self.doc
                    .insert_row_at_with_hint(sheet_index, row_index, formula_cells)?;
            }
        };

        self.shift_nodes(
            num_ops,
            row_index,
            sheet_index,
            &mutation_op,
            &mutation_type,
        );

        Ok(())
    }

    // TODO: Use delete with hint
    pub fn delete_row_at(
        &mut self,
        sheet_index: usize,
        row_index: usize,
    ) -> Result<Vec<Option<Cell>>, StorageErrorKind> {
        let num_ops = 1;
        let mutation_op = StructuralMutationOp::Delete;
        let mutation_type = StructuralMutationType::Row;

        // TODO: We need to make a formal guarantee that insert does not happen if an error occurred.
        let result = match self.shifting_hints(
            sheet_index,
            num_ops,
            row_index,
            &mutation_op,
            &mutation_type,
        )? {
            None => self.doc.delete_row_at(sheet_index, row_index)?,
            Some(formula_cells) => {
                self.doc
                    .delete_row_at_with_hint(sheet_index, row_index, formula_cells)?
            }
        };

        self.shift_nodes(
            num_ops,
            row_index,
            sheet_index,
            &mutation_op,
            &mutation_type,
        );

        Ok(result)
    }

    pub fn add_column(&mut self, sheet_index: usize) -> Result<(), StorageErrorKind> {
        self.doc.add_column(sheet_index)
    }

    // TODO: Use insert with hint
    pub fn insert_column_at(
        &mut self,
        sheet_index: usize,
        col_index: usize,
    ) -> Result<(), StorageErrorKind> {
        let num_ops = 1;
        let mutation_op = StructuralMutationOp::Insert;
        let mutation_type = StructuralMutationType::Column;

        // TODO: We need to make a formal guarantee that insert does not happen if an error occurred.
        match self.shifting_hints(
            sheet_index,
            num_ops,
            col_index,
            &mutation_op,
            &mutation_type,
        )? {
            None => self.doc.insert_column_at(sheet_index, col_index)?,
            Some(formula_cells) => {
                self.doc
                    .insert_column_at_with_hint(sheet_index, col_index, formula_cells)?
            }
        };

        self.shift_nodes(
            num_ops,
            col_index,
            sheet_index,
            &mutation_op,
            &mutation_type,
        );

        Ok(())
    }

    // TODO: Use delete with hint
    pub fn delete_column_at(
        &mut self,
        sheet_index: usize,
        col_index: usize,
    ) -> Result<Vector<Cell>, StorageErrorKind> {
        let num_ops = 1;
        let mutation_op = StructuralMutationOp::Delete;
        let mutation_type = StructuralMutationType::Column;

        // TODO: We need to make a formal guarantee that insert does not happen if an error occurred.
        let result = match self.shifting_hints(
            sheet_index,
            num_ops,
            col_index,
            &mutation_op,
            &mutation_type,
        )? {
            None => self.doc.delete_column_at(sheet_index, col_index)?,
            Some(formula_cells) => {
                self.doc
                    .delete_column_at_with_hint(sheet_index, col_index, formula_cells)?
            }
        };

        self.shift_nodes(
            num_ops,
            col_index,
            sheet_index,
            &mutation_op,
            &mutation_type,
        );

        Ok(result)
    }

    pub fn persist_evaluated_value(
        &mut self,
        coord: &Coordinate,
        value: Value,
    ) -> Result<(), StorageErrorKind> {
        self.doc.persist_evaluated_value(coord, value)
    }

    fn shift_nodes(
        &mut self,
        num_ops: usize,
        ops_index: usize,
        sheet_index: usize,
        mutation_op: &StructuralMutationOp,
        mutation_type: &StructuralMutationType,
    ) {
        self.shift_dependency_table_nodes(
            num_ops,
            ops_index,
            sheet_index,
            mutation_op,
            mutation_type,
        );

        self.shift_ephemeral_store_nodes(
            num_ops,
            ops_index,
            sheet_index,
            mutation_op,
            mutation_type,
        );

        self.dirty_nodes_on_shift(num_ops, ops_index, sheet_index, mutation_op, mutation_type);
    }

    fn shift_dependency_table_nodes(
        &mut self,
        num_ops: usize,
        ops_index: usize,
        sheet_index: usize,
        mutation_op: &StructuralMutationOp,
        mutation_type: &StructuralMutationType,
    ) {
        let mut dependency_table = self.dependency_table.write().unwrap();

        let envelope = match mutation_type {
            StructuralMutationType::Row => {
                SheetRange::RowRange((ops_index, usize::max_size())).envelope()
            }
            StructuralMutationType::Column => {
                SheetRange::ColumnRange((ops_index, usize::max_size())).envelope()
            }
        };

        let mut to_insert = vec![];
        let mut to_remove = vec![];
        for node_location in dependency_table.nodes_in(&envelope) {
            let shifted = shifter::shift_node(
                &node_location,
                num_ops,
                sheet_index,
                ops_index,
                mutation_op,
                mutation_type,
            );
            match shifted {
                None => {
                    to_remove.push(node_location.clone());
                }
                Some(new_location) => {
                    to_insert.push((
                        new_location,
                        (
                            node_location.clone(),
                            *dependency_table.node_id(node_location).unwrap(),
                        ),
                    ));
                }
            };
        }

        dependency_table.update_nodes(to_remove, to_insert).expect(
            "The nodes passed in should be valid nodes already present in dependency table.",
        );
    }

    // Don't need to add deleted nodes to ephemeral store here because the nodes in ephemeral store
    // are new, and have never been recalc before, and therefore is always dirty.
    fn shift_ephemeral_store_nodes(
        &mut self,
        num_ops: usize,
        ops_index: usize,
        sheet_index: usize,
        mutation_op: &StructuralMutationOp,
        mutation_type: &StructuralMutationType,
    ) {
        let mut new_updated_formula_cells =
            HashSet::with_capacity(self.ephemeral_store.updated_formula_cells.capacity());

        for cell in self.ephemeral_store.updated_formula_cells.iter() {
            let new_coord = match shifter::shift_coordinate(
                &cell,
                num_ops,
                sheet_index,
                ops_index,
                mutation_op,
                mutation_type,
            ) {
                None => {
                    // Should be deleted, don't add it to the new map.
                    continue;
                }
                Some(coord) => coord,
            };

            new_updated_formula_cells.insert(new_coord);
        }

        self.ephemeral_store.updated_formula_cells = new_updated_formula_cells;

        let mut new_dirtied_nodes =
            HashSet::with_capacity(self.ephemeral_store.dirtied_nodes.capacity());
        for dirtied_node in self.ephemeral_store.dirtied_nodes.iter() {
            let new_node = match shifter::shift_node(
                dirtied_node,
                num_ops,
                sheet_index,
                ops_index,
                mutation_op,
                mutation_type,
            ) {
                None => {
                    // We don't need to do anything here. Since we collect all nodes. If the dependents
                    // need to be dirtied, they're already in the dirtied_nodes set.
                    continue;
                }
                Some(node) => node,
            };

            new_dirtied_nodes.insert(new_node);
        }

        self.ephemeral_store.dirtied_nodes = new_dirtied_nodes;

        let mut new_map =
            HashMap::with_capacity(self.ephemeral_store.added_dependencies.capacity());
        // Exact same logic as dependency table. Will DRY if necessary in the future
        for (precedent, dependents) in self.ephemeral_store.added_dependencies.iter() {
            let new_precedent = match shifter::shift_node(
                &precedent,
                num_ops,
                sheet_index,
                ops_index,
                mutation_op,
                mutation_type,
            ) {
                None => {
                    // The precedent is deleted. All the dependents are now no longer depending on it.
                    continue;
                }
                Some(node) => node,
            };

            let mut new_dependents = HashSet::new();
            for dependent in dependents {
                let new_dependent = match shifter::shift_node(
                    &dependent,
                    num_ops,
                    sheet_index,
                    ops_index,
                    mutation_op,
                    mutation_type,
                ) {
                    None => {
                        // The dependent is deleted. No longer needed in the relationship.
                        continue;
                    }
                    Some(node) => node,
                };

                new_dependents.insert(new_dependent);
            }

            new_map.insert(new_precedent, new_dependents);
        }

        self.ephemeral_store.added_dependencies = new_map;
    }

    fn dirty_nodes_on_shift(
        &mut self,
        _num_ops: usize,
        ops_index: usize,
        _sheet_index: usize,
        _mutation_op: &StructuralMutationOp,
        mutation_type: &StructuralMutationType,
    ) {
        let dependency_table = self.dependency_table.read().unwrap();

        // For any moving part (below/right), we dirty it and its dependents.
        // For a node that is (above/left), it is only dirtied if it is a dependent on a node
        // (below/right). Else, it didn't move.
        // Also, nodes that are straddling the boundaries will be dirtied.
        // Nodes that are deleted should be dirtied in the preceding function.
        // (Maybe should refactor this for locality sake)
        let envelope = match mutation_type {
            StructuralMutationType::Row => {
                SheetRange::RowRange((ops_index, usize::max_size())).envelope()
            }
            StructuralMutationType::Column => {
                SheetRange::ColumnRange((ops_index, usize::max_size())).envelope()
            }
        };

        for location in dependency_table.nodes_in(&envelope) {
            self.ephemeral_store.dirtied_nodes.insert(location.clone());
        }
    }

    fn shifting_hints(
        &self,
        sheet_index: usize,
        num_ops: usize,
        ops_index: usize,
        mutation_op: &StructuralMutationOp,
        mutation_type: &StructuralMutationType,
    ) -> Result<Option<HashSet<SheetCoordinate>>, StorageErrorKind> {
        let cells_count = self
            .doc
            .sheet_at(sheet_index)
            .ok_or_else(|| StorageErrorKind::NotFoundError)?
            .cells_len();
        let dependency_table = self.dependency_table.read().unwrap();
        let precedents_count = dependency_table.precedents_len();

        // Only use hints if we don't have many formula cells. As using hints will cause single-threaded formula shifting.
        if precedents_count <= cells_count / 4 {
            let mut formula_cells = HashSet::new();

            for dependent in dependency_table.all_dependents().iter() {
                match shifter::shift_node(
                    dependent,
                    num_ops,
                    sheet_index,
                    ops_index,
                    mutation_op,
                    mutation_type,
                ) {
                    None => continue,
                    Some(new_dependent) => {
                        for sheet_coord in new_dependent.into_iter_with(&self.doc, false) {
                            formula_cells.insert(sheet_coord.clone());
                        }
                    }
                }
            }

            for updated_cell in self.ephemeral_store.updated_formula_cells.iter() {
                if updated_cell.sheet() == sheet_index {
                    let new_coord = match shifter::shift_coordinate(
                        updated_cell,
                        num_ops,
                        sheet_index,
                        ops_index,
                        mutation_op,
                        mutation_type,
                    ) {
                        None => {
                            // Should be deleted, don't add it to the new map.
                            continue;
                        }
                        Some(coord) => coord,
                    };

                    formula_cells.insert(new_coord.sheet_coord().clone());
                }
            }

            Ok(Some(formula_cells))
        } else {
            Ok(None)
        }
    }

    fn construct_cell_range(
        start_coord: &Coordinate,
        rows_added: usize,
        col_added: usize,
    ) -> Result<CellRange, StorageErrorKind> {
        let end_coord = SheetCoordinate::new(
            start_coord.row() + rows_added - 1,
            start_coord.col() + col_added - 1,
        )?;
        CellRange::from_coords(start_coord.sheet_coord().clone(), end_coord)
    }

    fn populate_updated_cells(
        &mut self,
        sheet_index: usize,
        cell_range: &CellRange,
    ) -> Result<(), StorageErrorKind> {
        let sheet = self
            .doc
            .sheet_at(sheet_index)
            .ok_or_else(|| StorageErrorKind::NotFoundError)?;

        let mut range_view = sheet.cells_in_range(cell_range);

        for (col_index, rows) in range_view.values().iter_mut().enumerate() {
            for row_index in 0..rows.len() {
                match rows.get(row_index) {
                    None => {}
                    Some(cell) => {
                        if cell.dirtied_at() == self.sequence() {
                            self.ephemeral_store
                                .updated_formula_cells
                                .insert(Coordinate::new(
                                    sheet_index,
                                    cell_range.start().row() + row_index,
                                    cell_range.start().col() + col_index,
                                )?);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn populate_volatiles(
        &mut self,
        sheet_index: usize,
        cell_range: &CellRange,
    ) -> Result<(), StorageErrorKind> {
        let sheet = self
            .doc
            .sheet_at(sheet_index)
            .ok_or_else(|| StorageErrorKind::NotFoundError)?;

        let mut range_view = sheet.cells_in_range(cell_range);

        for (col_index, col) in range_view.values().iter_mut().enumerate() {
            for index in 0..col.len() {
                let has_volatile = col
                    .get(index)
                    .and_then(|cell| cell.formula())
                    .map_or(false, |formula| Document::has_volatile(formula.parsed()));

                if has_volatile {
                    self.volatiles.insert(Coordinate::new(
                        sheet_index,
                        cell_range.start().row() + index,
                        cell_range.start().col() + col_index,
                    )?);
                }
            }
        }

        Ok(())
    }

    fn has_volatile(expr: &Expression) -> bool {
        match expr {
            Expression::ValueString(_)
            | Expression::ValueNum(_)
            | Expression::ValueBool(_)
            | Expression::Err(_)
            | Expression::Ref(_)
            | Expression::RefA1(_) => false,
            Expression::Percent(expr) | Expression::Parens(expr) | Expression::Negate(expr) => {
                Document::has_volatile(expr)
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
                let first_has_volatile = Document::has_volatile(expr1);
                let second_has_volatile = Document::has_volatile(expr2);

                first_has_volatile || second_has_volatile
            }
            Expression::Fn(name, exprs) => {
                VOLATILE_FUNCTIONS.contains(name.as_str())
                    || exprs.iter().any(|expr| Document::has_volatile(expr))
            }
        }
    }

    fn extract_and_add_dependencies(
        &mut self,
        start_coord: &Coordinate,
        cell_range: &CellRange,
    ) -> Result<(), StorageErrorKind> {
        let relationships = self.construct_relationships(start_coord.sheet(), cell_range)?;
        self.add_dependencies(relationships)
    }

    fn construct_relationships(
        &mut self,
        sheet_index: usize,
        cell_range: &CellRange,
    ) -> Result<HashMap<Coordinate, HashSet<NodeType>>, StorageErrorKind> {
        let sheet = self.doc.get_sheet_mut(sheet_index)?;
        let mut range_view = sheet.cells_in_range(&cell_range);
        let mut result = HashMap::new();

        for (col_index, rows) in range_view.values().iter_mut().enumerate() {
            for row_index in 0..rows.len() {
                let relationships =
                    rows.get(row_index)
                        .and_then(|cell| cell.formula())
                        .map(|formula| {
                            let coord = Coordinate::new(
                                sheet_index,
                                    cell_range.start().row() + row_index,
                                    cell_range.start().col() + col_index,
                            ).expect("Coordinate should be valid when constructing relationships. TODO: Verify this.");


                            let precedents =
                                dependency::extract_precedents(formula.parsed(), &coord);
                            (coord, precedents)
                        });

                match relationships {
                    None => {}
                    Some((coord, precedents)) => {
                        result.insert(coord, precedents);
                    }
                };
            }
        }

        Ok(result)
    }

    // Nodes is a map of Coordinates to its precedents.
    fn add_dependencies(
        &mut self,
        nodes: HashMap<Coordinate, HashSet<NodeType>>,
    ) -> Result<(), StorageErrorKind> {
        let ephemeral_store = self.ephemeral_store_mut();

        for (coord, precedents) in nodes.into_iter() {
            let sheet_index = coord.sheet();

            let node_at_coord = NodeLocation {
                sheet_index,
                node_type: NodeType::CellRange(CellRange::from_coords(
                    coord.sheet_coord().clone(),
                    coord.sheet_coord().clone(),
                )?),
            };

            // Add the node at coord as a precedent with empty dependents.
            // i.e. all formula cells will be a precedent.
            match ephemeral_store.added_dependencies.get_mut(&node_at_coord) {
                None => {
                    ephemeral_store
                        .added_dependencies
                        .insert(node_at_coord.clone(), HashSet::new());
                }
                Some(_) => {}
            }

            for precedent in precedents.into_iter() {
                let precedent = NodeLocation {
                    sheet_index,
                    node_type: precedent,
                };

                match ephemeral_store.added_dependencies.get_mut(&precedent) {
                    None => {
                        let mut dependents = HashSet::new();
                        dependents.insert(node_at_coord.clone());

                        ephemeral_store
                            .added_dependencies
                            .insert(precedent, dependents);
                    }
                    Some(dependents) => {
                        dependents.insert(node_at_coord.clone());
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Document;
    use crate::dependency::{DependencyTable, NodeLocation, NodeType};
    use crate::model::EphemeralDocumentStore;
    use crate::storage::Storage;

    use crate::storage::location::{CellRange, Coordinate};
    use crate::storage::raw_parser;
    use std::collections::HashSet;
    use std::sync::{Arc, RwLock};

    #[test]
    fn test_has_volatile() {
        let volatile = raw_parser::parse_cell_content("=RAND()").unwrap();

        assert_eq!(Document::has_volatile(&volatile), true);
    }

    #[test]
    fn test_populating_volatiles() {
        let mut document = Document::new(Arc::new(RwLock::new(DependencyTable::new())));
        document.add_sheet();

        let a1 = Coordinate::new(0, 0, 0).expect("Valid");
        document
            .insert_cell_facts(
                &a1,
                vec![vec![
                    Some("=NOW()".to_string()),
                    Some("=UNRELATED()".to_string()),
                    Some("=RAND()".to_string()),
                ]],
            )
            .unwrap();

        let mut expected = HashSet::new();
        expected.insert(a1.clone());
        expected.insert(Coordinate::new(0, 2, 0).expect("Valid"));

        assert_eq!(document.volatiles, expected)
    }

    /// Test formula shifting, on a single call to inserts/deletes.
    /// Ensure that we have these nodes:
    /// 1) Node on left/below.
    /// 2) Node straddling the operation.
    /// 3) Node that is directly at the operation boundary.
    /// 4) Node on right/above

    // Have nodes in different row/col to make testing easier to reason about.
    #[test]
    fn test_node_shifting_insert_row() {
        let ref_a1 = vec![Some("=SUM(1)".to_string())];
        let below = vec![None, None, Some("=A1".to_string())];
        let straddle = vec![Some("=SUM(C1:C2, A1)".to_string())];
        let on_operation = vec![None, Some("=A1".to_string())];
        let above = vec![Some("=A1".to_string())];

        let cells = vec![ref_a1, below, straddle, on_operation, above];

        let previous_coords = to_coords(&[(1, 2), (2, 0), (3, 1), (4, 0)], 0);

        let new_coords = to_coords(
            &[
                // Below, move 1 step down.
                (1, 3),
                //Straddle formula is above, do not move.
                (2, 0),
                // On insert, move 1 step down.
                (3, 2),
                // Above, do not move.
                (4, 0),
            ],
            0,
        );

        let previous_node_locations = vec![
            NodeLocation::from_coord(&Coordinate::new(0, 2, 1).expect("Valid")).unwrap(),
            NodeLocation::from_cell_range(0, CellRange::new(0, 1, 2, 2).unwrap()),
            NodeLocation::from_coord(&Coordinate::new(0, 1, 3).expect("Valid")).unwrap(),
            NodeLocation::from_coord(&Coordinate::new(0, 0, 4).expect("Valid")).unwrap(),
        ];

        let new_node_locations = vec![
            // Below, move 1 step down.
            NodeLocation::from_coord(&Coordinate::new(0, 3, 1).expect("Valid")).unwrap(),
            // Straddle, expand.
            NodeLocation::from_cell_range(0, CellRange::new(0, 2, 2, 2).unwrap()),
            // On insert, move 1 step down.
            NodeLocation::from_coord(&Coordinate::new(0, 2, 3).expect("Valid")).unwrap(),
            // Above, do not move.
            NodeLocation::from_coord(&Coordinate::new(0, 0, 4).expect("Valid")).unwrap(),
        ];

        perform_shift_test(
            cells,
            &previous_coords,
            &new_coords,
            &previous_node_locations,
            &new_node_locations,
            Box::new(|document| document.insert_row_at(0, 1).unwrap()),
            Box::new(|document| {
                document.delete_row_at(0, 1).unwrap();
            }),
        );
    }

    #[test]
    fn test_node_shifting_delete_row() {
        let ref_a1 = vec![Some("=SUM(1)".to_string())];
        let below = vec![None, None, Some("=A1".to_string())];
        let straddle = vec![Some("=SUM(C1:C3, A1)".to_string())];
        let on_operation = vec![None, Some("=A1".to_string())];
        let above = vec![Some("=A1".to_string())];

        let cells = vec![ref_a1, below, straddle, on_operation.clone(), above];

        let previous_coords = to_coords(&[(1, 2), (2, 0), (3, 1), (4, 0)], 0);
        let new_coords = to_coords(
            &[
                // Below, move 1 step up.
                (1, 1),
                //Straddle formula is above, do not move.
                (2, 0),
                // On delete row,
                // Should be deleted.
                // Above, do not move.
                (4, 0),
            ],
            0,
        );

        let previous_node_locations = vec![
            NodeLocation::from_coord(&Coordinate::new(0, 2, 1).expect("Valid")).unwrap(),
            NodeLocation::from_cell_range(0, CellRange::new(0, 2, 2, 2).unwrap()),
            NodeLocation::from_coord(&Coordinate::new(0, 1, 3).expect("Valid")).unwrap(),
            NodeLocation::from_coord(&Coordinate::new(0, 0, 4).expect("Valid")).unwrap(),
        ];

        let new_node_locations = vec![
            // Below, move 1 step up.
            NodeLocation::from_coord(&Coordinate::new(0, 1, 1).expect("Valid")).unwrap(),
            // Straddle, shrink.
            NodeLocation::from_cell_range(0, CellRange::new(0, 1, 2, 2).unwrap()),
            // On delete,
            //node deleted.

            // Above, do not move.
            NodeLocation::from_coord(&Coordinate::new(0, 0, 4).expect("Valid")).unwrap(),
        ];

        perform_shift_test(
            cells,
            &previous_coords,
            &new_coords,
            &previous_node_locations,
            &new_node_locations,
            Box::new(|document: &mut Document| {
                document.delete_row_at(0, 1).unwrap();
            }),
            Box::new(move |document: &mut Document| {
                document.insert_row_at(0, 1).unwrap();

                // Introduce back the deleted row:
                document
                    .insert_cell_facts(
                        &Coordinate::new(0, 0, 3).unwrap(),
                        vec![on_operation.clone()],
                    )
                    .unwrap();
            }),
        );
    }

    #[test]
    fn test_node_shifting_insert_column() {
        let col_a = vec![
            // Ref A1
            Some("=SUM(1)".to_string()),
            // Left
            Some("=A1".to_string()),
            // Straddle
            Some("=SUM(A3:C3, A1)".to_string()),
        ];

        let col_b = vec![
            // On operation.
            Some("=A1".to_string()),
        ];

        let col_c = vec![
            // Right
            Some("=A1".to_string()),
        ];

        let cells = vec![col_a, col_b, col_c];

        let previous_coords = to_coords(&[(0, 1), (0, 2), (1, 0), (2, 0)], 0);
        let new_coords = to_coords(
            &[
                // Left, do not move.
                (0, 1),
                //Straddle formula is left, do not move.
                (0, 2),
                // On insert column, move right.
                (2, 0),
                // Right, Move right.
                (3, 0),
            ],
            0,
        );

        let previous_node_locations = vec![
            NodeLocation::from_coord(&Coordinate::new(0, 1, 0).expect("Valid")).unwrap(),
            NodeLocation::from_cell_range(0, CellRange::new(2, 2, 0, 2).unwrap()),
            NodeLocation::from_coord(&Coordinate::new(0, 0, 1).expect("Valid")).unwrap(),
            NodeLocation::from_coord(&Coordinate::new(0, 0, 2).expect("Valid")).unwrap(),
        ];

        let new_node_locations = vec![
            // Left, do not move.
            NodeLocation::from_coord(&Coordinate::new(0, 1, 0).expect("Valid")).unwrap(),
            // Straddle, expand.
            NodeLocation::from_cell_range(0, CellRange::new(2, 2, 0, 3).unwrap()),
            // On insert, move right
            NodeLocation::from_coord(&Coordinate::new(0, 0, 2).expect("Valid")).unwrap(),
            // Right, move right.
            NodeLocation::from_coord(&Coordinate::new(0, 0, 3).expect("Valid")).unwrap(),
        ];

        perform_shift_test(
            cells,
            &previous_coords,
            &new_coords,
            &previous_node_locations,
            &new_node_locations,
            Box::new(|document: &mut Document| {
                document.insert_column_at(0, 1).unwrap();
            }),
            Box::new(move |document: &mut Document| {
                document.delete_column_at(0, 1).unwrap();
            }),
        );
    }

    #[test]
    fn test_node_shifting_delete_column() {
        let col_a = vec![
            // Ref A1
            Some("=SUM(1)".to_string()),
            // Left
            Some("=A1".to_string()),
            // Straddle
            Some("=SUM(A3:C3, A1)".to_string()),
        ];

        let col_b = vec![
            // On operation.
            Some("=A1".to_string()),
        ];

        let col_c = vec![
            // Right
            Some("=A1".to_string()),
        ];

        let cells = vec![col_a, col_b.clone(), col_c];

        let previous_coords = to_coords(&[(0, 1), (0, 2), (1, 0), (2, 0)], 0);
        let new_coords = to_coords(
            &[
                // Left, do not move.
                (0, 1),
                //Straddle formula is left, do not move.
                (0, 2),
                // On delete column
                // Formula is deleted.

                // Right, Move left.
                (1, 0),
            ],
            0,
        );

        let previous_node_locations = vec![
            NodeLocation::from_coord(&Coordinate::new(0, 1, 0).expect("Valid")).unwrap(),
            NodeLocation::from_cell_range(0, CellRange::new(2, 2, 0, 2).unwrap()),
            NodeLocation::from_coord(&Coordinate::new(0, 0, 1).expect("Valid")).unwrap(),
            NodeLocation::from_coord(&Coordinate::new(0, 0, 2).expect("Valid")).unwrap(),
        ];

        let new_node_locations = vec![
            // Left, do not move.
            NodeLocation::from_coord(&Coordinate::new(0, 1, 0).expect("Valid")).unwrap(),
            // Straddle, shrink.
            NodeLocation::from_cell_range(0, CellRange::new(2, 2, 0, 1).unwrap()),
            // On delete, node is deleted

            // Right, move left.
            NodeLocation::from_coord(&Coordinate::new(0, 0, 1).expect("Valid")).unwrap(),
        ];

        perform_shift_test(
            cells,
            &previous_coords,
            &new_coords,
            &previous_node_locations,
            &new_node_locations,
            Box::new(|document: &mut Document| {
                document.delete_column_at(0, 1).unwrap();
            }),
            Box::new(move |document: &mut Document| {
                document.insert_column_at(0, 1).unwrap();

                document
                    .insert_cell_facts(
                        &Coordinate::new(0, 0, 1).expect("Valid"),
                        vec![col_b.clone()],
                    )
                    .unwrap();
            }),
        );
    }

    fn to_coords(col_rows: &[(usize, usize)], sheet_index: usize) -> Vec<Coordinate> {
        col_rows
            .iter()
            .map(|(col, row)| Coordinate::new(sheet_index, *row, *col).unwrap())
            .collect::<Vec<_>>()
    }

    fn perform_shift_test(
        cells: Vec<Vec<Option<String>>>,
        previous_coords: &Vec<Coordinate>,
        new_coords: &Vec<Coordinate>,
        previous_node_locations: &Vec<NodeLocation>,
        new_node_locations: &Vec<NodeLocation>,
        op: Box<dyn Fn(&mut Document) -> ()>,
        post_op: Box<dyn Fn(&mut Document) -> ()>,
    ) {
        let uuid = Storage::obtain().add_ledger();
        // First, we write and perform dependency adds to get a correct initial dependency table.
        let (write_result, _) = Storage::obtain()
            .transact_write(uuid, |document| {
                document.add_sheet();

                document
                    .insert_cell_facts(&Coordinate::new(0, 0, 0).expect("Valid"), cells.clone())
                    .unwrap();

                // First, verify that our tests are bug-free by verifying the initial nodes are where we expect them to be.
                verify_locations_in_ephemeral_store(
                    &previous_node_locations,
                    &previous_coords,
                    &document.ephemeral_store,
                );

                // Then, perform insert/delete.
                op(document);

                verify_locations_in_ephemeral_store(
                    &new_node_locations,
                    &new_coords,
                    &document.ephemeral_store,
                );

                // Finally, recover by performing the opposite action of insert/delete.
                post_op(document);
                ()
            })
            .unwrap();
        write_result
            .eval_handle
            .join()
            .unwrap_or_else(|_| panic!("Failed to join."));
        Storage::obtain()
            .transact_write(uuid, |document| {
                // First, verify that our tests are bug-free by verifying the initial nodes are where we expect them to be.
                // Wrap in block to scope the read lock.
                {
                    let dependency_table = document.dependency_table.read().unwrap();

                    verify_locations_in_dependency_table(
                        &previous_node_locations,
                        &dependency_table,
                    );
                }

                // Then, perform insert_row.
                op(document);
                {
                    let dependency_table = document.dependency_table.read().unwrap();
                    verify_locations_in_dependency_table(&new_node_locations, &dependency_table);
                }

                post_op(document);

                ()
            })
            .unwrap();
    }

    fn verify_locations_in_ephemeral_store(
        expected_locations: &[NodeLocation],
        expected_coords: &[Coordinate],
        store: &EphemeralDocumentStore,
    ) {
        for location in expected_locations.iter() {
            store.added_dependencies.get(location).unwrap_or_else(|| {
                panic!(
                    "No node found at expected spot. Expected node: {:#?}",
                    location
                )
            });
        }

        for expected in expected_coords {
            assert!(
                store.updated_formula_cells.contains(expected),
                format!(
                    "No coord found at expected spot. Expected coord: {:#?}",
                    expected
                )
            );
        }
    }

    fn verify_locations_in_dependency_table(
        locations: &[NodeLocation],
        dependency_table: &DependencyTable,
    ) {
        for location in locations.iter() {
            let location_id = dependency_table
                .node_id(location)
                .expect("Should have location in id map");

            assert!(
                dependency_table.has_precedent_with_id(location_id),
                format!(
                    "No node found at expected spot. Expected node: {:#?}",
                    location
                )
            );

            // A1 contains all the dependencies, except for range dependents (dependents can only be formula cells now).
            if let NodeType::CellRange(cell_range) = &location.node_type {
                if cell_range.start().eq(&cell_range.end()) {
                    let a1_id = dependency_table
                        .node_id(
                            &NodeLocation::from_coord(&Coordinate::new(0, 0, 0).expect("Valid"))
                                .unwrap(),
                        )
                        .expect("Should have A1 node");

                    assert!(
                        dependency_table
                            .dependents_of_by_id(a1_id)
                            .unwrap()
                            .contains(location),
                        format!(
                            "No node found at expected spot. Expected node: {:#?}",
                            location
                        )
                    );
                }
            }

            assert!(
                dependency_table
                    .node_dirty_clean_state_by_id(location_id)
                    .is_some(),
                format!(
                    "No node found at expected spot. Expected node: {:#?}",
                    location
                )
            );
            assert!(
                dependency_table
                    .nodes_in(&location.clone().into())
                    .next()
                    .is_some(),
                format!(
                    "No node found at expected spot. Expected node: {:#?}",
                    location
                )
            );
        }
    }
}

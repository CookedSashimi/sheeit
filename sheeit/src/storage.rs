//! This module provides 2 main things:
//!
//! 1. A re-export of the storage crate, so that users only need to import this crate.
//! 2. Any storage-related shim that is not part of the responsibility of the storage crate,
//!    like handling dependencies.
use crate::dependency::{DependencyTable, Node, NodeLocation};
use crate::model::{Document, EphemeralDocumentStore};
use crate::ErrorKind;
use crate::{dependency, evaluation};
use chashmap::CHashMap;
use lazy_static::lazy_static;
use sheeit_computation::EvalContext;
pub use sheeit_storage::location::*;
pub use sheeit_storage::raw_parser::*;
pub use sheeit_storage::size_check::*;
pub use sheeit_storage::transformer::*;
pub use sheeit_storage::util::*;
pub use sheeit_storage::StorageErrorKind;
pub use sheeit_storage::*;
pub use sheeit_storage::{Cell, CoreDocument, EvalErrorVal, Sequence, Sheet, Value};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard};
use std::thread::JoinHandle;
use uuid::Uuid;

/// Mutable write-state. Allows for non-Clone items and anything else that's only relevant in the write path.
struct WriteState {
    pub document: Document,
}

impl WriteState {
    pub fn new(dependency_table: Arc<RwLock<DependencyTable>>) -> WriteState {
        WriteState {
            document: Document::new(dependency_table),
        }
    }

    fn before_user_write(&mut self) {
        let prev_sequence = self.document.sequence();
        self.document.set_sequence(prev_sequence + 1);
    }

    // TODO: CRITICAL PERFORMANCE AREA
    // This is not performant at all. We should optimize this somehow, maybe via graph compression.
    fn add_dependencies(&mut self, ephemeral_store: &EphemeralDocumentStore) {
        let dependencies = &ephemeral_store.added_dependencies;
        let mut dependency_table_write = self.document.dependency_table.write().unwrap();

        for (precedent, dependents) in dependencies.iter() {
            dependency_table_write.insert_precedent(precedent);

            for dependent in dependents.iter() {
                dependency_table_write.add_dependency(precedent, dependent.clone())
            }
        }

        for (precedent, dependents) in dependencies.iter() {
            for dependent in dependents.iter() {
                let transitive_dependents =
                    self.transitive_dependents(&dependent, precedent, &dependency_table_write);

                for transitive_dependent in transitive_dependents {
                    dependency_table_write.add_dependency(&precedent, transitive_dependent);
                }
            }
        }
    }

    fn is_node_dirty(&self, node: &Node, dependency_table: &DependencyTable) -> bool {
        match dependency_table.node_by_id(node.id()) {
            None => {
                // The source node is deleted. Therefore, it is dirty.
                true
            }
            Some(location) => {
                // The source node was moved/changed. Therefore, it is dirty.
                if !location.eq(node.location()) {
                    true
                } else {
                    dependency_table
                        .node_dirty_clean_state_by_id(node.id())
                        .map_or(false, |(last_dirtied, _last_cleaned)| {
                            *last_dirtied > *node.dirtied_at()
                        })
                }
            }
        }
    }

    fn transitive_dependents(
        &self,
        dependent: &NodeLocation,
        source_precedent: &NodeLocation,
        dependency_table: &DependencyTable,
    ) -> HashSet<NodeLocation> {
        // The map & clone is needed here because we can't have an immutable reference
        // and mutable reference of dependency_table at the same time, even though the
        // mutations only happen in the hashmap, but there's no easy way
        // to let the compiler know.
        let direct_precedents: HashSet<NodeLocation> = dependency_table
            .precedents_at(&dependent)
            .into_iter()
            .cloned()
            .collect();

        let mut result = HashSet::new();
        for direct_precedent in direct_precedents {
            dependency_table
                .dependents_of(&direct_precedent)
                .into_iter()
                .for_each(|transitive_dependent| {
                    if transitive_dependent.eq(source_precedent)
                        || transitive_dependent.eq(dependent)
                    {
                        return;
                    }
                    result.insert(transitive_dependent.clone());

                    result.extend(self.transitive_dependents(
                        &transitive_dependent,
                        &direct_precedent,
                        dependency_table,
                    ));
                });
        }

        result
    }

    /// For each of the updated formula cells, get the corresponding precedent nodes.
    /// Dirty them and their dependents by updating the 'dirtied' map and
    /// Return the <Precedent, Dependents> dirtied nodes. (Nodes with dirtied_sequence tagged)
    fn dirty_nodes(
        &mut self,
        ephemeral_store: &EphemeralDocumentStore,
    ) -> HashMap<Node, Vec<Node>> {
        let sequence = self.document.sequence();

        let mut result = HashMap::new();

        // Finer-grained borrow to allow mutable borrow (On dependency_table) + immutable borrow (on volatiles)
        let dependency_table = &mut self.document.dependency_table.write().unwrap();
        let volatiles = &self.document.volatiles;

        // Maybe consider moving volatiles to WriteState instead?
        for volatile_cell in volatiles.iter() {
            WriteState::collect_dirty_nodes_at_coord(
                dependency_table,
                sequence,
                &mut result,
                volatile_cell,
            );
        }

        for updated_formula in ephemeral_store.updated_formula_cells.iter() {
            WriteState::collect_dirty_nodes_at_coord(
                dependency_table,
                sequence,
                &mut result,
                &updated_formula,
            );
        }

        for dirty_node in ephemeral_store.dirtied_nodes.iter() {
            dependency_table.make_node_dirty(dirty_node.clone(), sequence);
            WriteState::collect_dirty_nodes(dependency_table, sequence, &mut result, dirty_node);
        }

        result
    }

    fn collect_dirty_nodes_at_coord(
        dependency_table: &mut DependencyTable,
        sequence: u64,
        result: &mut HashMap<Node, Vec<Node>>,
        dirty_source: &Coordinate,
    ) {
        let dirty_precedents =
            dependency_table.make_dirty_precedents_at_point(dirty_source, sequence);

        dirty_precedents.iter().for_each(|precedent| {
            WriteState::collect_dirty_nodes(dependency_table, sequence, result, precedent)
        });
    }

    fn collect_dirty_nodes(
        dependency_table: &mut DependencyTable,
        sequence: Sequence,
        result: &mut HashMap<Node, Vec<Node>>,
        node_location: &NodeLocation,
    ) {
        // Unwrap should be safe here because precedent should already exist in dependency table.
        // Logic error if not.
        let node = dependency_table
            .create_node_at(node_location, sequence)
            .unwrap();

        // This precedent could have already been processed (e.g. Cell is updated AND is a volatile)
        // Skip if already processed.
        match result.get_mut(&node) {
            None => {
                let dependents: Vec<_> = dependency_table
                    .dependents_of(node_location)
                    .into_iter()
                    .cloned()
                    .collect();

                let dependents_node: Vec<_> = dependents
                    .iter()
                    .map(|dependent| {
                        // Unwrap should be safe here, because dependency table is expected to have
                        // all the nodes populated already. Logic error if not.
                        dependency_table
                            .create_node_at(&dependent, sequence)
                            .unwrap()
                    })
                    .collect();

                result.insert(node, dependents_node);

                // TODO: Perf improvements.
                // The only reason why we need to collect the dirty nodes for each corresponding precedent is because
                // during evaluation, for each formula in the dependent, we perform checks on the precedent (the token in the formula)
                // However, this is actually a special case. We actually only need to mark the precedent as dirty, and no need to
                // recursively dirty the precedent (adding dependents to the precedent)
                // I think we can just add the precedent to `result` here, but now results will have 2 faces:
                // precedents with actual dirty dependents, and precedents that are dirtied because some dependents got dirtied.
                // A better API is to introduce a new data structure that represents these 'dirtied from dependents' precedents.
                for dependent in dependents.iter() {
                    let precedents: Vec<_> = dependency_table
                        .precedents_at(dependent)
                        .into_iter()
                        .cloned()
                        .collect();

                    for precedent in precedents.iter() {
                        WriteState::collect_dirty_nodes(
                            dependency_table,
                            sequence,
                            result,
                            precedent,
                        );
                    }
                }
            }
            Some(_) => {}
        };
    }
}

// Ideally, we'd store a history of Documents to support things like undo.
// But for now, we just store the prev + current.
/// A 'ledger' that contains the history of a document. Contains some internal data structures such
/// as the DependencyTable that the user is not exposed to.
///
/// The user will utilize this to get the current or previous document.
/// In the future, when undo-redo is supported, the user can get a history of documents here.
pub struct Ledger {
    current: Arc<Mutex<WriteState>>,
    previous: CoreDocument,
    dependency_table: Arc<RwLock<DependencyTable>>,
}

impl Ledger {
    fn new() -> Ledger {
        let dependency_table = Arc::new(RwLock::new(DependencyTable::new()));
        let document_state = WriteState::new(dependency_table.clone());
        let previous = document_state.document.doc.clone();

        Ledger {
            current: Arc::new(Mutex::new(document_state)),
            previous,
            dependency_table,
        }
    }

    // Can we have a nice, sane API for set_current?

    fn set_previous(&mut self, new_previous: CoreDocument) {
        self.previous = new_previous;
    }

    fn previous(&self) -> CoreDocument {
        self.previous.clone()
    }

    fn current(&self) -> Arc<Mutex<WriteState>> {
        self.current.clone()
    }
}

lazy_static! {
    static ref STORAGE: Storage = Storage {
        ledgers: CHashMap::new()
    };
}

/// The singular store which contains all documents.
/// All documents are obtained via a UUID.
pub struct Storage {
    ledgers: CHashMap<Uuid, Ledger>,
}

/// The result from writing an evaluated value to a cell.
pub struct TransactEvalResult {
    pub document: CoreDocument,
    pub dirtied_source: Option<Node>,
}

/// The result from writing to a document.
pub struct TransactWriteResult {
    pub document: CoreDocument,
    pub eval_handle: JoinHandle<()>,
}

/// A ReadContext. The data structures inside are protected, but it provides several useful methods
/// to use during a read transaction.
pub struct ReadContext<'a> {
    dependency_table: RwLockReadGuard<'a, DependencyTable>,
}

/// A reference to a Cell that can be obtained in a read transaction.
/// Contains additional metadata to a Cell
#[derive(Debug)]
pub struct CellReadContext<'a> {
    cell: &'a Cell,
    is_dirty: bool,
}

pub(crate) struct EvalWriteContext<'a> {
    pub(crate) context: &'a EvalContext,
    pub(crate) source_node: &'a Node,
}

impl Storage {
    /// Obtain the global Storage in order to create/query/update a Document.
    pub fn obtain() -> &'static Storage {
        &STORAGE
    }

    /// Add a new Ledger to the storage. Allows you to create a document by passing in the Uuid returned.
    pub fn add_ledger(&self) -> Uuid {
        let uuid = Uuid::new_v4();
        self.ledgers.insert(uuid, Ledger::new());

        uuid
    }

    /// Starts a read transaction. A read transaction is only needed if the additional information from
    /// ReadContext is needed. (Namely, at this point in time, whether a Cell is dirty)
    /// Else, just obtain an owned Document via the read() method.
    ///
    /// A read transaction will acquire a read lock on the dependency table, which may impact writes.
    pub fn transact_read<T, F: Fn(&CoreDocument, &ReadContext) -> T>(
        &self,
        uuid: &Uuid,
        user_op: F,
    ) -> Result<T, ErrorKind> {
        let ledger = self
            .ledgers
            .get(uuid)
            .ok_or_else(|| ErrorKind::NotFoundError)?;

        let dependency_table = ledger.dependency_table.read().unwrap();

        let read_context = ReadContext { dependency_table };
        let previous = ledger.previous();

        let user_result = user_op(&previous, &read_context);

        Ok(user_result)
    }

    /// Starts a write transaction by obtaining an exclusive lock on the document.
    // TODO: Handle panics from user code.
    pub fn transact_write<T, F: FnMut(&mut Document) -> T>(
        &self,
        uuid: Uuid,
        mut user_op: F,
    ) -> Result<(TransactWriteResult, T), ErrorKind> {
        let mut ledger = self
            .ledgers
            .get_mut(&uuid)
            .ok_or_else(|| ErrorKind::NotFoundError)?;

        let current = ledger.current();
        let lock = current.lock();
        let _lock_is_err = lock.is_err();

        // TODO: We can be smarter on handling unintended panics here.
        // Figure out what to do with DocumentState: Maybe rebuild it?
        let mut data = lock.unwrap();

        (*data).before_user_write();
        let user_result = user_op(&mut (*data).document);

        let ephemeral_store = (*data).document.drain_ephemeral_store();

        (*data).add_dependencies(&ephemeral_store);
        let dirty_nodes = (*data).dirty_nodes(&ephemeral_store);

        ledger.set_previous((*data).document.doc.clone());

        let document_read = ledger.previous();
        let eval_handle = evaluation::start_eval(uuid, document_read, dirty_nodes);

        let result = TransactWriteResult {
            document: ledger.previous(),
            eval_handle,
        };

        Ok((result, user_result))
    }

    pub(crate) fn transact_write_eval(
        &self,
        uuid: &Uuid,
        context: &EvalWriteContext,
        read_sequence: Sequence,
        value: Value,
    ) -> Result<TransactEvalResult, ErrorKind> {
        // TODO: DRY this.
        let mut ledger = self
            .ledgers
            .get_mut(uuid)
            .ok_or_else(|| ErrorKind::NotFoundError)?;

        let current = ledger.current();
        let lock = current.lock();
        let _lock_is_err = lock.is_err();

        // TODO: We can be smarter on handling unintended panics here.
        // Figure out what to do with DocumentState: Maybe rebuild it?
        let data = &mut *(lock.unwrap());

        let (is_node_dirty, is_cell_dirtied) =
            Storage::check_dirty(&context, read_sequence, &value, data)?;

        data.before_user_write();
        if !is_cell_dirtied {
            let result = data
                .document
                .persist_evaluated_value(&context.context.coord, value)
                .map_err(|_e| ErrorKind::WriteEvalError);

            if result.is_err() {
                return Err(result.err().unwrap());
            }
        }

        // TODO: DRY this as well.
        ledger.set_previous(data.document.doc.clone());

        let document_read = ledger.previous();
        let eval_result = TransactEvalResult {
            document: document_read,
            dirtied_source: if is_node_dirty {
                Some(context.source_node.clone())
            } else {
                None
            },
        };

        Ok(eval_result)
    }

    fn check_dirty(
        context: &EvalWriteContext,
        read_sequence: u64,
        value: &Value,
        data: &mut WriteState,
    ) -> Result<(bool, bool), ErrorKind> {
        let dependency_table = data.document.dependency_table.read().unwrap(); // TODO: Figure out panics.

        if Value::EvalError(EvalErrorVal::CyclicDependency) != *value {
            let expression = data
                .document
                .sheet_at(context.context.coord.sheet())
                .ok_or_else(|| ErrorKind::NotFoundError)?
                .cell_at(context.context.coord.sheet_coord())
                .ok_or_else(|| ErrorKind::NotFoundError)?
                .formula()
                .ok_or_else(|| ErrorKind::NotFoundError)?
                .parsed();

            let precedents = dependency::extract_precedents(&expression, &context.context.coord);

            for node_type in precedents {
                let node_location = NodeLocation {
                    sheet_index: context.context.coord.sheet(),
                    node_type,
                };

                let (last_dirtied, last_cleaned) = dependency_table
                    .node_dirty_clean_state(&node_location)
                    .ok_or_else(|| ErrorKind::NotFoundError)?;

                if read_sequence < *last_dirtied || read_sequence < *last_cleaned {
                    return Err(ErrorKind::StaleRead);
                }

                if *last_dirtied != 0
                    && last_cleaned <= last_dirtied
                    && !node_location.is_in_range(&context.context.coord)
                {
                    return Err(ErrorKind::PrecedentStillDirty);
                }
            }
        }

        let is_node_dirty = data.is_node_dirty(&context.source_node, &dependency_table);

        let dirty_nodes = dependency_table
            .dirty_nodes_at(&context.context.coord, *context.source_node.dirtied_at());

        let is_cell_dirtied = is_node_dirty || !dirty_nodes.is_empty();

        Ok((is_node_dirty, is_cell_dirtied))
    }

    pub(crate) fn transact_write_clean_node(
        &self,
        uuid: Uuid,
        node: &Node,
    ) -> Result<(), ErrorKind> {
        // TODO: DRY this.
        let mut ledger = self
            .ledgers
            .get_mut(&uuid)
            .ok_or_else(|| ErrorKind::NotFoundError)?;

        let current = ledger.current();
        let lock = current.lock();
        let _lock_is_err = lock.is_err();

        // TODO: We can be smarter on handling unintended panics here.
        // Figure out what to do with DocumentState: Maybe rebuild it?
        let data = lock.unwrap();

        let mut dependency_table = (*data).document.dependency_table.write().unwrap();

        match dependency_table.node_dirty_clean_state_by_id(node.id()) {
            None => return Err(ErrorKind::SourceNodeDirtied),
            Some((last_dirtied, _last_cleaned)) => {
                let dirtied_at = *node.dirtied_at();

                if *last_dirtied != dirtied_at {
                    return Err(ErrorKind::SourceNodeDirtied);
                }
            }
        };

        // At this point, we need to update our last_cleaned status
        let next_sequence = (*data).document.sequence() + 1;
        dependency_table.set_node_clean_sequence_by_id(node.id(), next_sequence)?;

        let mut new_doc = (*data).document.doc.clone();
        new_doc.set_sequence(next_sequence);
        ledger.set_previous(new_doc);

        Ok(())
    }

    /// Obtain an owned copy of a Document.
    /// Does not block any reads or writes.
    pub fn read(&self, uuid: &Uuid) -> Option<CoreDocument> {
        self.ledgers.get(uuid).map(|ledger| ledger.previous())
    }

    /// Deletes a ledger. Returns an owned copy of the Document if available.
    pub fn delete(&self, uuid: &Uuid) -> Option<CoreDocument> {
        self.ledgers.remove(uuid).map(|ledger| ledger.previous)
    }
}

impl ReadContext<'_> {
    // TODO: Add tests for this and remove allow dead_code
    #[allow(dead_code)]
    fn read_cells_in_range<'iter, 'doc: 'iter, 'range: 'iter>(
        &'doc self,
        sheet: &'doc Sheet,
        sheet_index: usize,
        cell_range: &'range CellRange,
    ) -> Result<impl Iterator<Item = (Option<CellReadContext<'doc>>, Coordinate)> + 'iter, ErrorKind>
    {
        Ok(cell_range.into_iter_with(sheet, false).map(move |coord| {
            let coord = Coordinate::new_with_coord(sheet_index, coord)
                .expect("Coordinate should be valid.");

            let result_op = sheet
                .cell_at(coord.sheet_coord())
                .map(|cell| match cell.formula() {
                    None => CellReadContext {
                        cell,
                        is_dirty: false,
                    },
                    Some(formula) => {
                        let is_clean = self
                            .dependency_table
                            .dirty_nodes_at(&coord, formula.eval_at())
                            .is_empty();

                        CellReadContext {
                            cell,
                            is_dirty: !is_clean,
                        }
                    }
                });

            (result_op, coord)
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::location::{CellRange, Coordinate, SheetCoordinate};
    use super::transformer::TransformContext;
    use super::*;
    use super::{transformer, Formula};
    use super::{Cell, Value};
    use crate::dependency::NodeLocation;
    use std::collections::{HashMap, HashSet};

    macro_rules! hashset {
        ( $( $x:expr ),* ) => {  // Match zero or more comma delimited items
            {
                let mut temp_set = HashSet::new();  // Create a mutable HashSet
                $(
                    temp_set.insert($x); // Insert each item matched into the HashSet
                )*
                temp_set // Return the populated HashSet
            }
        };
    }

    macro_rules! assert_coord_ne {
        ($coord: expr, $sheet: expr, $row: expr, $col: expr) => {
            assert_ne!($coord, Coordinate::new($sheet, $row, $col).unwrap());
        };
    }

    #[test]
    pub fn test_read_after_write() {
        let storage = Storage::obtain();
        let uuid = storage.add_ledger();
        let cells = vec![
            Cell::with_value(Value::String(String::from("hello").into_boxed_str()), 0),
            Cell::with_value(Value::Integer(1), 0),
            Cell::with_value(Value::Number(2.0), 0),
            Cell::with_fact(
                String::from("=NOW()"),
                &Coordinate::new(0, 3, 0).unwrap(),
                0,
            )
            .ok()
            .unwrap(),
            Cell::with_value(Value::Number(60.0), 0),
        ];

        let (_, ()) = storage
            .transact_write(uuid, |document| {
                document.add_sheet();
                document.add_sheet();

                document.add_column(0).unwrap();
                document.add_column(0).unwrap();

                document
                    .add_cells_at_column(0, 0, cells.clone())
                    .ok()
                    .unwrap();

                document.add_column(1).unwrap();
                ()
            })
            .unwrap();

        let new_document = storage.read(&uuid).unwrap();
        for sheet in new_document.sheets() {
            for (col_index, column) in sheet.columns().iter().enumerate() {
                for (row_index, cell) in column.cells().iter().enumerate() {
                    let mut expected_cell = cells[row_index].clone();

                    if let Some(formula) = expected_cell.formula() {
                        let mut new_expr = formula.clone().parsed().clone();

                        transformer::transform_to_expr_ref(
                            &mut new_expr,
                            &TransformContext::new(0, row_index as isize, col_index as isize),
                        )
                        .ok()
                        .unwrap();

                        expected_cell.update_formula(
                            Formula::with_expression(
                                new_expr,
                                &Coordinate::new(0, row_index, col_index).unwrap(),
                            )
                            .expect("Should succeed parsing"),
                        );
                    }

                    assert_eq!(*cell, expected_cell);
                }
            }
        }
    }

    #[test]
    fn test_adding_dependencies() {
        let storage = Storage::obtain();
        let uuid = storage.add_ledger();

        let facts = vec![
            Some("=1 + 2".to_string()),
            Some("=A1 + 3".to_string()),
            Some("=A2 + 4".to_string()),
            Some("=SUM(A1:A3)".to_string()),
            Some("=A1 + A3".to_string()),
            Some("=A4 + 1".to_string()),
            Some("= 2 + 3".to_string()),
            Some("=\"hello\"".to_string()),
        ];

        let (_, ()) = storage
            .transact_write(uuid, |document| {
                document.add_sheet();
                document.add_column(0).unwrap();

                document
                    .insert_cell_facts(&Coordinate::new(0, 0, 0).unwrap(), vec![facts.clone()])
                    .ok()
                    .unwrap();

                ()
            })
            .ok()
            .unwrap();

        let write_state = storage.ledgers.get(&uuid).unwrap().current();
        let data = write_state.lock().unwrap();

        let a1 = NodeLocation::from_coord(&Coordinate::new(0, 0, 0).unwrap())
            .ok()
            .unwrap();
        let a2 = NodeLocation::from_coord(&Coordinate::new(0, 1, 0).unwrap())
            .ok()
            .unwrap();
        let a3 = NodeLocation::from_coord(&Coordinate::new(0, 2, 0).unwrap())
            .ok()
            .unwrap();
        let a4 = NodeLocation::from_coord(&Coordinate::new(0, 3, 0).unwrap())
            .ok()
            .unwrap();
        let a5 = NodeLocation::from_coord(&Coordinate::new(0, 4, 0).unwrap())
            .ok()
            .unwrap();
        let a6 = NodeLocation::from_coord(&Coordinate::new(0, 5, 0).unwrap())
            .ok()
            .unwrap();
        let a7 = NodeLocation::from_coord(&Coordinate::new(0, 6, 0).unwrap())
            .ok()
            .unwrap();
        let a1_a3 = NodeLocation::from_cell_range(
            0,
            CellRange::from_coords(
                SheetCoordinate::new(0, 0).unwrap(),
                SheetCoordinate::new(2, 0).unwrap(),
            )
            .ok()
            .unwrap(),
        );

        let data = &data.document;
        let dependency_table = data.dependency_table.read().unwrap();
        let a1_id = dependency_table.node_id(&a1).unwrap();
        let a2_id = dependency_table.node_id(&a2).unwrap();
        let a3_id = dependency_table.node_id(&a3).unwrap();
        let a4_id = dependency_table.node_id(&a4).unwrap();
        let a5_id = dependency_table.node_id(&a5).unwrap();
        let a6_id = dependency_table.node_id(&a6).unwrap();
        let a7_id = dependency_table.node_id(&a7).unwrap();

        let a1_a3_id = dependency_table.node_id(&a1_a3).unwrap();

        let a1_dependents = dependency_table.dependents_of_by_id(a1_id).unwrap();
        assert!(a1_dependents.contains(&a2));
        assert!(a1_dependents.contains(&a3));
        assert!(a1_dependents.contains(&a4));
        assert!(a1_dependents.contains(&a5));
        assert!(a1_dependents.contains(&a6));

        let a2_dependents = dependency_table.dependents_of_by_id(a2_id).unwrap();
        assert!(a2_dependents.contains(&a3));
        assert!(a2_dependents.contains(&a4));
        assert!(a2_dependents.contains(&a5));
        assert!(a2_dependents.contains(&a6));

        let a3_dependents = dependency_table.dependents_of_by_id(a3_id).unwrap();
        assert!(a3_dependents.contains(&a5));

        let a1_a3_dependents = dependency_table.dependents_of_by_id(a1_a3_id).unwrap();
        assert!(a1_a3_dependents.contains(&a4));
        assert!(a1_a3_dependents.contains(&a6));

        let a4_dependents = dependency_table.dependents_of_by_id(a4_id).unwrap();
        assert!(a4_dependents.contains(&a6));

        let a5_dependents = dependency_table.dependents_of_by_id(a5_id).unwrap();
        assert!(a5_dependents.is_empty());
        let a6_dependents = dependency_table.dependents_of_by_id(a6_id).unwrap();
        assert!(a6_dependents.is_empty());
        let a7_dependents = dependency_table.dependents_of_by_id(a7_id).unwrap();
        assert!(a7_dependents.is_empty());

        assert_eq!(dependency_table.precedents_len(), 8);

        assert_eq!(
            dependency_table
                .node_dirty_clean_state_by_id(a1_id)
                .unwrap()
                .0,
            1
        );
        assert_eq!(
            dependency_table
                .node_dirty_clean_state_by_id(a2_id)
                .unwrap()
                .0,
            1
        );
        assert_eq!(
            dependency_table
                .node_dirty_clean_state_by_id(a3_id)
                .unwrap()
                .0,
            1
        );
        assert_eq!(
            dependency_table
                .node_dirty_clean_state_by_id(a1_a3_id)
                .unwrap()
                .0,
            1
        );
        assert_eq!(
            dependency_table
                .node_dirty_clean_state_by_id(a4_id)
                .unwrap()
                .0,
            1
        );
        assert_eq!(
            dependency_table
                .node_dirty_clean_state_by_id(a5_id)
                .unwrap()
                .0,
            1
        );
        assert_eq!(
            dependency_table
                .node_dirty_clean_state_by_id(a6_id)
                .unwrap()
                .0,
            1
        );
        assert_eq!(
            dependency_table
                .node_dirty_clean_state_by_id(a7_id)
                .unwrap()
                .0,
            1
        );

        assert_eq!(dependency_table.nodes_len(), 8);
    }

    #[test]
    fn test_adding_dependencies_transferred() {
        let facts = vec![vec![
            Some("=A3".to_string()),
            Some("=B3:D4".to_string()),
            Some("=2:3:B:D".to_string()),
            Some("=A1:B2:E3:F4".to_string()),
            Some("=1:2:4:6".to_string()),
            Some("=A:C F:G".to_string()),
            Some("=A1:F4".to_string()),
        ]];

        let a1 = Coordinate::new(0, 0, 0).unwrap();
        let a2 = Coordinate::new(0, 1, 0).unwrap();
        let a3 = Coordinate::new(0, 2, 0).unwrap();
        let a4 = Coordinate::new(0, 3, 0).unwrap();
        let a5 = Coordinate::new(0, 4, 0).unwrap();
        let a6 = Coordinate::new(0, 5, 0).unwrap();
        let a7 = Coordinate::new(0, 6, 0).unwrap();

        let mut document = Document::new(Arc::new(RwLock::new(DependencyTable::new())));
        document.add_sheet();

        document
            .insert_cell_facts(&Coordinate::new(0, 0, 0).unwrap(), facts)
            .ok()
            .unwrap();

        let a3_node = NodeLocation::from_coord(&a3).ok().unwrap();
        let a3_dependents: HashSet<_> =
            get_dependents_location(&document.ephemeral_store().added_dependencies, &a3_node);

        assert_eq!(
            a3_dependents,
            hashset![NodeLocation::from_coord(&a1).ok().unwrap()]
        );

        let b3_d4_node = NodeLocation::from_cell_range(
            0,
            CellRange::from_coords(
                SheetCoordinate::new(2, 1).unwrap(),
                SheetCoordinate::new(3, 3).unwrap(),
            )
            .ok()
            .unwrap(),
        );
        let b3_d4_dependents =
            get_dependents_location(&document.ephemeral_store().added_dependencies, &b3_d4_node);
        assert_eq!(
            b3_d4_dependents,
            hashset![NodeLocation::from_coord(&a2).ok().unwrap()]
        );

        let node_2_3 = NodeLocation::from_row_range(0, 1, 2);
        let dependents_2_3 =
            get_dependents_location(&document.ephemeral_store().added_dependencies, &node_2_3);
        assert_eq!(
            dependents_2_3,
            hashset![NodeLocation::from_coord(&a3).ok().unwrap()]
        );

        let node_b_d = NodeLocation::from_column_range(0, 1, 3);
        let dependents_b_d =
            get_dependents_location(&document.ephemeral_store().added_dependencies, &node_b_d);
        assert_eq!(
            dependents_b_d,
            hashset![NodeLocation::from_coord(&a3).ok().unwrap()]
        );

        let a1_f4_node = NodeLocation::from_cell_range(
            0,
            CellRange::from_coords(
                SheetCoordinate::new(0, 0).unwrap(),
                SheetCoordinate::new(3, 5).unwrap(),
            )
            .ok()
            .unwrap(),
        );
        let a1_f4_dependents =
            get_dependents_location(&document.ephemeral_store().added_dependencies, &a1_f4_node);
        assert_eq!(
            a1_f4_dependents,
            hashset![
                NodeLocation::from_coord(&a4).ok().unwrap(),
                NodeLocation::from_coord(&a7).ok().unwrap()
            ]
        );

        let node_1_6 = NodeLocation::from_row_range(0, 0, 5);
        let dependents_1_6 =
            get_dependents_location(&document.ephemeral_store().added_dependencies, &node_1_6);
        assert_eq!(
            dependents_1_6,
            hashset![NodeLocation::from_coord(&a5).ok().unwrap()]
        );

        let node_a_g = NodeLocation::from_column_range(0, 0, 6);
        let dependents_a_g =
            get_dependents_location(&document.ephemeral_store().added_dependencies, &node_a_g);
        assert_eq!(
            dependents_a_g,
            hashset![NodeLocation::from_coord(&a6).ok().unwrap()]
        );

        for coord in vec![&a1, &a2, &a4, &a5, &a6, &a7] {
            let node = NodeLocation::from_coord(coord).ok().unwrap();

            assert!(document
                .ephemeral_store()
                .added_dependencies
                .get(&node)
                .unwrap()
                .is_empty());
        }

        assert_eq!(document.ephemeral_store().added_dependencies.len(), 13);
    }

    fn get_dependents_location(
        table: &HashMap<NodeLocation, HashSet<NodeLocation>>,
        precedent: &NodeLocation,
    ) -> HashSet<NodeLocation> {
        match table.get(precedent) {
            None => panic!("Fail"),
            Some(dependents) => dependents.iter().map(|location| location.clone()).collect(),
        }
    }

    #[test]
    fn test_read_flow() {
        let storage = Storage::obtain();
        let uuid = storage.add_ledger();

        let cells = vec![
            //Ref A1
            vec![
                Some("=100%".to_string()),
                Some("=100%".to_string()),
                Some("=A1".to_string()),
            ],
            vec![Some("=A1".to_string())],
            vec![Some("=A1".to_string())],
        ];

        let (result, _) = storage
            .transact_write(uuid, |document| {
                document.add_sheet();

                document
                    .insert_cell_facts(&Coordinate::new(0, 0, 0).unwrap(), cells.clone())
                    .unwrap();

                ()
            })
            .unwrap();

        storage
            .transact_read(&uuid, |doc, context| {
                for (cell_read, coord) in context
                    .read_cells_in_range(
                        doc.sheet_at(0).unwrap(),
                        0,
                        &CellRange::new(0, 2, 0, 2).unwrap(),
                    )
                    .unwrap()
                {
                    match cell_read {
                        None => {
                            assert_coord_ne!(coord, 0, 0, 0);
                            assert_coord_ne!(coord, 0, 1, 0);
                            assert_coord_ne!(coord, 0, 2, 0);
                            assert_coord_ne!(coord, 0, 0, 1);
                            assert_coord_ne!(coord, 0, 0, 2);
                        }
                        Some(cell_read) => assert!(cell_read.is_dirty),
                    }
                }

                ()
            })
            .unwrap();

        result
            .eval_handle
            .join()
            .unwrap_or_else(|_| panic!("Failed join"));

        storage
            .transact_read(&uuid, |doc, context| {
                for (cell_read, coord) in context
                    .read_cells_in_range(
                        doc.sheet_at(0).unwrap(),
                        0,
                        &CellRange::new(0, 2, 0, 2).unwrap(),
                    )
                    .unwrap()
                {
                    match cell_read {
                        None => {
                            assert_coord_ne!(coord, 0, 0, 0);
                            assert_coord_ne!(coord, 0, 1, 0);
                            assert_coord_ne!(coord, 0, 2, 0);
                            assert_coord_ne!(coord, 0, 0, 1);
                            assert_coord_ne!(coord, 0, 0, 2);
                        }
                        Some(cell_read) => assert!(!cell_read.is_dirty),
                    }
                }

                ()
            })
            .unwrap();
    }
}

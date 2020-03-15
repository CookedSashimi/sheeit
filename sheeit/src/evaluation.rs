/// This module contains the main logic for evaluating a Document after a write.
/// It does so by evaluating all the dirty nodes given in a single-threaded manner (Can be improved upon in the future)
/// Extreme care needs to be taken for the evaluation algorithm to work, as the evaluation cycle is
/// a key part of the concurrent-evaluation protocol.
mod nodes;

use crate::dependency;
use crate::dependency::{Node, NodeLocation, NodeType};
use crate::storage::{EvalWriteContext, Storage};
use crate::ErrorKind;
use nodes::NodeByLocation;

use crate::storage::location::{Coordinate, IntoIteratorWith, RefersToLocation};
use crate::storage::raw_parser::Ref;
use crate::storage::{Cell, CoreDocument, EvalErrorVal, Value};
use sheeit_computation::{EvalContext, EvalErrorKind};

use std::cell;
use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;
use std::thread::JoinHandle;
use std::time::Duration;
use std::{collections, thread};
use uuid::Uuid;

type RcRefCell<T> = Rc<RefCell<T>>;

#[derive(Debug, Clone)]
pub struct EvalCycleContext {
    context: EvalContext,
    // We can use Arc<Mutex<HashSet<Coordinate>>> in a multithreaded eval. Single threaded for now.
    already_eval: RcRefCell<HashSet<Coordinate>>,
    already_eval_node: RcRefCell<HashSet<NodeByLocation>>,
    source_node: Node,
    dirty_nodes: RcRefCell<collections::HashSet<NodeByLocation>>,
    uuid: Uuid,
}

type Precedent = Node;
type Dependent = Node;

pub fn start_eval(
    uuid: Uuid,
    document: CoreDocument,
    dirty_nodes: collections::HashMap<Precedent, Vec<Dependent>>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let document_cell = Rc::new(RefCell::new(document.clone()));
        let already_eval = Rc::new(RefCell::new(HashSet::new()));
        let already_eval_node = Rc::new(RefCell::new(HashSet::new()));

        let dirty_nodes_cell = Rc::new(RefCell::new(dirty_nodes.iter().fold(
            collections::HashSet::new(),
            |mut acc, (precedent, _dependents)| {
                acc.insert(NodeByLocation::from(precedent.clone()));

                acc
            },
        )));

        'precedent: for (dirty_precedent, dirty_dependents) in dirty_nodes.iter() {
            let context = EvalCycleContext {
                uuid,
                context: EvalContext {
                    coord: Coordinate::new(dirty_precedent.location().sheet_index, 0, 0)
                        .expect("Should be valid"), // This is a placeholder that will be replaced by eval_node
                },
                already_eval: already_eval.clone(),
                already_eval_node: already_eval_node.clone(),
                dirty_nodes: dirty_nodes_cell.clone(),
                source_node: dirty_precedent.clone(),
            };

            match eval_node(uuid, &context, document_cell.clone(), dirty_precedent) {
                Ok(()) => {}
                Err(e) => match e {
                    ErrorKind::SourceNodeDirtied => continue 'precedent,
                    _ => continue,
                },
            }

            'dependent: for dirty_dependent in dirty_dependents.iter() {
                let context = EvalCycleContext {
                    uuid,
                    context: EvalContext {
                        coord: Coordinate::new(dirty_dependent.location().sheet_index, 0, 0)
                            .expect("Should be valid"), // This is a placeholder that will be replaced by eval_node
                    },
                    already_eval: already_eval.clone(),
                    already_eval_node: already_eval_node.clone(),
                    dirty_nodes: dirty_nodes_cell.clone(),
                    source_node: dirty_dependent.clone(),
                };

                match eval_node(uuid, &context, document_cell.clone(), dirty_dependent) {
                    Ok(()) => {}
                    Err(e) => match e {
                        ErrorKind::SourceNodeDirtied => continue 'dependent,
                        _ => continue,
                    },
                }
            }
        }
    })
}

fn eval_node(
    uuid: Uuid,
    context: &EvalCycleContext,
    document_cell: Rc<RefCell<CoreDocument>>,
    node: &Node,
) -> Result<(), ErrorKind> {
    {
        let mut already_eval_node = (*context.already_eval_node).borrow_mut();

        if already_eval_node.contains(node.location()) {
            return Ok(());
        }

        already_eval_node.insert(node.clone().into());
    }

    let iter_doc = { (*document_cell).borrow().clone() };

    for sheet_coord in node.location().into_iter_with(&iter_doc, false) {
        let coord = Coordinate::new_with_coord(node.location().sheet_index, sheet_coord.clone())
            .expect("Coordinate during evaluation should be valid.");

        let mut inner_context = context.clone();
        inner_context.context.coord = coord;
        inner_context.source_node = node.clone();

        start_eval_cycle_at_context_coord(uuid, inner_context, document_cell.clone())?;
    }

    // TODO: Handle error. I don't think it should throw here, but we need to provide more careful analysis.
    match Storage::obtain().transact_write_clean_node(uuid, node) {
        Ok(_) => {}
        Err(e) => match e {
            ErrorKind::SourceNodeDirtied => return Err(e),
            _ => {
                panic!(
                    "Unexpected error when writing clean node! Node: {:#?} Error: {:#?}",
                    node, e
                );
            }
        },
    }

    Ok(())
}

fn start_eval_cycle_at_context_coord(
    uuid: Uuid,
    context: EvalCycleContext,
    document_cell: Rc<RefCell<CoreDocument>>,
) -> Result<(), ErrorKind> {
    {
        let mut already_eval = context.already_eval.borrow_mut();

        if already_eval.contains(&context.context.coord) {
            return Ok(());
        } else {
            already_eval.insert(context.context.coord.clone());
        }
    }

    let doc = { (*document_cell).borrow().clone() };
    match doc
        .sheet_at(context.context.coord.sheet())
        .and_then(|sheet| sheet.cell_at(context.context.coord.sheet_coord()))
    {
        None => {
            // This cell doesn't exist, move on.
        }
        Some(cell) => {
            if cell_needs_eval(&context, cell) {
                perform_eval(uuid, &context, document_cell)?;
            }
        }
    };

    Ok(())
}

fn cell_needs_eval(context: &EvalCycleContext, cell: &Cell) -> bool {
    match cell.formula() {
        Some(formula) => {
            let dirty_precedents = (*context.dirty_nodes).borrow();
            let node_location = NodeLocation::from_coord(&context.context.coord)
                .expect("Coordinate should be valid here.");

            if dirty_precedents.contains(&node_location) {
                true
            } else {
                let precedents =
                    dependency::extract_precedents(formula.parsed(), &context.context.coord);

                precedents.into_iter().any(|precedent| {
                    dirty_precedents.contains(&NodeLocation::from_node_type(
                        context.context.coord.sheet(),
                        precedent,
                    ))
                })
            }
        }
        None => false,
    }
}

fn perform_eval(
    uuid: Uuid,
    context: &EvalCycleContext,
    updated_document: Rc<RefCell<CoreDocument>>,
) -> Result<(), ErrorKind> {
    let eval_error: Rc<RefCell<Option<ErrorKind>>> = Rc::new(RefCell::new(None));

    loop {
        // TODO: Figure out how to remove these unnecessary clones...
        let doc_clone = { (*updated_document).borrow().clone() };
        let context_clone = context.clone();
        let updated_document_clone = updated_document.clone();
        let eval_error_clone = eval_error.clone();

        let eval_result = sheeit_computation::eval_cell(
            &context.context,
            doc_clone,
            Rc::new(RefCell::new(move |expr_ref: &Ref, _doc: &CoreDocument| {
                let context = context_clone.clone();
                let updated_document = updated_document_clone.clone();
                let eval_error = eval_error_clone.clone();

                let ranges = match expr_ref.refers_to(context.context.coord.sheet_coord()) {
                    Ok(ranges) => ranges,
                    Err(_e) => {
                        (*eval_error).replace(Some(ErrorKind::NotFoundError));
                        return Err(EvalErrorKind::RefEvalError);
                    }
                };

                for range in ranges {
                    let node_type: NodeType = range.into();
                    let node_location = NodeLocation {
                        sheet_index: context.context.coord.sheet(),
                        node_type,
                    };

                    let node = {
                        let already_eval_node_borrow: cell::Ref<_> =
                            (*context.already_eval_node).borrow();
                        if already_eval_node_borrow.contains(&node_location) {
                            continue;
                        }

                        match (*context.dirty_nodes).borrow().get(&node_location) {
                            Some(dirty_node) => dirty_node.clone(),
                            None => {
                                // Node is not dirty, we can just use the existing document.
                                let doc_result = { (*updated_document).borrow().clone() };

                                return Ok(doc_result);
                            }
                        }
                    };

                    match eval_node(uuid, &context, updated_document.clone(), node.node()) {
                        Ok(_) => {}
                        Err(e) => {
                            (*eval_error).replace(Some(e));
                            return Err(EvalErrorKind::RefEvalError);
                        }
                    };
                }

                let doc_result = { (*updated_document).borrow().clone() };

                Ok(doc_result)
            })),
        );

        // match eval_result {
        //     Err(ref e) => {
        //         println!(
        //             "{:?} eval result ERROR for cell: {:?}, error: {:#?}",
        //             thread::current().id(),
        //             context.context.coord,
        //             e
        //         );
        //     }
        //     Ok((ref doc, ref res)) => {
        //         println!(
        //             "{:?} eval result SUCCESS for cell: {:?}, sequence: {:#?}, val: {}",
        //             thread::current().id(),
        //             context.context.coord,
        //             doc.sequence(),
        //             res
        //         );
        //     }
        // }

        // TODO: DRY the match arms.
        return match eval_result {
            // I don't know which API is better. Should we trust the document that we mutate ourselves, or the
            // document that eval_cell provides us?
            Ok((_new_doc, val)) => {
                match write_eval_to_store(&uuid, &context, updated_document.clone(), val) {
                    Ok(_res) => Ok(()),
                    Err(ErrorKind::PrecedentStillDirty) | Err(ErrorKind::StaleRead) => {
                        thread::sleep(Duration::from_micros(10));
                        updated_document.replace(Storage::obtain().read(&uuid).unwrap());

                        continue;
                    }
                    Err(e) => Err(e),
                }
            }
            // TODO: Better error handling and mapping of errors
            Err(EvalErrorKind::NotAFormula) => {
                // Evaluated cell is a value cell, nothing to do here.
                Ok(())
            }
            Err(EvalErrorKind::RefEvalError) => {
                // Unwrap must be safe here because we only populate eval_error on returning RefEvalError
                Err((*eval_error).borrow().as_ref().unwrap().clone())
            }
            Err(e) => {
                let val = match e {
                    EvalErrorKind::InvalidSequence => {
                        Value::EvalError(EvalErrorVal::CyclicDependency)
                    }
                    EvalErrorKind::UnsupportedExpression(_) => {
                        Value::EvalError(EvalErrorVal::UnsupportedExpression)
                    }
                    _ => Value::EvalError(EvalErrorVal::Invalid),
                };

                match write_eval_to_store(&uuid, &context, updated_document.clone(), val) {
                    Ok(_res) => Ok(()),
                    Err(ErrorKind::PrecedentStillDirty) | Err(ErrorKind::StaleRead) => {
                        thread::sleep(Duration::from_micros(10));
                        updated_document.replace(Storage::obtain().read(&uuid).unwrap());

                        continue;
                    }
                    Err(e) => Err(e),
                }
            }
        };
    }
}

fn write_eval_to_store(
    uuid: &Uuid,
    context: &EvalCycleContext,
    updated_document: Rc<RefCell<CoreDocument>>,
    val: Value,
) -> Result<(), ErrorKind> {
    let write_context = EvalWriteContext {
        context: &context.context,
        source_node: &context.source_node,
    };

    let sequence = { (*updated_document).borrow().sequence() };

    let _val_clone = val.clone();
    let write_result = Storage::obtain().transact_write_eval(uuid, &write_context, sequence, val);
    match write_result {
        Ok(result) => {
            // println!(
            //     "{:?} coord: {:?} val: {}, transact write eval result OK, dirtied source: {:#?},\nC1:{:#?}",
            //     thread::current().id(),
            //     context.context.coord,
            //     val_clone,
            //     result.dirtied_source,
            //     result.document.sheet_at(0).unwrap().cell_at(&SheetCoordinate{row: 0, col:2})
            // );

            updated_document.replace(result.document);

            match result.dirtied_source {
                None => Ok(()),
                Some(_node) => Err(ErrorKind::SourceNodeDirtied),
            }
        }
        Err(e) => {
            // println!(
            //     "{:?} coord: {:?}, val: {} transact write eval result ERROR, error: {}",
            //     thread::current().id(),
            //     context.context.coord,
            //     val_clone,
            //     e
            //
            // );
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::Storage;

    use crate::storage::location::{CellRange, CellRangeView, Coordinate, SheetCoordinate};
    use crate::storage::{EvalErrorVal, Value};
    use std::cmp;

    fn to_values(mut view: CellRangeView) -> Vec<Vec<Value>> {
        view.values().iter_mut().fold(vec![], |mut acc, rows| {
            let mut col = vec![];
            let rows_len = rows.len();

            for i in 0..rows_len {
                col.push(rows.get(i).cloned().unwrap().value().clone())
            }

            acc.push(col);
            acc
        })
    }

    #[test]
    fn test_eval() {
        let storage = Storage::obtain();
        let uuid = storage.add_ledger();

        let col_a = vec![
            Some("=200%".to_string()),
            Some("=1".to_string()),
            Some("2".to_string()),
        ];

        let col_b = vec![Some("=A1:A3".to_string())];

        let (write_result, _) = storage
            .transact_write(uuid, |document| {
                document.add_sheet();
                document
                    .insert_cell_facts(
                        &Coordinate::new(0, 0, 0).unwrap(),
                        vec![col_a.clone(), col_b.clone()],
                    )
                    .unwrap();
                ()
            })
            .expect("Should succeed.");

        write_result
            .eval_handle
            .join()
            .unwrap_or_else(|e| panic!("Join failed. {:#?}", e));

        let doc = storage.read(&uuid).unwrap();
        let view = doc
            .sheet_at(0)
            .unwrap()
            .cells_in_range(&CellRange::new(0, 2, 0, 1).expect("Should succeed"));

        let values = to_values(view);

        assert_eq!(
            values,
            vec![
                vec![Value::Number(2.0), Value::Number(1.0), Value::Number(2.0),],
                vec![
                    Value::Spill(Box::new(vec![vec![
                        Value::Number(2.0),
                        Value::Number(1.0),
                        Value::Number(2.0)
                    ]])),
                    Value::SpillRef(SheetCoordinate::new(0, 1).unwrap()),
                    Value::SpillRef(SheetCoordinate::new(0, 1).unwrap())
                ]
            ]
        )
    }

    #[test]
    fn test_cyclic_eval() {
        let storage = Storage::obtain();
        let uuid = storage.add_ledger();

        let col_a = vec![
            Some("=A2".to_string()),
            Some("=A1".to_string()),
            Some("=A3".to_string()),
            Some("=4".to_string()),
            Some("=A1:A4".to_string()),
        ];

        let (write_result, _) = storage
            .transact_write(uuid, |document| {
                document.add_sheet();
                document
                    .insert_cell_facts(&Coordinate::new(0, 0, 0).unwrap(), vec![col_a.clone()])
                    .unwrap();
                ()
            })
            .expect("Should succeed.");

        write_result
            .eval_handle
            .join()
            .unwrap_or_else(|_| panic!("Join failed."));

        let doc = storage.read(&uuid).unwrap();
        let view = doc
            .sheet_at(0)
            .unwrap()
            .cells_in_range(&CellRange::new(0, 4, 0, 0).expect("Should succeed"));

        let values = to_values(view);

        assert_eq!(
            values,
            vec![vec![
                Value::EvalError(EvalErrorVal::CyclicDependency),
                Value::EvalError(EvalErrorVal::CyclicDependency),
                Value::EvalError(EvalErrorVal::CyclicDependency),
                Value::Number(4.0),
                Value::Spill(Box::new(vec![vec![
                    Value::EvalError(EvalErrorVal::CyclicDependency),
                    Value::EvalError(EvalErrorVal::CyclicDependency),
                    Value::EvalError(EvalErrorVal::CyclicDependency),
                    Value::Number(4.0)
                ]]))
            ]]
        )
    }

    #[test]
    fn test_cycle_eval_multiple_writes() {
        let storage = Storage::obtain();
        let uuid = storage.add_ledger();

        let (write_result, _) = storage
            .transact_write(uuid, |document| {
                document.add_sheet();
                document
                    .insert_cell_facts(
                        &Coordinate::new(0, 0, 0).unwrap(),
                        vec![vec![Some("=A2".to_string())]],
                    )
                    .unwrap();
                ()
            })
            .expect("Should succeed.");

        write_result
            .eval_handle
            .join()
            .unwrap_or_else(|_| panic!("Join failed"));

        let (write_result, _) = storage
            .transact_write(uuid, |document| {
                document.add_sheet();
                document
                    .insert_cell_facts(
                        &Coordinate::new(0, 0, 0).unwrap(),
                        vec![vec![None, Some("=A1".to_string())]],
                    )
                    .unwrap();
                ()
            })
            .expect("Should succeed.");

        write_result
            .eval_handle
            .join()
            .unwrap_or_else(|_| panic!("Join failed."));

        let doc = storage.read(&uuid).unwrap();
        let view = doc
            .sheet_at(0)
            .unwrap()
            .cells_in_range(&CellRange::new(0, 1, 0, 0).expect("Should succeed"));

        let values = to_values(view);

        assert_eq!(
            values,
            vec![vec![
                Value::EvalError(EvalErrorVal::CyclicDependency),
                Value::EvalError(EvalErrorVal::CyclicDependency)
            ]]
        )
    }

    #[test]
    fn test_volatile_eval() {
        let facts = vec![vec![
            Some("=RAND()".to_string()),
            Some("=A1".to_string()),
            Some("=A2".to_string()),
            Some("=5%".to_string()),
        ]];

        let storage = Storage::obtain();
        let uuid = storage.add_ledger();

        let (write_result, _) = storage
            .transact_write(uuid, |document| {
                document.add_sheet();
                document
                    .insert_cell_facts(&Coordinate::new(0, 0, 0).unwrap(), facts.clone())
                    .unwrap();
                ()
            })
            .expect("Should succeed.");

        write_result
            .eval_handle
            .join()
            .unwrap_or_else(|_| panic!("Join failed."));

        let doc = storage.read(&uuid).unwrap();

        let a1_value = doc
            .sheet_at(0)
            .unwrap()
            .cell_at(&SheetCoordinate::new(0, 0).unwrap())
            .unwrap()
            .value()
            .clone();

        let mut view = doc
            .sheet_at(0)
            .unwrap()
            .cells_in_range(&CellRange::new(0, 3, 0, 0).expect("Should succeed"));

        let mut max_sequence = 0;
        let mut a4_sequence = 0;

        for rows in view.values().iter_mut() {
            for i in 0..rows.len() {
                let cell = rows.get(i).unwrap();
                let eval_at = cell.formula().unwrap().eval_at();

                if i == 3 {
                    a4_sequence = eval_at;
                } else {
                    assert_eq!(cell.value().clone(), a1_value)
                }

                max_sequence = cmp::max(eval_at, max_sequence);
            }
        }

        // Add some unrelated formula to B1
        let (write_result2, _) = storage
            .transact_write(uuid, |document| {
                document.add_sheet();
                document
                    .insert_cell_facts(
                        &Coordinate::new(0, 0, 0).unwrap(),
                        vec![vec![], vec![Some("=20%".to_string())]],
                    )
                    .unwrap();
                ()
            })
            .expect("Should succeed.");

        write_result2
            .eval_handle
            .join()
            .unwrap_or_else(|_| panic!("Join failed."));

        // Then assert that A4 is not re-evaluated,
        // but A1-A3 are re-evaluated.
        let doc = storage.read(&uuid).unwrap();

        let a1_new_value = doc
            .sheet_at(0)
            .unwrap()
            .cell_at(&SheetCoordinate::new(0, 0).unwrap())
            .unwrap()
            .value()
            .clone();

        let mut view = doc
            .sheet_at(0)
            .unwrap()
            .cells_in_range(&CellRange::new(0, 3, 0, 0).expect("Should succeed"));

        for rows in view.values().iter_mut() {
            for i in 0..rows.len() {
                let cell = rows.get(i).unwrap();
                let eval_at = cell.formula().unwrap().eval_at();

                if i == 3 {
                    assert_eq!(eval_at, a4_sequence);
                } else {
                    assert!(eval_at > max_sequence);
                    assert_eq!(cell.value().clone(), a1_new_value);
                    assert_ne!(cell.value().clone(), a1_value);
                }
            }
        }
    }

    #[test]
    fn test_skip_eval_unnecessary_cell() {
        let facts = vec![
            vec![
                // We will change the precedent (B1)
                Some("=1 + SUM(2, B1)".to_string()),
                // We will modify this cell directly.
                Some("=SUM(1,2)".to_string()),
                // We will not touch this cell.
                Some("=SUM(3,4)".to_string()),
            ],
            vec![
                Some("=B2".to_string()),
                Some("=5".to_string()),
                Some("=SUM(A1:A3)".to_string()),
            ],
        ];

        let storage = Storage::obtain();
        let uuid = storage.add_ledger();

        let (write_result, _) = storage
            .transact_write(uuid, |document| {
                document.add_sheet();
                document
                    .insert_cell_facts(&Coordinate::new(0, 0, 0).unwrap(), facts.clone())
                    .unwrap();
                ()
            })
            .expect("Should succeed.");

        write_result
            .eval_handle
            .join()
            .expect("Eval should succeed");

        let doc = storage.read(&uuid).unwrap();

        let mut cells = doc
            .sheet_at(0)
            .unwrap()
            .cells_in_range(&CellRange::new(0, 2, 0, 1).expect("valid"));
        let a3_cell = cells.values().get_mut(0).unwrap().get(2).unwrap().clone();
        let a3_cell_eval_at = a3_cell.formula().unwrap().eval_at();

        let values = to_values(cells);

        assert_eq!(
            values,
            vec![
                vec![Value::Number(8.0), Value::Number(3.0), Value::Number(7.0)],
                vec![Value::Number(5.0), Value::Number(5.0), Value::Number(18.0)]
            ]
        );

        let (write_result, _) = storage
            .transact_write(uuid, |document| {
                document
                    .insert_cell_facts(
                        &Coordinate::new(0, 1, 1).unwrap(),
                        vec![vec![Some("6".to_string())]],
                    )
                    .unwrap();

                document
                    .insert_cell_facts(
                        &Coordinate::new(0, 1, 0).unwrap(),
                        vec![vec![Some("=SUM(1,3)".to_string())]],
                    )
                    .unwrap();
                ()
            })
            .expect("Should succeed.");

        write_result
            .eval_handle
            .join()
            .expect("Eval should succeed");
        let doc_second_eval = storage.read(&uuid).unwrap();

        let mut cells_second_eval = doc_second_eval
            .sheet_at(0)
            .unwrap()
            .cells_in_range(&CellRange::new(0, 2, 0, 1).expect("valid"));
        let a3_cell_second_eval = cells_second_eval
            .values()
            .get_mut(0)
            .unwrap()
            .get(2)
            .unwrap()
            .clone();
        let a3_cell_eval_at_second_eval = a3_cell_second_eval.formula().unwrap().eval_at();

        let values_second_eval = to_values(cells_second_eval);

        assert_eq!(
            values_second_eval,
            vec![
                vec![Value::Number(9.0), Value::Number(4.0), Value::Number(7.0)],
                vec![Value::Number(6.0), Value::Number(6.0), Value::Number(20.0)]
            ]
        );

        assert_eq!(a3_cell_eval_at_second_eval, a3_cell_eval_at);
    }
}

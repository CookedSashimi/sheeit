use crate::storage::location::Coordinate;
use crate::storage::Sequence;

use bimap::BiHashMap;
use lazy_static::lazy_static;

use super::coordinate::ToEnvelope;
use super::nodes::{Node, NodeLocation};
use rstar::{RTree, RTreeObject};
use std::collections::{HashMap, HashSet};

use crate::dependency::Envelope;
use crate::ErrorKind;

use uuid::Uuid;

pub type Dependents = HashSet<Uuid>;

type DirtiedSequence = Sequence;
type CleanedSequence = Sequence;

type NewNodeLocation = NodeLocation;
type OldNodeLocation = NodeLocation;

pub struct DependencyTable {
    map: HashMap<Uuid, Dependents>,
    rtree: RTree<NodeLocation>,
    dirtied_cleaned: HashMap<Uuid, (DirtiedSequence, CleanedSequence)>,
    ids: BiHashMap<NodeLocation, Uuid>,
}

impl Node {
    // TODO: Use constructor
    // fn new(location: NodeLocation, dirtied_at: Sequence) -> Node {
    //     Node {
    //         id: Uuid::new_v4(),
    //         location,
    //         dirtied_at,
    //     }
    // }

    pub fn location(&self) -> &NodeLocation {
        &self.location
    }

    pub fn id(&self) -> &Uuid {
        &self.id
    }

    pub fn dirtied_at(&self) -> &Sequence {
        &self.dirtied_at
    }
}

lazy_static! {
    static ref EMPTY_DEPENDENT: HashSet<Uuid> = HashSet::new();
}

impl Default for DependencyTable {
    fn default() -> Self {
        DependencyTable {
            map: HashMap::new(),
            rtree: RTree::new(),
            dirtied_cleaned: HashMap::new(),
            ids: BiHashMap::new(),
        }
    }
}

impl DependencyTable {
    pub fn new() -> DependencyTable {
        Default::default()
    }

    pub fn create_node_at(
        &mut self,
        location: &NodeLocation,
        dirtied_at: Sequence,
    ) -> Result<Node, ErrorKind> {
        let id = *self
            .ids
            .get_by_left(location)
            .ok_or_else(|| ErrorKind::NotFoundError)?;

        Ok(Node {
            id,
            dirtied_at,
            location: location.clone(),
        })
    }

    pub fn insert_precedent(&mut self, precedent: &NodeLocation) {
        match self.ids.get_by_left(precedent) {
            Some(id) => {
                // This could've been a dependent. Add it to the precedent map, with the same ID.
                if !self.map.contains_key(id) {
                    self.map.insert(id.clone(), HashSet::new());
                }
            }
            None => {
                let id = Uuid::new_v4();

                self.ids.insert(precedent.clone(), id);
                self.map.insert(id, HashSet::new());
                self.rtree.insert(precedent.clone());

                self.dirtied_cleaned.insert(id, (0, 0));
            }
        }
    }

    pub fn add_dependency(&mut self, precedent: &NodeLocation, dependent: NodeLocation) {
        if !self.ids.contains_left(precedent) {
            self.insert_precedent(precedent);
        }

        if !self.ids.contains_left(&dependent) {
            let id = Uuid::new_v4();
            self.ids.insert(dependent.clone(), id);
            self.rtree.insert(dependent.clone());
            self.dirtied_cleaned.insert(id, (0, 0));
        }

        // We're not mutating ids here, but we need this in order to satisfy the borrow checker.
        // As we need a mutable reference to the map, but the borrow checker doesn't allow for 1 immutable + 1 mutable reference to self,
        // having both of them mutable counts as just 1 mutable reference to self.
        let ids = &mut self.ids;

        let precedent_id = ids
            .get_by_left(precedent)
            .unwrap_or_else(|| panic!("Precedent should have been inserted in id at this point."));

        let dependent_id = ids
            .get_by_left(&dependent)
            .unwrap_or_else(|| panic!("Dependent should have been inserted in id at this point."));

        match self.map.get_mut(precedent_id) {
            Some(dependents) => {
                // Opportunistically remove deleted dependents here.
                // This should be cheap (post-graph compression) since there should be a fixed amount
                // of dependents per precedent.
                dependents
                    .retain(|existing_dependent_id| ids.contains_right(existing_dependent_id));

                dependents.insert(dependent_id.clone());
            }
            None => panic!("Precedent should have been inserted in precedent map at this point"),
        };
    }

    pub fn make_node_dirty(&mut self, node_location: NodeLocation, seq: Sequence) {
        let id = self.ids.get_by_left(&node_location).unwrap_or_else(|| {
            panic!(
                "Unexpectedly trying to dirty a node that has not been inserted yet. Node: {:#?}",
                node_location
            );
        });

        match self.dirtied_cleaned.get_mut(&id) {
            None => panic!(
                "Trying to dirty a node that is not in the dirtied_cleaned set. Node: {:#?}",
                node_location
            ),
            Some((last_dirtied, _cleaned)) => {
                *last_dirtied = seq;
            }
        }
    }

    pub fn make_dirty_precedents_at_point(
        &mut self,
        coord: &Coordinate,
        seq: Sequence,
    ) -> Vec<NodeLocation> {
        let sheet_index = coord.sheet();
        let coordinate = coord.sheet_coord();

        let mut result = vec![];
        let precedents: Vec<_> = self
            .rtree
            .locate_in_envelope_intersecting(&coordinate.envelope().into())
            .filter(|node| {
                node.sheet_index == sheet_index && {
                    let id = self.ids.get_by_left(node).unwrap();

                    self.map.contains_key(id)
                }
            })
            .cloned()
            .collect();

        for precedent_by_location in precedents {
            let precedent = &precedent_by_location;

            let precedent_id = self
                .ids
                .get_by_left(precedent)
                .unwrap_or_else(|| panic!("Precedent exists in RTree but not in ID map."));

            match self.dirtied_cleaned.get_mut(&precedent_id) {
                None => {
                    panic!("Precedent exists in RTree but not in dirtied_cleaned map.");
                }
                Some((last_dirtied, _cleaned)) => {
                    *last_dirtied = seq;
                }
            }

            let dependents = self
                .map
                .get(precedent_id)
                .expect("Precedents should be in sync");

            for dependent_id in dependents {
                match self.dirtied_cleaned.get_mut(dependent_id) {
                    None => {
                        // TODO: This logic needs to change after graph compression.
                        panic!("Dependent exists in precedent map but not in dirtied_cleaned map.")
                    }
                    Some((last_dirtied, _cleaned)) => {
                        *last_dirtied = seq;
                    }
                }
            }

            result.push(precedent.clone());
        }

        result
    }

    pub fn dirty_nodes_at(&self, coord: &Coordinate, seq: Sequence) -> Vec<&NodeLocation> {
        self.rtree
            .locate_in_envelope_intersecting(&coord.sheet_coord().envelope().into())
            .filter(|node| node.sheet_index == coord.sheet())
            .filter(|node| {
                self.ids
                    .get_by_left(node)
                    .and_then(|id| self.dirtied_cleaned.get(id))
                    .map_or(true, |(last_dirtied, _last_cleaned)| *last_dirtied > seq)
            })
            .collect()
    }

    pub fn precedents_at(&self, node_location: &NodeLocation) -> Vec<&NodeLocation> {
        self.rtree
            .locate_in_envelope_intersecting(&node_location.envelope())
            .filter(|precedent| {
                let precedent_id = self.ids.get_by_left(precedent).unwrap();

                self.map.contains_key(precedent_id)
                    && precedent.sheet_index == node_location.sheet_index
            })
            .fold(vec![], |mut acc, precedent| {
                acc.push(&precedent);
                acc
            })
    }

    pub fn dependents_of(&self, node_location: &NodeLocation) -> HashSet<&NodeLocation> {
        self.ids
            .get_by_left(node_location)
            .and_then(|id| self.map.get(id))
            .map(|dependents| {
                dependents
                    .iter()
                    .filter_map(|dependent_id| self.ids.get_by_right(dependent_id))
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_else(HashSet::new)
    }

    pub fn all_dependents(&self) -> HashSet<NodeLocation> {
        self.map
            .iter()
            .flat_map(|(_, dependents)| {
                dependents
                    .iter()
                    .filter_map(|dependent_id| self.ids.get_by_right(dependent_id).cloned())
            })
            .collect()
    }

    // TODO: Figure out how we can return an iterator here.
    pub fn dependents_of_by_id(&self, precedent_id: &Uuid) -> Option<Vec<NodeLocation>> {
        self.map.get(precedent_id).map(|dependents| {
            dependents
                .iter()
                .filter_map(|dependent_id| {
                    // It's possible for dependent to actually be deleted, but still resides within
                    // the precendent-dependents relation. Because when we delete a node, if it's a dependent,
                    // we delete it everywhere, but wont' be able to delete it in the precedents map.
                    self.ids.get_by_right(dependent_id).cloned()
                })
                .collect::<Vec<_>>()
        })
    }

    pub fn has_precedent_with_id(&self, id: &Uuid) -> bool {
        self.map.contains_key(id)
    }

    pub fn precedents_len(&self) -> usize {
        self.map.len()
    }

    pub fn nodes_len(&self) -> usize {
        self.dirtied_cleaned.len()
    }

    pub fn node_by_id(&self, node_id: &Uuid) -> Option<&NodeLocation> {
        self.ids.get_by_right(node_id)
    }

    pub fn node_id(&self, node_location: &NodeLocation) -> Option<&Uuid> {
        self.ids.get_by_left(node_location)
    }

    pub fn node_dirty_clean_state_by_id(
        &self,
        node_id: &Uuid,
    ) -> Option<&(DirtiedSequence, CleanedSequence)> {
        self.dirtied_cleaned.get(node_id)
    }

    pub fn node_dirty_clean_state(
        &self,
        node_location: &NodeLocation,
    ) -> Option<&(DirtiedSequence, CleanedSequence)> {
        self.ids
            .get_by_left(node_location)
            .and_then(|node_id| self.node_dirty_clean_state_by_id(node_id))
    }

    // TODO: Figure out a nicer API to avoid spilling the RTree implementation detail.
    pub fn nodes_in<'a>(&'a self, location: &'a Envelope) -> impl Iterator<Item = &NodeLocation> {
        self.rtree.locate_in_envelope_intersecting(location.into())
    }

    pub fn update_nodes(
        &mut self,
        to_remove: Vec<OldNodeLocation>,
        to_update: Vec<(NewNodeLocation, (OldNodeLocation, Uuid))>,
    ) -> Result<(), ErrorKind> {
        for location in to_remove {
            let id = self
                .ids
                .get_by_left(&location)
                .ok_or_else(|| ErrorKind::NotFoundError)?;

            self.rtree.remove(&location);
            self.map.remove(id);
            self.dirtied_cleaned.remove(id);
            self.ids.remove_by_left(&location);
        }

        let mut newly_added = HashSet::new();
        for (new_location, (old_location, uuid)) in to_update {
            newly_added.insert(new_location.clone());

            self.ids.insert(new_location.clone(), uuid);

            if !newly_added.contains(&old_location) {
                self.rtree.remove(&old_location);
            }

            if !self.rtree.contains(&new_location) {
                self.rtree.insert(new_location.clone());
            }

            // println!("{:?} From {:#?} To {:#?}, rtree now has:", thread::current().id(), old_location, new_location);
            // for node in self.rtree.iter() {
            //     println!("{:?} rtree has: {:#?}", thread::current().id(), node);
            // }
        }

        Ok(())
    }

    pub fn set_node_clean_sequence_by_id(
        &mut self,
        node_id: &Uuid,
        clean_sequence: Sequence,
    ) -> Result<(), ErrorKind> {
        let (_last_dirtied, last_cleaned) = self
            .dirtied_cleaned
            .get_mut(node_id)
            .ok_or_else(|| ErrorKind::NotFoundError)?;

        *last_cleaned = clean_sequence;

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    // #[test]
    // fn test_rtree_insert_remove() {
    //     let node = NodeByLocation
    // }
}

use crate::storage::location::{SheetCoordinate, SheetRange};
use crate::storage::size_check::NonNegativeIsize;
use rstar::{RTreeObject, AABB};

use super::nodes::NodeType;
use crate::dependency::NodeLocation;

#[derive(Debug, Clone)]
pub struct Envelope(AABB<[isize; 2]>);

impl<'a> From<&'a Envelope> for &'a AABB<[isize; 2]> {
    fn from(envelope: &'a Envelope) -> Self {
        &envelope.0
    }
}

impl From<Envelope> for AABB<[isize; 2]> {
    fn from(envelope: Envelope) -> Self {
        envelope.0
    }
}

impl From<AABB<[isize; 2]>> for Envelope {
    fn from(aabb: AABB<[isize; 2]>) -> Self {
        Envelope(aabb)
    }
}

impl From<NodeLocation> for Envelope {
    fn from(node_location: NodeLocation) -> Self {
        Envelope(node_location.node_type.envelope())
    }
}

pub trait ToEnvelope {
    fn envelope(&self) -> Envelope;
}

impl ToEnvelope for SheetCoordinate {
    fn envelope(&self) -> Envelope {
        Envelope(AABB::from_point([
            self.row().ensure().expect("Invalid usize"),
            self.col().ensure().expect("Invalid usize"),
        ]))
    }
}

impl ToEnvelope for SheetRange {
    fn envelope(&self) -> Envelope {
        Envelope(NodeType::from(self.clone()).envelope())
    }
}

use crate::dependency::{Node, NodeLocation};
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone)]
pub struct NodeByLocation {
    node: Node,
    location: NodeLocation,
}

impl NodeByLocation {
    pub(super) fn node(&self) -> &Node {
        &self.node
    }
}

impl PartialEq for NodeByLocation {
    fn eq(&self, other: &Self) -> bool {
        self.location.eq(&other.location)
    }
}

impl Eq for NodeByLocation {}

impl Hash for NodeByLocation {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.location.hash(state);
    }
}

impl From<Node> for NodeByLocation {
    fn from(node: Node) -> Self {
        NodeByLocation {
            location: node.location().clone(),
            node,
        }
    }
}

impl Borrow<NodeLocation> for NodeByLocation {
    fn borrow(&self) -> &NodeLocation {
        &self.location
    }
}

use std::convert::TryInto;

use crate::constants::*;

// enum NodeType {
//     Internal,
//     Leaf,
// }

pub struct LeafNode<'a> {
    buffer: &'a mut [u8],
}

impl<'a> LeafNode<'a> {
    pub fn new(buffer: &'a mut [u8]) -> Self {
        LeafNode { buffer }
    }
}

impl LeafNode<'_> {
    pub fn reset_node_num_cells(&mut self) {
        self.buffer[LEAF_NODE_NUM_CELLS_OFFSET..LEAF_NODE_NUM_CELLS_OFFSET + 4]
            .iter_mut()
            .for_each(|b| *b = 0u8);
    }

    pub fn leaf_node_num_cells(&mut self) -> u32 {
        let num_cells: u32 = u32::from_le_bytes(
            self.buffer[LEAF_NODE_NUM_CELLS_OFFSET..LEAF_NODE_NUM_CELLS_OFFSET + 4]
                .try_into()
                .unwrap(),
        );
        num_cells
    }

    pub fn leaf_node_cell(&mut self, cell_num: usize) -> &mut [u8] {
        &mut self.buffer[LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE..]
    }

    pub fn leaf_node_key(&mut self, cell_num: usize) -> u32 {
        let p_leaf_node_cell = self.leaf_node_cell(cell_num);
        let key: u32 = u32::from_le_bytes(p_leaf_node_cell[..32].try_into().unwrap());
        key
    }

    pub fn leaf_node_value(&mut self, cell_num: usize) -> &mut [u8] {
        let cell = self.leaf_node_cell(cell_num);
        let value = &mut cell[LEAF_NODE_KEY_SIZE..];
        value
    }
}

// #[cfg(test)]
// mod tests {
//     use crate::{btree::LeafNode, constants::PAGE_SIZE};

//     #[test]
//     fn it_works() {
//         use crate::pager::Page;

//         let mut page = Page {
//             buffer: [0u8; PAGE_SIZE],
//         };

//         let ln = LeafNode::new(&mut page.buffer);

//         ln.
//     }
// }

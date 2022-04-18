use core::num;
use std::convert::TryInto;
use std::path::Path;

use crate::pager::{Pager, PagerError};

use crate::constants::*;

pub struct Table {
    pub root_page_num: u32,
    pager: Pager,
}

#[derive(Debug)]
pub enum TableError {
    Pager(PagerError),
    SplitNotImplemented,
    BadPageSize,
}

impl Table {
    pub fn new<P>(filename: P) -> Result<Self, TableError>
    where
        P: AsRef<Path>,
    {
        Pager::new(filename)
            .map_err(TableError::Pager)
            .and_then(|mut pager| {
                if pager.num_pages == 0 {
                    let root_node_page = pager.get_page(0).map_err(TableError::Pager)?;
                    let mut ln = crate::btree::LeafNode::new(&mut root_node_page.buffer);
                    ln.reset_node_num_cells();
                }
                Ok(Table {
                    root_page_num: 0,
                    pager,
                })
            })
    }

    pub fn start(&mut self) -> Result<Cursor, TableError> {
        let page_num = self.root_page_num;
        let page = self.pager.get_page(page_num).map_err(TableError::Pager)?;

        let mut ln = crate::btree::LeafNode::new(&mut page.buffer);
        let num_cells = ln.leaf_node_num_cells();
        Ok(Cursor {
            table: self,
            cell_num: 0,
            page_num,
            end_of_table: num_cells == 0,
        })
    }

    pub fn end(&mut self) -> Result<Cursor, TableError> {
        let page_num = self.root_page_num;
        let page = self.pager.get_page(page_num).map_err(TableError::Pager)?;

        let mut ln = crate::btree::LeafNode::new(&mut page.buffer);
        let num_cells = ln.leaf_node_num_cells();
        Ok(Cursor {
            table: self,
            cell_num: num_cells,
            page_num,
            end_of_table: true,
        })
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        self.pager.flush().iter().for_each(|(sr, wr)| {
            sr.as_ref()
                .expect("dropping table failed to flush pages to disk (seek)");
            wr.as_ref()
                .expect("dropping table failed to flush pages to disk (write)");
        })
    }
}

pub struct Cursor<'table> {
    table: &'table mut Table,
    page_num: u32,
    cell_num: u32,
    pub end_of_table: bool,
}

impl Cursor<'_> {
    pub fn value(&mut self) -> Result<&mut [u8], TableError> {
        let page = self
            .table
            .pager
            .get_page(self.page_num)
            .map_err(TableError::Pager)?;
        let cell = &mut page.buffer
            [LEAF_NODE_HEADER_SIZE + self.cell_num as usize * LEAF_NODE_CELL_SIZE..];
        let value = &mut cell[LEAF_NODE_KEY_SIZE..];
        Ok(value)
    }

    pub fn advance(&mut self) {
        let page = self.table.pager.get_page(self.page_num);
        match page {
            Ok(page) => {
                let mut ln = crate::btree::LeafNode::new(&mut page.buffer);
                let num_cells = ln.leaf_node_num_cells();
                self.cell_num += 1;
                if self.cell_num >= num_cells {
                    self.end_of_table = true;
                }
            }
            _ => {
                // How would this happen?
            }
        }
    }

    pub fn insert(&mut self, key: u32, value: &mut [u8]) -> Result<(), TableError> {
        let page = self
            .table
            .pager
            .get_page(self.page_num)
            .map_err(TableError::Pager)?;

        let mut ln = crate::btree::LeafNode::new(&mut page.buffer);

        let num_cells = ln.leaf_node_num_cells();
        if num_cells as usize > LEAF_NODE_MAX_CELLS {
            return Err(TableError::SplitNotImplemented);
        }

        if self.cell_num < num_cells {
            // we are inserting into the middle of already existing cells
            // so just move everyone over one down to the right
            let mut ln = crate::btree::LeafNode::new(&mut page.buffer);

            for i in (self.cell_num..num_cells).rev() {
                let cell_i: &mut [u8; LEAF_NODE_CELL_SIZE] = ln
                    .leaf_node_cell(i as usize)
                    .try_into()
                    .map_err(|_| TableError::BadPageSize)?;
                let cell_before: &mut [u8; LEAF_NODE_CELL_SIZE] = &mut page.buffer
                    [LEAF_NODE_HEADER_SIZE + self.cell_num as usize * LEAF_NODE_CELL_SIZE..]
                    .try_into()
                    .map_err(|_| TableError::BadPageSize)?; // ln
                                                            // .leaf_node_cell(i as usize - 1)
                                                            // .try_into()
                                                            // .map_err(|_| TableError::BadPageSize)?;
                std::mem::swap(cell_i, cell_before);
            }
        }

        Ok(())
    }
}

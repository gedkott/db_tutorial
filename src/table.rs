use std::path::Path;

use crate::pager::{Pager, PagerError};

use crate::constants::*;

pub struct Table {
    pub num_rows: u32,
    pager: Pager,
}

#[derive(Debug)]
pub enum TableError {
    Pager(PagerError),
}

impl Table {
    pub fn new<P>(filename: P) -> Result<Self, TableError>
    where
        P: AsRef<Path>,
    {
        Pager::new(filename)
            .map_err(TableError::Pager)
            .map(|pager| {
                let num_rows: u32 = (pager.file_length / ROW_SIZE as u64) as u32;
                Table { num_rows, pager }
            })
    }

    pub fn start(&mut self) -> Cursor {
        let end_of_table = self.num_rows == 0;
        Cursor {
            table: self,
            row_num: 0,
            end_of_table,
        }
    }

    pub fn end(&mut self) -> Cursor {
        let row_num = self.num_rows;
        Cursor {
            table: self,
            end_of_table: true,
            row_num,
        }
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        let total_num_rows = self.num_rows as usize;
        let num_full_pages = total_num_rows / ROWS_PER_PAGE;
        let num_additional_rows = total_num_rows % ROWS_PER_PAGE;
        let num_additional_bytes = num_additional_rows * ROW_SIZE;
        self.pager
            .flush(num_full_pages, num_additional_bytes)
            .expect("dropping table failed to flush pages to disk");
    }
}

pub struct Cursor<'table> {
    table: &'table mut Table,
    row_num: u32,
    pub end_of_table: bool,
}

impl Cursor<'_> {
    pub fn value(&mut self) -> Result<&mut [u8], TableError> {
        let page_num = self.row_num / ROWS_PER_PAGE as u32;
        let page = self
            .table
            .pager
            .get_page(page_num)
            .map_err(TableError::Pager)?;
        let row_offset = self.row_num % ROWS_PER_PAGE as u32;
        let byte_offset = row_offset * ROW_SIZE as u32;
        Ok(&mut page.buffer[byte_offset as usize..byte_offset as usize + ROW_SIZE])
    }

    pub fn advance(&mut self) {
        self.row_num += 1;
        if self.row_num >= self.table.num_rows {
            self.end_of_table = true;
        }
    }
}

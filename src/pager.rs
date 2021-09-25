use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

use std::path::Path;

use crate::constants::*;

#[derive(Debug)]
pub struct Page {
    pub buffer: [u8; PAGE_SIZE],
}

pub struct Pager {
    file: File,
    pub file_length: u64,
    pages: HashMap<u32, Page>,
}

#[derive(Debug)]
pub enum PagerError {
    File(std::io::Error),
    PagesFull,
}

impl Pager {
    pub fn new<P>(filename: P) -> Result<Self, PagerError>
    where
        P: AsRef<Path>,
    {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filename)
            .and_then(|mut file| file.seek(SeekFrom::End(0)).map(|len| (file, len)))
            .map(|(file, len)| Pager {
                file,
                pages: HashMap::new(),
                file_length: len,
            })
            .map_err(PagerError::File)
    }

    pub fn get_page(&mut self, page_num: u32) -> Result<&mut Page, PagerError> {
        if page_num > MAX_PAGES as u32 {
            Err(PagerError::PagesFull)
        } else {
            match self.pages.entry(page_num) {
                Entry::Occupied(o) => Ok(o.into_mut()),
                Entry::Vacant(v) => {
                    let mut page = Page {
                        buffer: [0u8; PAGE_SIZE],
                    };

                    let total_num_pages_in_file_now = if self.file_length % PAGE_SIZE as u64 > 0 {
                        // We might save a partial page at the end of the file
                        (self.file_length / PAGE_SIZE as u64) + 1
                    } else {
                        self.file_length / PAGE_SIZE as u64
                    };

                    // if the page number requested is greater than the total num of pages
                    // we have recorded in the file then there is nothing in the file for us to read
                    // this will be true the first time we write to a fresh page and until we first write
                    // to the file for that fresh page (bytes in the new page won't be counted until we write to file/disk)
                    if page_num as u64 <= total_num_pages_in_file_now {
                        self.file
                            .seek(SeekFrom::Start((page_num as usize * PAGE_SIZE) as u64))
                            .map_err(PagerError::File)?;
                        self.file
                            .read_exact(&mut page.buffer)
                            .or_else(|e| match e.kind() {
                                // This means that we could not fill the entire buffer which is fine since we can't (we know its not a full page)
                                std::io::ErrorKind::UnexpectedEof => Ok(()),
                                _ => Err(e),
                            })
                            .map_err(PagerError::File)?;
                    }

                    // return the page buffer whether its totally fresh or had been written to disk before
                    Ok(v.insert(page))
                }
            }
        }
    }

    // the table knows about rows, not the pager; so we expect that data as input
    pub fn flush(
        &mut self,
        num_full_pages: usize,
        num_additional_bytes: usize,
    ) -> Result<(), PagerError> {
        for page_num in 0..num_full_pages {
            let page = match self.pages.get_mut(&(page_num as u32)) {
                Some(p) => p,
                None => {
                    // during a flush, if there is no page in memory, then nothing about that page needs to be flushed
                    // since the user could not possibly have changed it if it was never read into memory
                    continue;
                }
            };
            self.file
                .seek(SeekFrom::Start((page_num as usize * PAGE_SIZE) as u64))
                .map_err(PagerError::File)?;

            self.file
                .write_all(&page.buffer)
                .map_err(PagerError::File)?;
        }

        let last_possible_partial_page = num_full_pages;
        if num_additional_bytes > 0 {
            match self.pages.get_mut(&(last_possible_partial_page as u32)) {
                Some(page) => {
                    self.file
                        .seek(SeekFrom::Start(
                            (last_possible_partial_page as usize * PAGE_SIZE) as u64,
                        ))
                        .map_err(PagerError::File)?;

                    self.file
                        .write_all(&page.buffer[..num_additional_bytes])
                        .or_else(|e| match e.kind() {
                            // I believe this means that
                            std::io::ErrorKind::UnexpectedEof => Ok(()),
                            _ => Err(e),
                        })
                        .map_err(PagerError::File)
                }
                None => {
                    // if the page we are trying to flush isn't in memory then it doesn't need to be flushed
                    Ok(())
                }
            }
        } else {
            Ok(())
        }
    }
}

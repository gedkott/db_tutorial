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
    pub num_pages: u32,
}

#[derive(Debug)]
pub enum PagerError {
    File(std::io::Error),
    PagesFull,
    CorruptFile,
}

fn get_file_with_length(mut file: File) -> std::io::Result<(File, u64)> {
    // https://man7.org/linux/man-pages/man2/lseek.2.html
    let seeker = file.seek(SeekFrom::End(0));
    seeker.map(|len| (file, len))
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
            .and_then(get_file_with_length)
            .map_err(PagerError::File)
            .and_then(|(f, length)| {
                if length as usize % PAGE_SIZE != 0 {
                    Err(PagerError::CorruptFile)
                } else {
                    Ok((f, length))
                }
            })
            .map(|(file, len)| Pager {
                file,
                pages: HashMap::new(),
                file_length: len,
                num_pages: (len as usize / PAGE_SIZE) as u32,
            })
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

                    self.file
                        .seek(SeekFrom::Start((page_num as usize * PAGE_SIZE) as u64))
                        .map_err(PagerError::File)?;

                    self.file
                        .read_exact(&mut page.buffer)
                        .or_else(|e| match e.kind() {
                            // If someone tries to get a page that corresponds to a file portion that responds with UnexpectedEoF when read then we don't have any data there yet and that is normal behavior
                            std::io::ErrorKind::UnexpectedEof => Ok(()),
                            _ => Err(e),
                        })
                        .map_err(PagerError::File)?;

                    if page_num >= self.num_pages {
                        self.num_pages += 1;
                    }

                    // return the page buffer whether its totally fresh or had been written to disk before
                    Ok(v.insert(page))
                }
            }
        }
    }

    pub fn flush(&mut self) -> Vec<(Result<u64, PagerError>, Result<(), PagerError>)> {
        let mut results = vec![];
        for (page_num, page) in self.pages.iter_mut() {
            let seek_res = self
                .file
                .seek(SeekFrom::Start((*page_num as usize * PAGE_SIZE) as u64))
                .map_err(PagerError::File);

            let write_res = self.file.write_all(&page.buffer).map_err(PagerError::File);

            results.push((seek_res, write_res));
        }
        results
    }

    // pub fn flush_page(
    //     &mut self,
    //     page_num: u32,
    // ) -> (Result<u64, PagerError>, Result<(), PagerError>) {
    //     let page = match self.pages.get_mut(&page_num) {
    //         Some(p) => p,
    //         None => {
    //             // was never loaded into memory???
    //             return (Ok(0u64), Ok(()));
    //         }
    //     };
    //     let seek_res = self
    //         .file
    //         .seek(SeekFrom::Start((page_num as usize * PAGE_SIZE) as u64))
    //         .map_err(PagerError::File);

    //     let write_res = self.file.write_all(&page.buffer).map_err(PagerError::File);

    //     (seek_res, write_res)
    // }
}

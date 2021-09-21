use std::array::TryFromSliceError;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::io::{stdin, stdout};
use std::io::{Read, Seek, SeekFrom, Write};
use std::iter::repeat;

use std::path::Path;
use std::str::from_utf8;

const EMAIL_SIZE: usize = std::mem::size_of::<[u8; 255]>();
const USERNAME_SIZE: usize = std::mem::size_of::<[u8; 32]>();
const ID_SIZE: usize = std::mem::size_of::<u32>();
const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * MAX_PAGES;

#[derive(Debug)]
struct Row<'a> {
    id: u32,
    username: &'a [u8],
    email: &'a [u8],
}

enum Statement<'a> {
    Insert { row: Row<'a> },
    Select,
}

const MAX_PAGES: usize = 100;
const PAGE_SIZE: usize = 4096;

#[derive(Debug)]
struct Page {
    buffer: [u8; PAGE_SIZE],
}

struct Pager {
    file: File,
    file_length: u64,
    pages: HashMap<u32, Page>,
}

#[derive(Debug)]
enum PagerError {
    File(std::io::Error),
    PagesFull,
}

impl Pager {
    fn new<P>(filename: P) -> Result<Self, PagerError>
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

    fn get_page(&mut self, page_num: u32) -> Result<&mut Page, PagerError> {
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
    fn flush(
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

struct Table {
    num_rows: u32,
    pager: Pager,
}

#[derive(Debug)]
enum TableError {
    Pager(PagerError),
}

impl Table {
    fn new<P>(filename: P) -> Result<Self, TableError>
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

    fn execute_statement<'a>(
        &'a mut self,
        statement: Statement<'a>,
    ) -> Result<ReplResult, ExecuteError> {
        match statement {
            Statement::Insert { row } => {
                if self.num_rows == TABLE_MAX_ROWS as u32 {
                    Err(ExecuteError::TableFull)
                } else {
                    let bytes = serialize_row(&row);
                    let mut row_buffer = self
                        .get_buffer_for_row_in_page_mut(self.num_rows)
                        .map_err(ExecuteError::Table)?;
                    row_buffer.write_all(&bytes).map_err(ExecuteError::Write)?;
                    self.num_rows += 1;
                    Ok(ReplResult::Success)
                }
            }
            Statement::Select => {
                let mut rows = Vec::new();

                for i in 0..self.num_rows {
                    let row_buffer = self
                        .get_buffer_for_row_in_page(i)
                        .map_err(ExecuteError::Table)?;
                    let sized_row_buffer =
                        (&*row_buffer).try_into().map_err(ExecuteError::RowRead)?;
                    let row = deserialize_row(sized_row_buffer);
                    rows.push(ResultRow {
                        id: row.id,
                        username: row.username.to_owned(),
                        email: row.email.to_owned(),
                    });
                }
                Ok(ReplResult::Rows(rows))
            }
        }
    }

    fn get_buffer_for_row_in_page_mut(&mut self, row_num: u32) -> Result<&mut [u8], TableError> {
        let page_num = row_num / ROWS_PER_PAGE as u32;
        let page = self.pager.get_page(page_num).map_err(TableError::Pager)?;
        let row_offset = row_num % ROWS_PER_PAGE as u32;
        let byte_offset = row_offset * ROW_SIZE as u32;
        Ok(&mut page.buffer[byte_offset as usize..byte_offset as usize + ROW_SIZE])
    }

    fn get_buffer_for_row_in_page(&mut self, row_num: u32) -> Result<&[u8], TableError> {
        let page_num = row_num / ROWS_PER_PAGE as u32;
        let page = self.pager.get_page(page_num).map_err(TableError::Pager)?;
        let row_offset = row_num % ROWS_PER_PAGE as u32;
        let byte_offset = row_offset * ROW_SIZE as u32;
        Ok(&page.buffer[byte_offset as usize..byte_offset as usize + ROW_SIZE])
    }

    fn start(&mut self) -> Cursor {
        let end_of_table = self.num_rows == 0;
        Cursor {
            table: self,
            row_num: 0,
            end_of_table,
        }
    }

    fn end(&mut self) -> Cursor {
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

#[derive(Debug)]
enum StatementError {
    Sql,
    TooLong,
    InvalidId,
}

#[derive(Debug)]
enum ExecuteError {
    RowRead(TryFromSliceError),
    Write(std::io::Error),
    TableFull,
    Table(TableError),
}

struct Cursor<'a> {
    table: &'a mut Table,
    row_num: u32,
    end_of_table: bool,
}

impl<'a> Cursor<'a> {
    fn value(&'a mut self) -> Result<&'a mut [u8], TableError> {
        self.table.get_buffer_for_row_in_page_mut(self.row_num)
    }

    fn advance(&'a mut self) {
        self.row_num += 1;
        if self.row_num >= self.table.num_rows {
            self.end_of_table = true;
        }
    }
}

#[derive(Debug)]
enum ReplResult {
    Rows(Vec<ResultRow>),
    Success,
}

enum ReplAction<'a> {
    Exit,
    Statement { original_input: &'a str },
    Unsupported { message: String },
}

#[derive(Debug)]
enum ReplErr {
    IOErr(std::io::Error),
    Execute(ExecuteError),
    Statement(StatementError),
}

fn main() {
    // parse command line args
    let args: Vec<String> = std::env::args().collect();
    let database_file_name = args.get(1).expect("must provide file name for database");

    // initialize any thing we need for the REPL
    let mut input_buffer = String::new();

    let mut table = Table::new(database_file_name).expect("could not create table");

    // Loop until "exit" input is provided
    loop {
        input_buffer.clear();
        print!("db > ");
        match read_user_input(&mut input_buffer) {
            Ok(input) => match input.into() {
                ReplAction::Exit => break,
                ReplAction::Statement { original_input } => {
                    println!("processing statement {:?}", original_input);
                    match prepare_statement(original_input)
                        .map_err(ReplErr::Statement)
                        .and_then(|s| {
                            match s {
                                Statement::Insert { row: _ } => {
                                    println!("executing insert statement");
                                }
                                Statement::Select => {
                                    println!("executing select statement");
                                }
                            }
                            table.execute_statement(s).map_err(ReplErr::Execute)
                        }) {
                        Ok(results) => match results {
                            ReplResult::Rows(rows) => {
                                rows.iter().for_each(|r| {
                                    println!(
                                        "{:?}, {:?}, {:?}",
                                        r.id,
                                        from_utf8(&r.username).unwrap().trim_matches(char::from(0)),
                                        from_utf8(&r.email).unwrap().trim_matches(char::from(0))
                                    );
                                });
                            }
                            _ => println!("result {:?}", results),
                        },
                        Err(e) => println!("db message: {:?}", &e),
                    }
                }
                ReplAction::Unsupported { message } => println!("db message: {}", &message),
            },
            Err(e) => {
                println!("db message: {:?}", &e)
            }
        }
    }
}

fn serialize_row(row: &Row) -> [u8; ROW_SIZE] {
    let mut buf = [0u8; ROW_SIZE];

    let ibytes = &u32::to_be_bytes(row.id)[..];
    let ubytes = row.username;
    let ebytes = row.email;

    (&mut buf[..USERNAME_OFFSET]).write_all(ibytes).unwrap();

    let num_un_bytes = if USERNAME_SIZE > ubytes.len() {
        USERNAME_SIZE
    } else {
        ubytes.len()
    };
    let num_email_bytes = if EMAIL_SIZE > ebytes.len() {
        EMAIL_SIZE
    } else {
        ebytes.len()
    };

    (&mut buf[USERNAME_OFFSET..USERNAME_OFFSET + num_un_bytes])
        .write_all(ubytes)
        .unwrap();

    if USERNAME_SIZE - num_un_bytes > 0 {
        (&mut buf[USERNAME_OFFSET + num_un_bytes..USERNAME_OFFSET + USERNAME_SIZE])
            .write_all(
                &repeat(0u8)
                    .take(USERNAME_SIZE - num_un_bytes)
                    .collect::<Vec<u8>>(),
            )
            .unwrap();
    }

    (&mut buf[EMAIL_OFFSET..EMAIL_OFFSET + num_email_bytes])
        .write_all(ebytes)
        .unwrap();

    if EMAIL_SIZE - num_email_bytes > 0 {
        (&mut buf[EMAIL_OFFSET + num_email_bytes..ROW_SIZE])
            .write_all(
                &repeat(0u8)
                    .take(EMAIL_SIZE - num_email_bytes)
                    .collect::<Vec<u8>>(),
            )
            .unwrap();
    }

    buf
}

fn deserialize_row(buf: &[u8; ROW_SIZE]) -> Row {
    let id = u32::from_be_bytes(buf[..USERNAME_OFFSET].try_into().unwrap());
    let username = &buf[USERNAME_OFFSET..EMAIL_OFFSET];
    let email = &buf[EMAIL_OFFSET..ROW_SIZE];

    Row {
        id,
        username,
        email,
    }
}

#[derive(Debug)]
struct ResultRow {
    id: u32,
    username: Vec<u8>,
    email: Vec<u8>,
}

fn prepare_statement(original_input: &str) -> Result<Statement, StatementError> {
    if original_input.starts_with("insert") {
        let mut parts = original_input.split(' ');
        let id = parts.nth(1);
        let username = parts.next();
        let email = parts.next();
        match (id, username, email) {
            (Some(id), Some(username), Some(email)) => {
                let id = id.parse().map_err(|_| StatementError::InvalidId)?;

                let username = username.as_bytes();
                let email = email.as_bytes();

                if username.len() > USERNAME_SIZE || email.len() > EMAIL_SIZE {
                    Err(StatementError::TooLong)
                } else {
                    Ok(Statement::Insert {
                        row: Row {
                            id,
                            username,
                            email,
                        },
                    })
                }
            }
            _ => Err(StatementError::Sql),
        }
    } else if original_input.starts_with("select") {
        Ok(Statement::Select)
    } else {
        Err(StatementError::Sql)
    }
}

fn read_user_input(input_buffer: &mut String) -> Result<&str, ReplErr> {
    flush_stdout()
        .and_then(|_| stdin().read_line(input_buffer))
        .and_then(move |n| ensure_stdout_newline((n, input_buffer)))
        .map(|s| s.trim())
        .map_err(ReplErr::IOErr)
}

fn flush_stdout() -> Result<(), std::io::Error> {
    stdout().flush()
}

fn ensure_stdout_newline((n, input): (usize, &mut String)) -> Result<&str, std::io::Error> {
    match (n, &input[..]) {
        (0, "") => {
            // Standard EOF scenario; no newline character for sure
            stdout().write_all(b"\n").map(move |_| &input[0..n])
        }
        _ => {
            // when reading EOF at end of *line*, read_line reads EOF twice
            // there will be no new line in this case since EOF is being read
            // so we print a new line to make sure that we don't print anything
            // on the same line as the original prompt; def no newline
            if !input.contains('\n') {
                stdout().write_all(b"\n").map(move |_| &input[0..n])
            } else {
                // input contains a newline (which is prob already handled by stdout)
                // and its not needed for the caller in our application since the newline
                // character is meaningless in the user input buffer
                Ok(&input[0..n - 1])
            }
        }
    }
}

enum MetaCommand {
    Exit,
    Unsupported,
}

impl<'a> From<&'a str> for ReplAction<'a> {
    fn from(s: &'a str) -> Self {
        if let Some('.') = s.chars().next() {
            match s.into() {
                MetaCommand::Exit => ReplAction::Exit,
                MetaCommand::Unsupported => ReplAction::Unsupported {
                    message: format!("command {:?} is unsupported", s),
                },
            }
        } else {
            ReplAction::Statement { original_input: s }
        }
    }
}

impl From<&str> for MetaCommand {
    fn from(s: &str) -> Self {
        match s.trim() {
            ".exit" => MetaCommand::Exit,
            _ => MetaCommand::Unsupported,
        }
    }
}

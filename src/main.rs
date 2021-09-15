use std::array::TryFromSliceError;
use std::collections::HashMap;
use std::convert::TryInto;
use std::io::{stdin, stdout};
use std::io::{Read, Write};
use std::str::from_utf8;

const PAGE_SIZE: usize = 4096;
const EMAIL_SIZE: usize = std::mem::size_of::<[u8; 255]>();
const USERNAME_SIZE: usize = std::mem::size_of::<[u8; 32]>();
const ID_SIZE: usize = std::mem::size_of::<u32>();
const ROW_SIZE: usize = std::mem::size_of::<Row>();
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;
// const TABLE_MAX_PAGES: usize = 100;
// const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

enum MetaCommand {
    Exit,
    Unsupported,
}

enum Statement {
    Insert { row: Row },
    Select,
}

#[derive(Debug)]
struct Row {
    id: u32,
    username: [u8; USERNAME_SIZE],
    email: [u8; EMAIL_SIZE],
}

#[derive(Debug)]
struct Page {
    buffer: [u8; PAGE_SIZE],
}

struct Table {
    num_rows: u32,
    pages: HashMap<u32, Page>,
}

#[derive(Debug)]
enum StatementError {
    Sql,
}

#[derive(Debug)]
enum ExecuteError {
    RowRead(TryFromSliceError),
    Write(std::io::Error),
}

#[derive(Debug)]
enum ReplResult {
    Rows(Vec<Row>),
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
    // initialize any thing we need for the REPL
    let mut input_buffer = String::new();

    let mut table = Table {
        pages: HashMap::new(),
        num_rows: 0,
    };

    // Loop until "exit" input is provided
    loop {
        input_buffer.clear();
        print!("db > ");
        match read_user_input(&mut input_buffer) {
            Ok(input) => match input.into() {
                ReplAction::Exit => break,
                ReplAction::Statement { original_input } => {
                    match prepare_statement(original_input)
                        .map_err(ReplErr::Statement)
                        .and_then(|s| execute_statement(s, &mut table).map_err(ReplErr::Execute))
                    {
                        Ok(results) => match results {
                            ReplResult::Rows(rows) => {
                                rows.iter().for_each(|r| {
                                    println!(
                                        "{:?}, {:?}, {:?}",
                                        r.id,
                                        from_utf8(&r.username[..])
                                            .unwrap()
                                            .trim_matches(char::from(0)),
                                        from_utf8(&r.email[..])
                                            .unwrap()
                                            .trim_matches(char::from(0))
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
    let ubytes = &row.username[..];
    let ebytes = &row.email[..];

    (&mut buf[..USERNAME_OFFSET]).write_all(ibytes).unwrap();
    (&mut buf[USERNAME_OFFSET..EMAIL_OFFSET])
        .write_all(ubytes)
        .unwrap();
    (&mut buf[EMAIL_OFFSET..ROW_SIZE])
        .write_all(ebytes)
        .unwrap();

    buf
}

fn deserialize_row(buf: &[u8; ROW_SIZE]) -> Row {
    let mut ibytes = [0u8; ID_SIZE];
    let mut username = [0u8; USERNAME_SIZE];
    let mut email = [0u8; EMAIL_SIZE];

    (&buf[..USERNAME_OFFSET]).read_exact(&mut ibytes).unwrap();
    (&buf[USERNAME_OFFSET..EMAIL_OFFSET])
        .read_exact(&mut username)
        .unwrap();
    (&buf[EMAIL_OFFSET..ROW_SIZE])
        .read_exact(&mut email)
        .unwrap();

    let id = u32::from_be_bytes(ibytes);

    Row {
        id,
        username,
        email,
    }
}

fn row_slot(table: &mut Table, row_num: u32) -> &mut [u8] {
    let page_num = row_num / ROWS_PER_PAGE as u32;
    let page = table.pages.entry(page_num).or_insert_with(|| Page {
        buffer: [0u8; PAGE_SIZE],
    });
    let row_offset = row_num % ROWS_PER_PAGE as u32;
    let byte_offset = row_offset * ROW_SIZE as u32;
    &mut page.buffer[byte_offset as usize..(byte_offset as usize + ROW_SIZE)]
}

fn execute_statement(statement: Statement, table: &mut Table) -> Result<ReplResult, ExecuteError> {
    match statement {
        Statement::Insert { row } => {
            println!("executing insert statement");
            let bytes = serialize_row(&row);
            let mut point = row_slot(table, table.num_rows);
            point.write_all(&bytes).map_err(ExecuteError::Write)?;
            table.num_rows += 1;
            Ok(ReplResult::Success)
        }
        Statement::Select => {
            println!("executing select statement");
            let mut rows = Vec::new();

            for i in 0..table.num_rows {
                let point = row_slot(table, i);
                let sized_point: &mut [u8; ROW_SIZE] =
                    point.try_into().map_err(ExecuteError::RowRead)?;
                let row = deserialize_row(sized_point);
                rows.push(row);
            }
            Ok(ReplResult::Rows(rows))
        }
    }
}

fn prepare_statement(original_input: &str) -> Result<Statement, StatementError> {
    println!("processing statement {:?}", original_input);
    if original_input.starts_with("insert") {
        let mut parts = original_input.split(' ');
        let id = parts.nth(1);
        let username = parts.next();
        let email = parts.next();
        match (id, username, email) {
            (Some(id), Some(username), Some(email)) => {
                let id = id.parse().map_err(|_| StatementError::Sql)?;

                let mut un = [0u8; USERNAME_SIZE];
                for (i, b) in username.as_bytes().iter().enumerate() {
                    if i == USERNAME_SIZE {
                        break;
                    } else {
                        un[i] = *b;
                    }
                }

                let mut em = [0u8; EMAIL_SIZE];
                for (i, b) in email.as_bytes().iter().enumerate() {
                    if i == EMAIL_SIZE {
                        break;
                    } else {
                        em[i] = *b;
                    }
                }

                Ok(Statement::Insert {
                    row: Row {
                        id,
                        username: un,
                        email: em,
                    },
                })
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

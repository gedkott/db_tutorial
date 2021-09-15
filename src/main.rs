use std::collections::HashMap;
use std::convert::TryInto;
use std::io::{stdin, stdout};
use std::io::{Read, Write};
use std::str::from_utf8;

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
    username: [u8; 32],
    email: [u8; 255],
}

fn serialize_row(row: &Row) -> [u8; 291] {
    let mut buf = [0u8; 291];

    let ibytes = &u32::to_be_bytes(row.id)[..];
    let ubytes = &row.username[..];
    let ebytes = &row.email[..];

    (&mut buf[..4]).write_all(ibytes).unwrap();
    (&mut buf[4..36]).write_all(ubytes).unwrap();
    (&mut buf[36..]).write_all(ebytes).unwrap();

    buf
}

fn deserialize_row(buf: &[u8; 291]) -> Row {
    let mut ibytes = [0u8; 4];
    let mut username = [0u8; 32];
    let mut email = [0u8; 255];

    (&buf[..4]).read_exact(&mut ibytes).unwrap();
    (&buf[4..36]).read_exact(&mut username).unwrap();
    (&buf[36..]).read_exact(&mut email).unwrap();

    let id = u32::from_be_bytes(ibytes);

    Row {
        id,
        username,
        email,
    }
}

#[derive(Debug)]
struct Page {
    buffer: [u8; 4096],
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
                        .and_then(|s| execute_statement(s, &mut table))
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
                            _ => println!("results from statement have arrived {:?}", results),
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

fn execute_statement(
    statement: Statement,
    table: &mut Table,
) -> Result<ReplResult, StatementError> {
    match statement {
        Statement::Insert { row } => {
            println!("executing insert statement");
            let bytes = serialize_row(&row);
            let rows_per_page = 4096 / 291;
            let page_num = table.num_rows / rows_per_page;
            let page = table.pages.entry(page_num).or_insert_with(|| Page {
                buffer: [0u8; 4096],
            });
            let row_offset = table.num_rows % rows_per_page;
            let byte_offset = row_offset * 291;
            let mut point =
                &mut page.buffer[byte_offset as usize..(byte_offset as usize + 291)];
            point.write_all(&bytes).unwrap();
            println!("{:?}", &mut page.buffer[byte_offset as usize..(byte_offset as usize + 291)]);
            table.num_rows += 1;
            Ok(ReplResult::Success)
        }
        Statement::Select => {
            println!("executing select statement");
            let rows_per_page = 4096 / 291;
            let mut rows = Vec::new();

            for i in 0..table.num_rows {
                let page_num = i / rows_per_page;
                let page = &table.pages[&page_num];
                let row_offset = i % rows_per_page;
                let byte_offset = row_offset * 291;
                let point = &page.buffer[byte_offset as usize..byte_offset as usize + 291];
                let sized_point = point.try_into().unwrap();
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

                let mut un = [0u8; 32];
                for (i, b) in username.as_bytes().iter().enumerate() {
                    if i == 32 {
                        break;
                    } else {
                        un[i] = *b;
                    }
                }

                let mut em = [0u8; 255];
                for (i, b) in email.as_bytes().iter().enumerate() {
                    if i == 255 {
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

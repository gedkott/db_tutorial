use std::io::Write;
use std::io::{stdin, stdout};

use std::str::from_utf8;

mod btree;
mod constants;
mod pager;
mod table;
mod virtual_machine;

use table::Table;
use virtual_machine::{
    prepare_statement, ResultRow, Statement, StatementError, VMErr, VMResult, VirtualMachine,
};

enum ReplAction<'a> {
    Exit,
    Statement { original_input: &'a str },
    Unsupported { message: String },
}

#[derive(Debug)]
pub enum ReplResult {
    Rows(Vec<ResultRow>),
    Success,
}

#[derive(Debug)]
enum ReplErr {
    IOErr(std::io::Error),
    Execute(VMErr),
    Statement(StatementError),
}

fn main() {
    // parse command line args
    let args: Vec<String> = std::env::args().collect();
    let database_file_name = args.get(1).expect("must provide file name for database");

    // initialize any thing we need for the REPL
    let mut input_buffer = String::new();

    let mut table = Table::new(database_file_name).expect("could not create table");
    let mut virtual_machine = VirtualMachine { table: &mut table };

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
                            virtual_machine
                                .execute_statement(s)
                                .map_err(ReplErr::Execute)
                        }) {
                        Ok(results) => match results {
                            VMResult::Rows(rows) => {
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

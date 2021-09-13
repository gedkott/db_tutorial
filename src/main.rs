use std::io::Write;
use std::io::{stdin, stdout};

enum MetaCommand {
    Exit,
    Unsupported,
}

enum StatementType {
    Insert,
    Select,
}

struct Statement {
    statement_type: StatementType,
}

#[derive(Debug)]
enum StatementError {
    Sql,
}

#[derive(Debug)]
struct Results {}

enum ReplAction<'a> {
    Exit,
    Statement { original_input: &'a str },
    Unknown { message: String },
}

#[derive(Debug)]
enum ReplErr {
    IOErr(std::io::Error),
}

fn main() {
    // initialize any thing we need for the REPL
    let mut input_buffer = String::new();

    // Loop until "exit" input is provided
    loop {
        input_buffer.clear();
        print!("db > ");
        match read_user_input(&mut input_buffer) {
            Ok(input) => match input.into() {
                ReplAction::Exit => break,
                ReplAction::Statement { original_input } => {
                    match prepare_statement(original_input).and_then(execute_statement) {
                        Ok(results) => {
                            println!("results from statement have arrived {:?}", results)
                        }
                        Err(e) => println!("db message: {:?}", &e),
                    }
                }
                ReplAction::Unknown { message } => println!("db message: {}", &message),
            },
            Err(e) => {
                println!("db message: {:?}", &e)
            }
        }
    }
}

fn execute_statement(statement: Statement) -> Result<Results, StatementError> {
    match statement.statement_type {
        StatementType::Insert => {
            println!("executing insert statement");
            Ok(Results {})
        }
        StatementType::Select => {
            println!("executing select statement");
            Ok(Results {})
        }
    }
}

fn prepare_statement(original_input: &str) -> Result<Statement, StatementError> {
    println!("processing statement {:?}", original_input);
    let statement_type = if original_input.starts_with("insert") {
        Ok(StatementType::Insert)
    } else if original_input.starts_with("select") {
        Ok(StatementType::Select)
    } else {
        Err(StatementError::Sql)
    };
    statement_type.map(|st| Statement { statement_type: st })
}

fn read_user_input(input_buffer: &mut String) -> Result<&str, ReplErr> {
    flush_stdout()
        .and_then(|_| stdin().read_line(input_buffer).map(|n| (n, input_buffer)))
        .and_then(ensure_stdout_newline)
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
                MetaCommand::Unsupported => ReplAction::Unknown {
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

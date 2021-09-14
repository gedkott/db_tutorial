use std::io::Write;
use std::io::{stdin, stdout};

struct Repl {
    input_buffer: String,
    b_tree: Vec<u8>,
}

impl Repl {
    fn prepare_statement(&self) -> Result<Statement, StatementError> {
        let clean_input = self.input_buffer.trim();
        println!("processing statement {:?}", clean_input);
        if clean_input.starts_with("insert") {
            let mut parts = clean_input.split(' ');
            match (parts.nth(1), parts.nth(2), parts.nth(3)) {
                (Some(id), Some(username), Some(email)) => Ok(Statement::Insert {
                    data: (id, username, email),
                }),
                _ => Err(StatementError::Sql),
            }
        } else if clean_input.starts_with("select") {
            Ok(Statement::Select)
        } else {
            Err(StatementError::Sql)
        }
    }
    fn execute_statement(&self, statement: Statement) -> Result<Results, StatementError> {
        match statement {
            Statement::Insert {
                data: (id, name, email),
            } => {
                println!("executing insert statement");
                Ok(Results {})
            }
            Statement::Select => {
                println!("executing select statement");
                Ok(Results {})
            }
        }
    }
    fn read_user_input(&mut self) -> Result<(), ReplErr> {
        print!("db > ");
        self.input_buffer.clear();
        flush_stdout()
            .and_then(|_| stdin().read_line(&mut self.input_buffer))
            .and_then(move |n| ensure_stdout_newline((n, &mut self.input_buffer)))
            .map_err(ReplErr::IOErr)
    }
}

enum MetaCommand {
    Exit,
    Unsupported,
}

enum Statement<'a> {
    Select,
    Insert { data: (&'a str, &'a str, &'a str) },
}

#[derive(Debug)]
enum StatementError {
    Sql,
}

#[derive(Debug)]
struct Results {}

enum ReplAction {
    Exit,
    Statement,
    Unsupported { message: String },
}

#[derive(Debug)]
enum ReplErr {
    IOErr(std::io::Error),
}

fn main() {
    // initialize any thing we need for the REPL
    let input_buffer = String::new();

    // create Repl
    let mut repl = Repl {
        input_buffer,
        b_tree: vec![],
    };

    // Loop until "exit" input is provided
    loop {
        match repl.read_user_input() {
            Ok(()) => match &repl.input_buffer[..].into() {
                ReplAction::Exit => break,
                ReplAction::Statement => {
                    match repl
                        .prepare_statement()
                        .and_then(|s| repl.execute_statement(s))
                    {
                        Ok(results) => {
                            println!("results from statement have arrived {:?}", results)
                        }
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

fn flush_stdout() -> Result<(), std::io::Error> {
    stdout().flush()
}

fn ensure_stdout_newline((n, input): (usize, &mut String)) -> Result<(), std::io::Error> {
    match (n, &input[..]) {
        (0, "") => {
            // Standard EOF scenario; no newline character for sure
            stdout().write_all(b"\n")
        }
        _ => {
            // when reading EOF at end of *line*, read_line reads EOF twice
            // there will be no new line in this case since EOF is being read
            // so we print a new line to make sure that we don't print anything
            // on the same line as the original prompt; def no newline
            if !input.contains('\n') {
                stdout().write_all(b"\n")
            } else {
                Ok(())
            }
        }
    }
}

impl<'a> From<&'a str> for ReplAction {
    fn from(s: &'a str) -> Self {
        if let Some('.') = s.chars().next() {
            match s.into() {
                MetaCommand::Exit => ReplAction::Exit,
                MetaCommand::Unsupported => ReplAction::Unsupported {
                    message: format!("meta command {:?} is unsupported", s),
                },
            }
        } else {
            ReplAction::Statement
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

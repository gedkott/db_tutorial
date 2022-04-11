use std::array::TryFromSliceError;
use std::convert::TryInto;
use std::io::Write;
use std::iter::repeat;

use crate::constants::*;
use crate::table::{Table, TableError};

#[derive(Debug)]
pub struct Row<'a> {
    id: u32,
    username: &'a [u8],
    email: &'a [u8],
}

#[derive(Debug)]
pub struct ResultRow {
    pub id: u32,
    pub username: Vec<u8>,
    pub email: Vec<u8>,
}

pub enum Statement<'a> {
    Insert { row: Row<'a> },
    Select,
}

#[derive(Debug)]
pub enum StatementError {
    Sql,
    TooLong,
    InvalidId,
}

pub struct VirtualMachine<'a> {
    pub table: &'a mut Table,
}

#[derive(Debug)]
pub enum VMErr {
    TableFull,
    RowRead(TryFromSliceError),
    Write(std::io::Error),
    Table(TableError),
}

#[derive(Debug)]
pub enum VMResult {
    Rows(Vec<ResultRow>),
    Success,
}

fn serialize_row(row: &Row) -> [u8; ROW_SIZE] {
    let mut buf = [0u8; ROW_SIZE];

    let ibytes = &u32::to_le_bytes(row.id)[..];
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
    let id = u32::from_le_bytes(buf[..USERNAME_OFFSET].try_into().unwrap());
    let username = &buf[USERNAME_OFFSET..EMAIL_OFFSET];
    let email = &buf[EMAIL_OFFSET..ROW_SIZE];

    Row {
        id,
        username,
        email,
    }
}

pub fn prepare_statement(original_input: &str) -> Result<Statement, StatementError> {
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

impl VirtualMachine<'_> {
    pub fn execute_statement<'a>(
        &'a mut self,
        statement: Statement<'a>,
    ) -> Result<VMResult, VMErr> {
        match statement {
            Statement::Insert { row } => {
                if self.table.num_rows == TABLE_MAX_ROWS as u32 {
                    Err(VMErr::TableFull)
                } else {
                    let mut cursor = self.table.end();
                    let mut row_buffer = cursor.value().map_err(VMErr::Table)?;
                    let bytes = serialize_row(&row);
                    row_buffer.write_all(&bytes).map_err(VMErr::Write)?;
                    self.table.num_rows += 1;
                    Ok(VMResult::Success)
                }
            }
            Statement::Select => {
                let mut rows = Vec::new();
                let mut cursor = self.table.start();

                while !cursor.end_of_table {
                    let row_buffer = cursor.value().map_err(VMErr::Table)?;
                    let sized_row_buffer = (&*row_buffer).try_into().map_err(VMErr::RowRead)?;
                    let row = deserialize_row(sized_row_buffer);
                    rows.push(ResultRow {
                        id: row.id,
                        username: row.username.to_owned(),
                        email: row.email.to_owned(),
                    });
                    cursor.advance();
                }

                Ok(VMResult::Rows(rows))
            }
        }
    }
}

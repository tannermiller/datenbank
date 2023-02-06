use crate::parser::Input;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum Error {}

pub struct ExecResult {
    pub rows_affected: usize,
}

pub fn execute(input: Input) -> Result<ExecResult, Error> {
    todo!()
}

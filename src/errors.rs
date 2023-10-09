use thiserror::*;

#[derive(Error, Debug)]
pub enum HashError {
    #[error("hash error")]
    HashError,
}

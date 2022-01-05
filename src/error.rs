use thiserror::Error;

#[derive(Error, Debug)]
pub enum RucronError {
    #[error("failed to parse argument from ArgStorage, error: {0}")]
    ParseArgsError(String),
    #[error("when run job found error: {0}")]
    RunTimeError(String),
    #[error("when lock found error: {0}")]
    LockError(String),
    #[error("when unlock found error: {0}")]
    UnLockError(String),
}

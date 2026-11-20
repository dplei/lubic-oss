use nom::Needed;
use reqwest::StatusCode;
use std::io;
use thiserror::Error;
use tokio::time::error::Elapsed;

#[derive(Error, Debug)]
pub enum Error {
    #[error("{0}")]
    Custom(String),

    #[error("Invalid or unsupported HLS playlist")]
    HlsInvalidPlaylist,

    #[error(transparent)]
    ElapsedError(#[from] Elapsed),

    #[error("httpflv chunk stream ended before requested frame was complete, buffered={buffered}")]
    HttpFlvIncompleteFrame { buffered: usize },

    #[error("httpflv chunk read timed out, buffered={buffered}")]
    HttpFlvReadTimeout { buffered: usize },

    #[error("httpflv video made no progress for {stalled_ms}ms while data kept arriving")]
    HttpFlvVideoStalled { stalled_ms: u64 },

    #[error("http status client error {status} for url ({url})")]
    HttpStatus { status: StatusCode, url: String },

    #[error(transparent)]
    IOError(#[from] io::Error),

    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),

    #[error(transparent)]
    UrlParseError(#[from] url::ParseError),

    #[error("Parsing {0} requires {1:?} bytes/chars.")]
    NomIncomplete(String, Needed),

    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
}

pub type Result<T> = core::result::Result<T, Error>;

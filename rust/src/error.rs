// Error type for the streamq Rust SDK.
//
// Design: a single Error enum with variants for each error category.
// This is idiomatic Rust, callers match on variants:
//
//   match client.create_topic("payments").await {
//       Ok(_) => {}
//       Err(Error::Conflict(msg)) => { /* already exists */ }
//       Err(Error::NotFound(msg)) => { /* doesn't exist */ }
//       Err(e) => { eprintln!("broker error: {e}"); }
//   }
//
// Why a single Error enum instead of separate types?
// Rust's error handling model favours one error type per crate/module.
// The ? operator works seamlessly with a single enum. Callers who need
// to distinguish errors use pattern matching, no downcasting required.
//
// We implement std::error::Error and Display so this type works with
// any error handling library (anyhow, thiserror, eyre, etc.).

use std::fmt;

/// All errors that can be returned by the streamq SDK.
#[derive(Debug)]
pub enum Error {
    /// The broker returned 404. Typically: topic does not exist.
    NotFound(String),

    /// The broker returned 409. Typically: topic already exists.
    Conflict(String),

    /// The broker returned any other non-2xx status.
    Api { status: u16, message: String },

    /// The consumer was explicitly closed via `consumer.close()`.
    ConsumerClosed,

    /// The consumer exhausted its reconnect attempts.
    MaxReconnects { attempts: usize, message: String },

    /// An HTTP transport error from reqwest.
    Http(reqwest::Error),

    /// A WebSocket transport error from tokio-tungstenite.
    WebSocket(tokio_tungstenite::tungstenite::Error),

    /// A JSON serialisation or deserialisation error.
    Json(serde_json::Error),

    /// A URL parsing error.
    Url(url::ParseError),

    /// Any other error not covered above.
    Other(String),
}

/// Convenience alias — all SDK functions return this Result type.
pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::NotFound(msg) => write!(f, "not found: {msg}"),
            Error::Conflict(msg) => write!(f, "conflict: {msg}"),
            Error::Api { status, message } => write!(f, "API error {status}: {message}"),
            Error::ConsumerClosed => write!(f, "consumer closed"),
            Error::MaxReconnects { attempts, message } => {
                write!(
                    f,
                    "gave up after {attempts} reconnect attempt(s): {message}"
                )
            }
            Error::Http(e) => write!(f, "HTTP error: {e}"),
            Error::WebSocket(e) => write!(f, "WebSocket error: {e}"),
            Error::Json(e) => write!(f, "JSON error: {e}"),
            Error::Url(e) => write!(f, "URL error: {e}"),
            Error::Other(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Http(e) => Some(e),
            Error::WebSocket(e) => Some(e),
            Error::Json(e) => Some(e),
            Error::Url(e) => Some(e),
            _ => None,
        }
    }
}

// From implementations
// These allow the ? operator to work seamlessly with underlying error types.

impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Self {
        Error::Http(e)
    }
}

impl From<tokio_tungstenite::tungstenite::Error> for Error {
    fn from(e: tokio_tungstenite::tungstenite::Error) -> Self {
        Error::WebSocket(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Json(e)
    }
}

impl From<url::ParseError> for Error {
    fn from(e: url::ParseError) -> Self {
        Error::Url(e)
    }
}

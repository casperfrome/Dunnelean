use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Error {
    pub code: String,
    pub message: String,
    pub commit_unknown: bool,
    pub retryable: bool,
}
impl Error {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            commit_unknown: false,
            retryable: false,
        }
    }
    pub fn config(message: impl Into<String>) -> Self {
        Self::new("INVALID_CONFIG", message)
    }
    pub fn unknown(message: impl Into<String>) -> Self {
        Self {
            commit_unknown: true,
            ..Self::new("COMMIT_UNKNOWN", message)
        }
    }
    pub fn cancelled() -> Self {
        Self::new("CANCELLED", "Cancellation requested")
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}
impl std::error::Error for Error {}
impl From<mysql_async::Error> for Error {
    fn from(e: mysql_async::Error) -> Self {
        let retryable =
            matches!(&e, mysql_async::Error::Server(s) if matches!(s.code, 1205 | 1213));
        Self {
            retryable,
            ..Self::new(
                if matches!(e, mysql_async::Error::Io(_)) {
                    "IO"
                } else {
                    "MYSQL"
                },
                e.to_string(),
            )
        }
    }
}
macro_rules! convert {
    ($ty:ty,$code:expr) => {
        impl From<$ty> for Error {
            fn from(e: $ty) -> Self {
                Self::new($code, e.to_string())
            }
        }
    };
}
convert!(arrow::error::ArrowError, "ARROW");
convert!(rusqlite::Error, "STATE_STORE");
convert!(std::io::Error, "IO");
convert!(serde_json::Error, "JSON");
convert!(reqwest::Error, "HTTP");
convert!(tonic::Status, "FLIGHT");
convert!(tonic::transport::Error, "FLIGHT_TRANSPORT");
convert!(arrow_flight::error::FlightError, "FLIGHT");
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = match self.code.as_str() {
            "INVALID_CONFIG" | "JSON" | "TYPE" | "SCHEMA" => StatusCode::BAD_REQUEST,
            "NOT_FOUND" => StatusCode::NOT_FOUND,
            "CONFLICT" | "REQUEST_CANCELLED" | "STATE_STORE_CHANGED" => StatusCode::CONFLICT,
            "BUSY" => StatusCode::TOO_MANY_REQUESTS,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(serde_json::json!({"error":self}))).into_response()
    }
}
pub async fn timeout<T>(
    ms: u64,
    future: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    tokio::time::timeout(std::time::Duration::from_millis(ms), future)
        .await
        .map_err(|_| Error::new("TIMEOUT", format!("Operation exceeded {ms} ms")))?
}

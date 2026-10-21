use axum::{http::StatusCode, response::Json};
use serde::{Deserialize, Serialize};

/// Health check response
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct HealthResponse {
    /// Server status
    #[cfg_attr(feature = "openapi", schema(example = "OK"))]
    pub status: String,
    /// LiveKit worker state; absent when the worker is disabled
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker: Option<crate::worker::WorkerHealth>,
}

/// Health check handler
/// Returns a simple JSON response indicating the server is running
#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        path = "/",
        responses(
            (status = 200, description = "Server is healthy", body = HealthResponse)
        ),
        tag = "health"
    )
)]
pub async fn health_check() -> Result<Json<HealthResponse>, StatusCode> {
    Ok(Json(HealthResponse {
        status: "OK".to_string(),
        worker: crate::worker::health(),
    }))
}

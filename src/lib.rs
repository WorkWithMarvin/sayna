// clippy flags the `#[must_use]` it sees on every `#[async_trait]` method that returns a
// `Result` (`double_must_use`): both the generated future and the return type are must_use,
// so the attribute we never wrote is reported as redundant. Nothing in the traits to change.
#![allow(clippy::double_must_use)]

pub mod auth;
pub mod config;
pub mod core;
pub mod docs;
pub mod errors;
pub mod handlers;
pub mod init;
pub mod livekit;
pub mod middleware;
pub mod routes;
pub mod state;
pub mod utils;
pub mod worker;

// Re-export commonly used items for convenience
pub use config::ServerConfig;
pub use core::*;
pub use errors::app_error::{AppError, AppResult};
pub use errors::auth_error::{AuthError, AuthResult};
pub use state::AppState;

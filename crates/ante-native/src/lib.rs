//! Ante native protocol: RESP2 framing with Ante-specific commands.
//!
//! This is intentionally not a Redis API. It reuses RESP's binary-safe
//! arrays and bulk strings because they are simple to implement and debug.

pub mod client;
pub mod codec;
pub mod protocol;
pub mod server;

pub use client::NativeClient;
pub use codec::Value;
pub use protocol::Command;
pub use server::{serve, serve_until};

/// Maximum size of a single bulk value on the wire.
pub const MAX_BULK_BYTES: usize = 16 * 1024 * 1024;
/// Maximum number of items in one request array.
pub const MAX_ARRAY_ITEMS: usize = 2048;

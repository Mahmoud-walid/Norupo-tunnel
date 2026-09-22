//! Shared domain logic for the Norupo tunnel.
//!
//! This crate is deliberately transport-agnostic: it knows nothing about gRPC
//! or hyper. It owns the vocabulary that the edge server, the agent and the
//! (eventual) control API all have to agree on — routing keys, the distributed
//! routing table, and authentication.

pub mod auth;
pub mod error;
pub mod ids;
pub mod routing;

pub use error::{Error, Result};

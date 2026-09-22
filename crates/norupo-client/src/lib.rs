//! The Norupo agent library.
//!
//! `norupo-client` is both the `norupo` binary and a library, so that
//! integration tests (and anyone embedding a tunnel in their own tool) can
//! drive an agent without shelling out.

pub mod agent;
pub mod config;

pub use agent::{run, AgentEvent, AgentOptions};
pub use config::{Cli, Command, HttpArgs};

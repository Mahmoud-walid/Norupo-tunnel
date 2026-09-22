//! Generated bindings for the Norupo wire protocol (`norupo.v1`).
//!
//! Everything in this crate is produced at build time from
//! `proto/norupo/v1/tunnel.proto` by `tonic-prost-build`. Nothing here should
//! be hand-written: the `.proto` file is the single source of truth for the
//! contract between the edge servers and the agents.

#![allow(clippy::doc_markdown)]

/// The `norupo.v1` package.
pub mod v1 {
    tonic::include_proto!("norupo.v1");
}

pub use v1::*;

/// Wire protocol version implemented by this build.
///
/// The server refuses `Hello` frames carrying a version it cannot speak, which
/// is what lets us evolve the frame set without bricking old agents.
pub const PROTOCOL_VERSION: u32 = 1;

/// Default per-stream flow-control window (256 KiB).
///
/// Large enough that a single fast response does not stall on window updates
/// over a transcontinental link, small enough that ten thousand idle streams
/// cannot pin gigabytes of edge memory.
pub const DEFAULT_WINDOW: u32 = 256 * 1024;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_roundtrip_through_prost() {
        use prost::Message;

        let frame = ClientFrame {
            payload: Some(client_frame::Payload::Hello(Hello {
                token: "tok_test".into(),
                agent_version: "0.1.0".into(),
                protocol_version: PROTOCOL_VERSION,
                metadata: Default::default(),
            })),
        };

        let encoded = frame.encode_to_vec();
        let decoded = ClientFrame::decode(&encoded[..]).expect("decode");
        assert_eq!(frame, decoded);
    }

    #[test]
    fn header_values_may_be_non_utf8() {
        // Header values are `bytes` precisely so that a latin-1 cookie cannot
        // crash the edge. Guard that decision with a test.
        let header = Header {
            name: "x-legacy".into(),
            value: bytes::Bytes::from_static(&[0xff, 0xfe]),
        };
        assert_eq!(header.value.len(), 2);
    }
}

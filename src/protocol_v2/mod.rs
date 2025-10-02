//! Experimental v2 Active Message data structures.
//!
//! These types are not wired into the runtime yet.  They allow us to prototype a
//! split between user payload bytes and structured control metadata while the
//! existing pipeline continues to operate unchanged.

pub mod control;
pub mod message;

pub use control::{
    AcceptanceMetadata, AckMetadata, ControlMetadata, DeliveryMode, ReceiptMetadata,
    ResponseContextMetadata, ResponseMetadata, TransportHints,
};
pub use message::{ActiveMessage, HandlerId, InstanceId, MessageHeader};

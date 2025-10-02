//! Compatibility shims for the legacy `protocols::v2` module.

pub mod control {
    pub use crate::api::control::{
        AcceptanceMetadata, AckMetadata, ControlMetadata, DeliveryMode, ReceiptMetadata,
        ResponseContextMetadata, ResponseMetadata, TransportHints,
    };
}

pub mod message {
    pub use crate::api::message::{ActiveMessage, HandlerId, InstanceId};
}

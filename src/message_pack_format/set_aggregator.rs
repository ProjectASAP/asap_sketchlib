//! MessagePack wire-format for [`crate::wrapper::SetAggregator`].
//!
//! Owns the borrow/owned wire DTO pair and the [`MessagePackCodec`]
//! impl for the wire-format-aligned `SetAggregator` wrapper.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use super::{Error as MsgPackError, MessagePackCodec};
use crate::wrapper::set_aggregator::SetAggregator;

/// Borrowed serialize-side wire DTO for [`crate::wrapper::SetAggregator`].
/// Used to avoid cloning the underlying set on the encode path.
#[derive(Serialize)]
pub(crate) struct StringSetRef<'a> {
    pub values: &'a HashSet<String>,
}

/// Owned deserialize-side wire DTO for [`crate::wrapper::SetAggregator`].
#[derive(Deserialize)]
pub(crate) struct StringSetOwned {
    pub values: HashSet<String>,
}

impl MessagePackCodec for SetAggregator {
    fn to_msgpack(&self) -> Result<Vec<u8>, MsgPackError> {
        let wrapper = StringSetRef {
            values: &self.values,
        };
        Ok(rmp_serde::to_vec(&wrapper)?)
    }

    fn from_msgpack(bytes: &[u8]) -> Result<Self, MsgPackError> {
        let wrapper: StringSetOwned = rmp_serde::from_slice(bytes)?;
        Ok(Self {
            values: wrapper.values,
        })
    }
}

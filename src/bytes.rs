//! Opaque byte payloads.

use std::sync::Arc;

/// Reference-counted opaque application-defined byte payload.
///
/// Cloning this type shares the underlying bytes. This is useful for Raft
/// command payloads and snapshot payloads, which are normally passed around as
/// immutable byte strings.
#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Bytes {
    inner: Arc<[u8]>,
}

impl Bytes {
    /// Makes a new payload from owned bytes.
    pub fn new(bytes: Vec<u8>) -> Self {
        Self {
            inner: Arc::from(bytes),
        }
    }

    /// Makes a new payload from shared bytes without copying.
    pub fn from_arc(bytes: Arc<[u8]>) -> Self {
        Self { inner: bytes }
    }

    /// Returns the payload as bytes.
    pub fn as_slice(&self) -> &[u8] {
        &self.inner
    }

    /// Returns the shared payload.
    pub fn as_arc(&self) -> &Arc<[u8]> {
        &self.inner
    }

    /// Returns the payload length in bytes.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if this payload is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Copies this payload into owned bytes.
    pub fn into_vec(self) -> Vec<u8> {
        self.inner.to_vec()
    }

    /// Converts this payload into shared bytes without copying.
    pub fn into_arc(self) -> Arc<[u8]> {
        self.inner
    }
}

impl AsRef<[u8]> for Bytes {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl From<Vec<u8>> for Bytes {
    fn from(value: Vec<u8>) -> Self {
        Self::new(value)
    }
}

impl From<Arc<[u8]>> for Bytes {
    fn from(value: Arc<[u8]>) -> Self {
        Self::from_arc(value)
    }
}

impl From<&[u8]> for Bytes {
    fn from(value: &[u8]) -> Self {
        Self {
            inner: Arc::from(value),
        }
    }
}

impl From<Bytes> for Arc<[u8]> {
    fn from(value: Bytes) -> Self {
        value.into_arc()
    }
}

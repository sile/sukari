//! Opaque byte payloads.

/// Opaque application-defined byte payload.
#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Bytes {
    inner: Vec<u8>,
}

impl Bytes {
    /// Makes a new payload from bytes.
    pub fn new(bytes: Vec<u8>) -> Self {
        Self { inner: bytes }
    }

    /// Returns the payload as bytes.
    pub fn as_slice(&self) -> &[u8] {
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

    /// Converts this payload into owned bytes.
    pub fn into_vec(self) -> Vec<u8> {
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

impl From<&[u8]> for Bytes {
    fn from(value: &[u8]) -> Self {
        Self::new(value.to_vec())
    }
}

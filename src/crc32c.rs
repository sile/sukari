//! CRC-32C (Castagnoli) checksum.
//!
//! This uses a slice-by-16 table implementation. It stays portable and safe
//! while avoiding the byte-at-a-time cost in segment checksum paths.

const POLYNOMIAL: u32 = 0x82F6_3B78;
const SLICE: usize = 16;
const TABLE: [[u32; 256]; SLICE] = make_table();

const fn make_table() -> [[u32; 256]; SLICE] {
    let mut table = [[0u32; 256]; SLICE];

    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 == 0 {
                crc >> 1
            } else {
                (crc >> 1) ^ POLYNOMIAL
            };
            bit += 1;
        }
        table[0][i] = crc;
        i += 1;
    }

    let mut slice = 1;
    while slice < SLICE {
        let mut i = 0;
        while i < 256 {
            let prev = table[slice - 1][i];
            table[slice][i] = (prev >> 8) ^ table[0][(prev & 0xFF) as usize];
            i += 1;
        }
        slice += 1;
    }

    table
}

/// Streaming CRC-32C checksum.
#[derive(Debug, Clone)]
pub(crate) struct Crc32c {
    state: u32,
}

impl Default for Crc32c {
    fn default() -> Self {
        Self::new()
    }
}

impl Crc32c {
    /// Creates a new CRC-32C accumulator.
    pub(crate) fn new() -> Self {
        Self { state: 0xFFFF_FFFF }
    }

    /// Feeds more bytes into the checksum.
    pub(crate) fn update(&mut self, data: &[u8]) {
        let mut crc = self.state;
        let mut i = 0;
        while i + SLICE <= data.len() {
            let w0 = u32::from_le_bytes(
                data[i..i + 4]
                    .try_into()
                    .expect("slice length is checked above"),
            ) ^ crc;
            let w1 = u32::from_le_bytes(
                data[i + 4..i + 8]
                    .try_into()
                    .expect("slice length is checked above"),
            );
            let w2 = u32::from_le_bytes(
                data[i + 8..i + 12]
                    .try_into()
                    .expect("slice length is checked above"),
            );
            let w3 = u32::from_le_bytes(
                data[i + 12..i + 16]
                    .try_into()
                    .expect("slice length is checked above"),
            );

            crc = TABLE[15][(w0 & 0xFF) as usize]
                ^ TABLE[14][((w0 >> 8) & 0xFF) as usize]
                ^ TABLE[13][((w0 >> 16) & 0xFF) as usize]
                ^ TABLE[12][(w0 >> 24) as usize]
                ^ TABLE[11][(w1 & 0xFF) as usize]
                ^ TABLE[10][((w1 >> 8) & 0xFF) as usize]
                ^ TABLE[9][((w1 >> 16) & 0xFF) as usize]
                ^ TABLE[8][(w1 >> 24) as usize]
                ^ TABLE[7][(w2 & 0xFF) as usize]
                ^ TABLE[6][((w2 >> 8) & 0xFF) as usize]
                ^ TABLE[5][((w2 >> 16) & 0xFF) as usize]
                ^ TABLE[4][(w2 >> 24) as usize]
                ^ TABLE[3][(w3 & 0xFF) as usize]
                ^ TABLE[2][((w3 >> 8) & 0xFF) as usize]
                ^ TABLE[1][((w3 >> 16) & 0xFF) as usize]
                ^ TABLE[0][(w3 >> 24) as usize];
            i += SLICE;
        }

        while i < data.len() {
            crc = (crc >> 8) ^ TABLE[0][((crc ^ u32::from(data[i])) & 0xFF) as usize];
            i += 1;
        }

        self.state = crc;
    }

    /// Returns the current CRC-32C value.
    pub(crate) fn value(&self) -> u32 {
        !self.state
    }
}

/// Computes the CRC-32C checksum of a byte slice.
pub(crate) fn crc32c(data: &[u8]) -> u32 {
    let mut crc = Crc32c::new();
    crc.update(data);
    crc.value()
}

#[cfg(test)]
mod tests {
    use super::Crc32c;

    #[test]
    fn known_vectors() {
        assert_eq!(super::crc32c(b""), 0);
        assert_eq!(super::crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(
            super::crc32c(b"The quick brown fox jumps over the lazy dog"),
            0x2262_0404
        );
    }

    #[test]
    fn streaming_equals_one_shot() {
        let input: Vec<u8> = (0..10_000).map(|i| (i % 251) as u8).collect();
        let one_shot = super::crc32c(&input);
        let mut crc = Crc32c::new();
        for chunk in input.chunks(37) {
            crc.update(chunk);
        }
        assert_eq!(crc.value(), one_shot);
    }

    #[test]
    fn fast_path_matches_byte_at_a_time() {
        let input: Vec<u8> = (0..512).map(|i| (i % 251) as u8).collect();
        for len in 0..=input.len() {
            let data = &input[..len];
            assert_eq!(
                super::crc32c(data),
                crc32c_byte_at_a_time(data),
                "len={len}"
            );
        }
    }

    fn crc32c_byte_at_a_time(data: &[u8]) -> u32 {
        let mut crc = 0xFFFF_FFFF;
        for byte in data {
            crc ^= u32::from(*byte);
            for _ in 0..8 {
                crc = if crc & 1 == 0 {
                    crc >> 1
                } else {
                    (crc >> 1) ^ super::POLYNOMIAL
                };
            }
        }
        !crc
    }
}

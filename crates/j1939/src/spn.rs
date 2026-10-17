#[derive(Debug, Clone, Copy)]
pub(crate) enum Width {
    One,
    Two,
    Four,
}

impl Width {
    fn bytes(self) -> usize {
        match self {
            Width::One => 1,
            Width::Two => 2,
            Width::Four => 4,
        }
    }

    /// Highest valid raw value. Above it are reserved, "error" and "not available" (J1939-71).
    fn max_valid(self) -> u32 {
        match self {
            Width::One => 0xFA,
            Width::Two => 0xFAFF,
            Width::Four => 0xFAFF_FFFF,
        }
    }
}

/// Position and scaling of one signal (SPN) inside an 8-byte J1939 payload.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Spn {
    /// 1-based byte position, as written in the J1939-71 tables.
    pub byte: usize,
    pub width: Width,
    pub resolution: f64,
    pub offset: f64,
}

impl Spn {
    pub fn decode(self, data: &[u8; 8]) -> Option<f64> {
        let raw = u32::from_le_bytes(self.read(data));
        if raw > self.width.max_valid() {
            return None;
        }
        Some(f64::from(raw) * self.resolution + self.offset)
    }

    /// Out-of-range values are clamped. `None` and non-finite values encode as "not available".
    pub fn encode(self, value: Option<f64>, data: &mut [u8; 8]) {
        let raw = match value {
            Some(v) if v.is_finite() => {
                let max = f64::from(self.width.max_valid());
                ((v - self.offset) / self.resolution)
                    .round()
                    .clamp(0.0, max) as u32
            }
            _ => u32::MAX,
        };
        let n = self.width.bytes();
        let start = self.byte - 1;
        data[start..start + n].copy_from_slice(&raw.to_le_bytes()[..n]);
    }

    fn read(self, data: &[u8; 8]) -> [u8; 4] {
        let n = self.width.bytes();
        let start = self.byte - 1;
        let mut buf = [0; 4];
        buf[..n].copy_from_slice(&data[start..start + n]);
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TWO_BYTES: Spn = Spn {
        byte: 2,
        width: Width::Two,
        resolution: 0.5,
        offset: -10.0,
    };

    #[test]
    fn reads_little_endian_at_one_based_position() {
        let data = [0xAA, 0x34, 0x12, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA];
        assert_eq!(
            TWO_BYTES.decode(&data),
            Some(f64::from(0x1234) * 0.5 - 10.0)
        );
    }

    #[test]
    fn writes_only_its_own_bytes() {
        let mut data = [0xAA; 8];
        TWO_BYTES.encode(Some(-10.0), &mut data);
        assert_eq!(data, [0xAA, 0x00, 0x00, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA]);
    }

    #[test]
    fn values_above_valid_range_are_none() {
        for raw in [0xFAFFu16, 0xFB00, 0xFDFF, 0xFE00, 0xFEFF, 0xFF00, 0xFFFF] {
            let mut data = [0xFF; 8];
            data[1..3].copy_from_slice(&raw.to_le_bytes());
            let expected = (raw <= 0xFAFF).then(|| f64::from(raw) * 0.5 - 10.0);
            assert_eq!(TWO_BYTES.decode(&data), expected, "raw = {raw:#06X}");
        }
    }

    #[test]
    fn max_valid_per_width() {
        let cases: [(Width, u32); 3] = [
            (Width::One, 0xFA),
            (Width::Two, 0xFAFF),
            (Width::Four, 0xFAFF_FFFF),
        ];
        for (width, max) in cases {
            let spn = Spn {
                byte: 1,
                width,
                resolution: 1.0,
                offset: 0.0,
            };
            let mut data = [0xFF; 8];
            spn.encode(Some(f64::MAX), &mut data);
            assert_eq!(spn.decode(&data), Some(f64::from(max)));
        }
    }

    #[test]
    fn encode_clamps_out_of_range() {
        let mut data = [0xFF; 8];
        TWO_BYTES.encode(Some(-1000.0), &mut data);
        assert_eq!(TWO_BYTES.decode(&data), Some(-10.0));

        TWO_BYTES.encode(Some(1e9), &mut data);
        assert_eq!(
            TWO_BYTES.decode(&data),
            Some(f64::from(0xFAFF) * 0.5 - 10.0)
        );
    }

    #[test]
    fn none_and_nan_encode_as_not_available() {
        for value in [None, Some(f64::NAN), Some(f64::INFINITY)] {
            let mut data = [0x00; 8];
            TWO_BYTES.encode(value, &mut data);
            assert_eq!(&data[1..3], &[0xFF, 0xFF]);
            assert_eq!(TWO_BYTES.decode(&data), None);
        }
    }
}

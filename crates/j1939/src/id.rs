const PDU2_MIN_PF: u32 = 240;
const GLOBAL_ADDRESS: u8 = 0xFF;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CanId {
    pub priority: u8,
    pub pgn: u32,
    pub source_address: u8,
    /// `Some` only for PDU1 messages, where PS is the destination address.
    pub destination_address: Option<u8>,
}

impl CanId {
    /// Parses a 29-bit extended CAN ID. Bits above bit 28 are ignored.
    pub fn from_raw(raw: u32) -> Self {
        let priority = ((raw >> 26) & 0x7) as u8;
        let data_page = (raw >> 24) & 0x3;
        let pf = (raw >> 16) & 0xFF;
        let ps = (raw >> 8) & 0xFF;
        let source_address = (raw & 0xFF) as u8;

        let (pgn, destination_address) = if pf >= PDU2_MIN_PF {
            ((data_page << 16) | (pf << 8) | ps, None)
        } else {
            ((data_page << 16) | (pf << 8), Some(ps as u8))
        };

        Self {
            priority,
            pgn,
            source_address,
            destination_address,
        }
    }

    /// Builds the 29-bit CAN ID. A PDU1 message without a destination goes to the global address.
    pub fn to_raw(&self) -> u32 {
        let pgn = self.pgn & 0x3_FFFF;
        let pf = (pgn >> 8) & 0xFF;
        let ps = if pf >= PDU2_MIN_PF {
            pgn & 0xFF
        } else {
            u32::from(self.destination_address.unwrap_or(GLOBAL_ADDRESS))
        };

        (u32::from(self.priority & 0x7) << 26)
            | ((pgn & 0x3_FF00) << 8)
            | (ps << 8)
            | u32::from(self.source_address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(priority: u8, pgn: u32, source_address: u8, destination_address: Option<u8>) -> CanId {
        CanId {
            priority,
            pgn,
            source_address,
            destination_address,
        }
    }

    #[test]
    fn parses_eec1() {
        assert_eq!(CanId::from_raw(0x0CF0_0400), id(3, 0xF004, 0x00, None));
    }

    #[test]
    fn parses_et1() {
        assert_eq!(CanId::from_raw(0x18FE_EE00), id(6, 0xFEEE, 0x00, None));
    }

    #[test]
    fn pdu1_ps_is_destination_not_part_of_pgn() {
        assert_eq!(
            CanId::from_raw(0x18EA_17F9),
            id(6, 0xEA00, 0xF9, Some(0x17))
        );
    }

    #[test]
    fn pf_239_is_still_pdu1() {
        assert_eq!(
            CanId::from_raw(0x18EF_2000),
            id(6, 0xEF00, 0x00, Some(0x20))
        );
    }

    #[test]
    fn pf_240_is_pdu2() {
        assert_eq!(CanId::from_raw(0x18F0_2000), id(6, 0xF020, 0x00, None));
    }

    #[test]
    fn data_page_bit_is_part_of_pgn() {
        assert_eq!(CanId::from_raw(0x19FE_EE00), id(6, 0x1_FEEE, 0x00, None));
    }

    #[test]
    fn bits_above_29_are_ignored() {
        assert_eq!(
            CanId::from_raw(0xE000_0000 | 0x0CF0_0400),
            CanId::from_raw(0x0CF0_0400)
        );
    }

    #[test]
    fn raw_round_trip() {
        for raw in [
            0x0CF0_0400,
            0x18FE_EE00,
            0x18EA_17F9,
            0x18EF_2000,
            0x18F0_2000,
            0x19FE_EE00,
            0x1CFE_F3AB,
        ] {
            assert_eq!(CanId::from_raw(raw).to_raw(), raw, "raw = {raw:#010X}");
        }
    }

    #[test]
    fn pdu1_without_destination_goes_to_global_address() {
        assert_eq!(id(6, 0xEA00, 0xF9, None).to_raw(), 0x18EA_FFF9);
    }
}

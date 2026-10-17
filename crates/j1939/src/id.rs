const PDU2_MIN_PF: u32 = 240;

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
}

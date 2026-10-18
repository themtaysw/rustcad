use std::time::SystemTime;

/// One extended (29-bit) CAN data frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub timestamp: SystemTime,
    pub id: u32,
    pub data: Vec<u8>,
}

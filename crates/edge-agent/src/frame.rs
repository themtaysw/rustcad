use std::time::{SystemTime, UNIX_EPOCH};

/// Start of 9999-12-31T23:59:59Z. Later timestamps cannot be written as RFC 3339, which
/// the receiver expects, so they are rejected where frames enter the agent.
pub const LATEST_UNIX_SECS: u64 = 253_402_300_799;

/// One extended (29-bit) CAN data frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub timestamp: SystemTime,
    pub id: u32,
    pub data: Vec<u8>,
}

/// Whether `ts` is between the Unix epoch and the end of year 9999.
#[must_use]
pub fn timestamp_in_range(ts: SystemTime) -> bool {
    ts.duration_since(UNIX_EPOCH)
        .is_ok_and(|since_epoch| since_epoch.as_secs() <= LATEST_UNIX_SECS)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn timestamp_range() {
        assert!(timestamp_in_range(UNIX_EPOCH));
        let last_second = UNIX_EPOCH + Duration::from_secs(LATEST_UNIX_SECS);
        assert!(timestamp_in_range(last_second + Duration::from_millis(999)));
        assert!(!timestamp_in_range(last_second + Duration::from_secs(1)));
        assert!(!timestamp_in_range(UNIX_EPOCH - Duration::from_secs(1)));
    }
}

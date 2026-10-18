use j1939::{CanId, Ccvs, Eec1, Et1, Lfe, Message, VehiclePosition};
use rand::Rng;

use crate::model::Tractor;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub id: u32,
    pub data: Vec<u8>,
}

struct Entry {
    pgn: u32,
    priority: u8,
    every_ticks: u64,
    build: fn(&Tractor) -> Message,
}

/// Steps the tractor model and turns it into CAN frames, each PGN at its own rate.
pub struct Simulator<R> {
    tractor: Tractor,
    rng: R,
    entries: Vec<Entry>,
    tick: u64,
    dt: f64,
    source_address: u8,
    garbage_rate: f64,
}

impl<R: Rng> Simulator<R> {
    /// EEC1 is sent on every tick. The other PGNs use J1939-71 style fixed periods,
    /// but never faster than one tick.
    pub fn new(rate_hz: f64, source_address: u8, garbage_rate: f64, rng: R) -> Self {
        let every = |period_s: f64| ((period_s * rate_hz).round() as u64).max(1);
        let entries = vec![
            Entry {
                pgn: Eec1::PGN,
                priority: 3,
                every_ticks: 1,
                build: |t| Message::Eec1(t.eec1()),
            },
            Entry {
                pgn: Ccvs::PGN,
                priority: 6,
                every_ticks: every(0.1),
                build: |t| Message::Ccvs(t.ccvs()),
            },
            Entry {
                pgn: Lfe::PGN,
                priority: 6,
                every_ticks: every(0.1),
                build: |t| Message::Lfe(t.lfe()),
            },
            Entry {
                pgn: Et1::PGN,
                priority: 6,
                every_ticks: every(1.0),
                build: |t| Message::Et1(t.et1()),
            },
            Entry {
                pgn: VehiclePosition::PGN,
                priority: 6,
                every_ticks: every(1.0),
                build: |t| Message::VehiclePosition(t.position()),
            },
        ];

        Self {
            tractor: Tractor::default(),
            rng,
            entries,
            tick: 0,
            dt: 1.0 / rate_hz,
            source_address,
            garbage_rate,
        }
    }

    pub fn tractor(&self) -> &Tractor {
        &self.tractor
    }

    pub fn tick(&mut self) -> Vec<Frame> {
        self.tractor.step(self.dt, &mut self.rng);

        let mut frames: Vec<Frame> = self
            .entries
            .iter()
            .filter(|e| self.tick.is_multiple_of(e.every_ticks))
            .map(|e| Frame {
                id: self.can_id(e.pgn, e.priority),
                data: (e.build)(&self.tractor).encode().to_vec(),
            })
            .collect();

        if self.rng.random_bool(self.garbage_rate) {
            frames.push(self.garbage());
        }

        self.tick += 1;
        frames
    }

    fn can_id(&self, pgn: u32, priority: u8) -> u32 {
        CanId {
            priority,
            pgn,
            source_address: self.source_address,
            destination_address: None,
        }
        .to_raw()
    }

    /// Traffic a real bus also carries: PGNs we do not decode, broken lengths
    /// and known PGNs whose sender reports "not available".
    fn garbage(&mut self) -> Frame {
        match self.rng.random_range(0..3) {
            0 => {
                let pgn = 0xFF00 | self.rng.random_range(0x00..=0xFF);
                Frame {
                    id: self.can_id(pgn, 6),
                    data: self.rng.random::<[u8; 8]>().to_vec(),
                }
            }
            1 => {
                let len = self.rng.random_range(0..8);
                Frame {
                    id: self.can_id(Eec1::PGN, 3),
                    data: vec![0xFF; len],
                }
            }
            _ => {
                let entry = &self.entries[self.rng.random_range(0..self.entries.len())];
                Frame {
                    id: self.can_id(entry.pgn, entry.priority),
                    data: vec![0xFF; 8],
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::{SeedableRng, rngs::StdRng};

    use super::*;

    fn sim(rate_hz: f64, garbage_rate: f64) -> Simulator<StdRng> {
        Simulator::new(rate_hz, 0x00, garbage_rate, StdRng::seed_from_u64(1))
    }

    fn count_per_pgn(frames: &[Frame]) -> [usize; 5] {
        let pgns = [
            Eec1::PGN,
            Ccvs::PGN,
            Lfe::PGN,
            Et1::PGN,
            VehiclePosition::PGN,
        ];
        pgns.map(|pgn| {
            frames
                .iter()
                .filter(|f| CanId::from_raw(f.id).pgn == pgn)
                .count()
        })
    }

    #[test]
    fn each_pgn_at_its_own_rate() {
        let mut sim = sim(20.0, 0.0);
        let frames: Vec<Frame> = (0..20 * 10).flat_map(|_| sim.tick()).collect();
        assert_eq!(count_per_pgn(&frames), [200, 100, 100, 10, 10]);
    }

    #[test]
    fn slow_rate_sends_everything_every_tick_at_most() {
        let mut sim = sim(2.0, 0.0);
        let frames: Vec<Frame> = (0..10).flat_map(|_| sim.tick()).collect();
        assert_eq!(count_per_pgn(&frames), [10, 10, 10, 5, 5]);
    }

    #[test]
    fn frames_carry_priority_and_source_address() {
        let mut sim = Simulator::new(10.0, 0x2A, 0.0, StdRng::seed_from_u64(1));
        for frame in sim.tick() {
            let id = CanId::from_raw(frame.id);
            assert_eq!(id.source_address, 0x2A);
            let expected_priority = if id.pgn == Eec1::PGN { 3 } else { 6 };
            assert_eq!(id.priority, expected_priority);
        }
        assert_eq!(sim.tick()[0].id, 0x0CF0_042A);
    }

    #[test]
    fn without_garbage_everything_decodes_to_values() {
        let mut sim = sim(20.0, 0.0);
        for frame in (0..20 * 60).flat_map(|_| sim.tick()) {
            let message = j1939::decode(CanId::from_raw(frame.id).pgn, &frame.data)
                .expect("simulator sent an undecodable frame");
            let has_value = match message {
                Message::Eec1(m) => m.engine_speed_rpm.is_some(),
                Message::Et1(m) => m.coolant_temp_c.is_some(),
                Message::Lfe(m) => m.fuel_rate_lph.is_some(),
                Message::Ccvs(m) => m.wheel_speed_kmh.is_some(),
                Message::VehiclePosition(m) => {
                    m.latitude_deg.is_some() && m.longitude_deg.is_some()
                }
            };
            assert!(has_value, "{message:?}");
        }
    }

    #[test]
    fn garbage_rate_one_adds_one_bad_frame_per_tick() {
        let mut clean = sim(20.0, 0.0);
        let mut dirty = sim(20.0, 1.0);
        for _ in 0..100 {
            assert_eq!(dirty.tick().len(), clean.tick().len() + 1);
        }
    }

    #[test]
    fn garbage_covers_all_kinds() {
        let mut sim = sim(20.0, 1.0);
        let (mut unknown, mut bad_length, mut not_available) = (0, 0, 0);
        for frame in (0..300).filter_map(|_| sim.tick().pop()) {
            match j1939::decode(CanId::from_raw(frame.id).pgn, &frame.data) {
                Err(j1939::DecodeError::UnknownPgn(_)) => unknown += 1,
                Err(j1939::DecodeError::InvalidLength { .. }) => bad_length += 1,
                Ok(message) => {
                    assert_eq!(message.encode(), [0xFF; 8]);
                    not_available += 1;
                }
            }
        }
        assert!(unknown > 0 && bad_length > 0 && not_available > 0);
    }
}

use j1939::{Ccvs, Eec1, Et1, Lfe, VehiclePosition};
use rand::Rng;

use crate::field;

const WARMUP_S: f64 = 30.0;
const AMBIENT_C: f64 = 12.0;
const THERMOSTAT_C: f64 = 88.0;

#[derive(Debug, Clone, Copy)]
struct Target {
    speed_kmh: f64,
    rpm: f64,
    load: f64,
}

const IDLE: Target = Target {
    speed_kmh: 0.0,
    rpm: 800.0,
    load: 0.1,
};
const WORKING: Target = Target {
    speed_kmh: 10.0,
    rpm: 2000.0,
    load: 0.75,
};
const TURNING: Target = Target {
    speed_kmh: 4.0,
    rpm: 1500.0,
    load: 0.3,
};

/// A tractor that idles while warming up, then works the field row by row.
#[derive(Debug, Clone)]
pub struct Tractor {
    time_s: f64,
    distance_m: f64,
    speed_kmh: f64,
    rpm: f64,
    load: f64,
    coolant_c: f64,
}

impl Default for Tractor {
    fn default() -> Self {
        Self {
            time_s: 0.0,
            distance_m: 0.0,
            speed_kmh: 0.0,
            rpm: IDLE.rpm,
            load: IDLE.load,
            coolant_c: AMBIENT_C,
        }
    }
}

impl Tractor {
    pub fn step(&mut self, dt: f64, rng: &mut impl Rng) {
        self.time_s += dt;
        let target = self.target();

        let load_target = (target.load + rng.random_range(-0.1..0.1)).clamp(0.0, 1.0);
        self.load = approach(self.load, load_target, dt, 2.0);
        self.rpm = approach(
            self.rpm,
            target.rpm + rng.random_range(-30.0..30.0),
            dt,
            0.5,
        );
        self.speed_kmh = approach(self.speed_kmh, target.speed_kmh, dt, 3.0);
        self.coolant_c = approach(self.coolant_c, THERMOSTAT_C + 6.0 * self.load, dt, 60.0);
        self.distance_m += self.speed_kmh / 3.6 * dt;
    }

    fn target(&self) -> Target {
        if self.time_s < WARMUP_S {
            IDLE
        } else if field::locate(self.distance_m).on_headland {
            TURNING
        } else {
            WORKING
        }
    }

    pub fn fuel_rate_lph(&self) -> f64 {
        2.0 + 28.0 * self.load * self.rpm / 2200.0
    }

    pub fn eec1(&self) -> Eec1 {
        Eec1 {
            engine_speed_rpm: Some(self.rpm),
        }
    }

    pub fn et1(&self) -> Et1 {
        Et1 {
            coolant_temp_c: Some(self.coolant_c),
        }
    }

    pub fn lfe(&self) -> Lfe {
        Lfe {
            fuel_rate_lph: Some(self.fuel_rate_lph()),
        }
    }

    pub fn ccvs(&self) -> Ccvs {
        Ccvs {
            wheel_speed_kmh: Some(self.speed_kmh),
        }
    }

    pub fn position(&self) -> VehiclePosition {
        let (lat, lon) = field::locate(self.distance_m).lat_lon();
        VehiclePosition {
            latitude_deg: Some(lat),
            longitude_deg: Some(lon),
        }
    }

    pub fn status(&self) -> String {
        let (lat, lon) = field::locate(self.distance_m).lat_lon();
        format!(
            "t={:.0}s rpm={:.0} speed={:.1}km/h coolant={:.1}C fuel={:.1}L/h pos={lat:.6},{lon:.6}",
            self.time_s,
            self.rpm,
            self.speed_kmh,
            self.coolant_c,
            self.fuel_rate_lph()
        )
    }
}

/// First-order lag: moves `current` towards `target` with time constant `tau` seconds.
fn approach(current: f64, target: f64, dt: f64, tau: f64) -> f64 {
    current + (target - current) * (1.0 - (-dt / tau).exp())
}

#[cfg(test)]
mod tests {
    use rand::{SeedableRng, rngs::StdRng};

    use super::*;

    const DT: f64 = 0.05;
    const STEPS_PER_SEC: u32 = 20;
    /// `WARMUP_S` in whole seconds, for loop counts.
    const WARMUP: u32 = 30;

    #[test]
    fn test_constants_match_the_model() {
        assert_eq!(f64::from(WARMUP), WARMUP_S);
        assert_eq!(DT * f64::from(STEPS_PER_SEC), 1.0);
    }

    fn run(tractor: &mut Tractor, seconds: u32) {
        let mut rng = StdRng::seed_from_u64(7);
        for _ in 0..seconds * STEPS_PER_SEC {
            tractor.step(DT, &mut rng);
        }
    }

    #[test]
    fn idles_in_place_while_warming_up() {
        let mut tractor = Tractor::default();
        run(&mut tractor, WARMUP - 1);

        assert_eq!(tractor.speed_kmh, 0.0);
        assert_eq!(tractor.distance_m, 0.0);
        assert!((700.0..900.0).contains(&tractor.rpm), "{}", tractor.rpm);
        assert!(tractor.coolant_c > AMBIENT_C + 10.0 && tractor.coolant_c < 60.0);
        assert!((2.0..6.0).contains(&tractor.fuel_rate_lph()));
    }

    #[test]
    fn works_the_field_after_warmup() {
        let mut tractor = Tractor::default();
        run(&mut tractor, WARMUP + 30);

        assert!(
            (9.0..11.0).contains(&tractor.speed_kmh),
            "{}",
            tractor.speed_kmh
        );
        assert!((1850.0..2150.0).contains(&tractor.rpm), "{}", tractor.rpm);
        assert!((15.0..28.0).contains(&tractor.fuel_rate_lph()));
        assert!(tractor.distance_m > 50.0);
    }

    #[test]
    fn slows_down_on_the_headland() {
        let mut tractor = Tractor::default();
        let mut rng = StdRng::seed_from_u64(7);
        let mut headland_speeds = Vec::new();
        for _ in 0..400 * STEPS_PER_SEC {
            tractor.step(DT, &mut rng);
            if field::locate(tractor.distance_m).on_headland {
                headland_speeds.push(tractor.speed_kmh);
            }
        }
        let slowest = headland_speeds.iter().copied().fold(f64::MAX, f64::min);
        assert!(!headland_speeds.is_empty());
        assert!(slowest < 7.0, "{slowest}");
    }

    #[test]
    fn coolant_settles_near_thermostat() {
        let mut tractor = Tractor::default();
        run(&mut tractor, 600);
        assert!(
            (85.0..96.0).contains(&tractor.coolant_c),
            "{}",
            tractor.coolant_c
        );
    }

    #[test]
    fn messages_round_trip_through_j1939() {
        let mut tractor = Tractor::default();
        run(&mut tractor, WARMUP + 10);

        let rpm = Eec1::decode(&tractor.eec1().encode())
            .engine_speed_rpm
            .unwrap();
        assert!((rpm - tractor.rpm).abs() <= 0.125);

        let position = VehiclePosition::decode(&tractor.position().encode());
        let lat = position.latitude_deg.unwrap();
        let lon = position.longitude_deg.unwrap();
        assert!((lat - field::ORIGIN_LAT).abs() < 0.01);
        assert!((lon - field::ORIGIN_LON).abs() < 0.01);
    }
}

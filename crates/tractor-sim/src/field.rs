/// South-west corner of the simulated field, near Prostějov.
pub const ORIGIN_LAT: f64 = 49.4719;
pub const ORIGIN_LON: f64 = 17.1118;

const ROW_LENGTH_M: f64 = 400.0;
const ROW_SPACING_M: f64 = 6.0;
const METERS_PER_DEG_LAT: f64 = 111_320.0;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Location {
    /// Meters east of the origin.
    pub east_m: f64,
    /// Meters north of the origin.
    pub north_m: f64,
    pub on_headland: bool,
}

impl Location {
    pub fn lat_lon(&self) -> (f64, f64) {
        let lat = ORIGIN_LAT + self.north_m / METERS_PER_DEG_LAT;
        let lon = ORIGIN_LON + self.east_m / (METERS_PER_DEG_LAT * ORIGIN_LAT.to_radians().cos());
        (lat, lon)
    }
}

/// Where the tractor is after driving `distance_m` along the field.
///
/// The path goes north along row 0, makes a half-circle turn on the headland,
/// goes south along row 1, turns again, and so on eastwards.
pub fn locate(distance_m: f64) -> Location {
    let radius = ROW_SPACING_M / 2.0;
    let turn_length = std::f64::consts::PI * radius;
    let lap = ROW_LENGTH_M + turn_length;

    let row = (distance_m / lap).floor();
    let along = distance_m - row * lap;
    let northbound = (row as u64).is_multiple_of(2);
    let row_east = row * ROW_SPACING_M;

    if along < ROW_LENGTH_M {
        let north_m = if northbound {
            along
        } else {
            ROW_LENGTH_M - along
        };
        return Location {
            east_m: row_east,
            north_m,
            on_headland: false,
        };
    }

    let angle = (along - ROW_LENGTH_M) / radius;
    let (center_north, sign) = if northbound {
        (ROW_LENGTH_M, 1.0)
    } else {
        (0.0, -1.0)
    };
    Location {
        east_m: row_east + radius - radius * angle.cos(),
        north_m: center_north + sign * radius * angle.sin(),
        on_headland: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_at(location: Location, east_m: f64, north_m: f64) {
        assert!(
            (location.east_m - east_m).abs() < 1e-9 && (location.north_m - north_m).abs() < 1e-9,
            "{location:?} is not at ({east_m}, {north_m})"
        );
    }

    #[test]
    fn starts_at_origin_heading_north() {
        assert_at(locate(0.0), 0.0, 0.0);
        assert_at(locate(100.0), 0.0, 100.0);
        assert!(!locate(100.0).on_headland);
    }

    #[test]
    fn turns_on_the_headland_into_the_next_row() {
        let turn_length = std::f64::consts::PI * ROW_SPACING_M / 2.0;

        let mid_turn = locate(ROW_LENGTH_M + turn_length / 2.0);
        assert!(mid_turn.on_headland);
        assert_at(
            mid_turn,
            ROW_SPACING_M / 2.0,
            ROW_LENGTH_M + ROW_SPACING_M / 2.0,
        );

        let next_row = locate(ROW_LENGTH_M + turn_length + 10.0);
        assert!(!next_row.on_headland);
        assert_at(next_row, ROW_SPACING_M, ROW_LENGTH_M - 10.0);
    }

    #[test]
    fn second_turn_is_at_the_south_end() {
        let turn_length = std::f64::consts::PI * ROW_SPACING_M / 2.0;
        let lap = ROW_LENGTH_M + turn_length;

        let mid_turn = locate(lap + ROW_LENGTH_M + turn_length / 2.0);
        assert!(mid_turn.on_headland);
        assert_at(mid_turn, 1.5 * ROW_SPACING_M, -ROW_SPACING_M / 2.0);
        assert_at(locate(2.0 * lap), 2.0 * ROW_SPACING_M, 0.0);
    }

    #[test]
    fn path_is_continuous() {
        let mut previous = locate(0.0);
        for step in 1..20_000 {
            let current = locate(step as f64 * 0.1);
            let jump = (current.east_m - previous.east_m).hypot(current.north_m - previous.north_m);
            assert!(jump <= 0.1 + 1e-9, "jump of {jump} m at step {step}");
            previous = current;
        }
    }

    #[test]
    fn converts_meters_to_degrees() {
        let (lat, lon) = Location {
            east_m: 0.0,
            north_m: 0.0,
            on_headland: false,
        }
        .lat_lon();
        assert_eq!((lat, lon), (ORIGIN_LAT, ORIGIN_LON));

        let (lat, lon) = Location {
            east_m: 1000.0,
            north_m: 1000.0,
            on_headland: false,
        }
        .lat_lon();
        assert!((lat - ORIGIN_LAT - 0.008_983).abs() < 1e-5);
        assert!((lon - ORIGIN_LON - 0.013_83).abs() < 1e-4);
    }
}

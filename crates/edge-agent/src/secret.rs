use std::fmt;

use serde::Deserialize;

/// A string that never shows up in logs or `Debug` output.
#[derive(Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(transparent)]
pub struct Secret(String);

impl Secret {
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.0.is_empty() {
            "<unset>"
        } else {
            "<redacted>"
        })
    }
}

impl From<String> for Secret {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for Secret {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_never_shows_the_value() {
        assert_eq!(format!("{:?}", Secret::from("s3cret")), "<redacted>");
        assert_eq!(format!("{:?}", Secret::default()), "<unset>");
        assert_eq!(Secret::from("s3cret").expose(), "s3cret");
    }
}

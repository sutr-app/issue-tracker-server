//! Backward-compatible public facade for the workspace split.
//!
//! New code should depend on the layer-specific crates. Existing local
//! consumers can retain the historical `issue-tracker-runner` package name.

pub use issue_tracker_app::*;
pub use issue_tracker_proto as proto;

#[cfg(test)]
mod tests {
    #[test]
    fn facade_exposes_the_historical_server_entry_point() {
        let _: fn(&str) -> Result<issue_tracker_app::server::ServerSettings, String> =
            issue_tracker_app::server::ServerSettings::from_toml;
    }
}

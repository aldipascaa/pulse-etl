//! PulseETL: extract (Open-Meteo / CSV) -> transform -> load (SQLite) -> JSON API + dashboard.

pub mod db;
pub mod error;
pub mod extract;
pub mod model;
pub mod pipeline;
pub mod scheduler;
pub mod server;
pub mod transform;

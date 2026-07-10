//! Concrete eviction policy implementations.

pub mod arc;
pub mod car;
pub mod clock;
pub mod coolhot;
pub mod lirs;
pub mod lru;

pub use arc::ArcPolicy;
pub use car::CarPolicy;
pub use clock::ClockPolicy;
pub use coolhot::CoolHotPolicy;
pub use lirs::LirsPolicy;
pub use lru::LruPolicy;

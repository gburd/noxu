//! Concrete eviction policy implementations.
//!
//! `lru` is the tier-one, JE-faithful default and always compiles.  The
//! scan-resistant alternatives (`clock`, `arc`, `car`, `lirs`, `coolhot`)
//! compile only under the `experimental-eviction-policies` feature.  See
//! `docs/src/reference/eviction-policies.md`.

pub mod lru;
pub use lru::LruPolicy;

#[cfg(feature = "experimental-eviction-policies")]
pub mod arc;
#[cfg(feature = "experimental-eviction-policies")]
pub mod car;
#[cfg(feature = "experimental-eviction-policies")]
pub mod clock;
#[cfg(feature = "experimental-eviction-policies")]
pub mod coolhot;
#[cfg(feature = "experimental-eviction-policies")]
pub mod lirs;

#[cfg(feature = "experimental-eviction-policies")]
pub use arc::ArcPolicy;
#[cfg(feature = "experimental-eviction-policies")]
pub use car::CarPolicy;
#[cfg(feature = "experimental-eviction-policies")]
pub use clock::ClockPolicy;
#[cfg(feature = "experimental-eviction-policies")]
pub use coolhot::CoolHotPolicy;
#[cfg(feature = "experimental-eviction-policies")]
pub use lirs::LirsPolicy;

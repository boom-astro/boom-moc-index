//! Streaming meta-index of HEALPix MOCs in Valkey.
//!
//! Given an alert's sky position, returns all currently-active skymaps
//! (GW localizations, GRB error regions, neutrino tracks) that overlap, in
//! sub-millisecond time. The MOCs themselves live outside the alert
//! database — only the inverted index (HEALPix cell → set of MOC IDs) is
//! kept hot in Valkey.

pub mod index;
pub mod moc;

pub use index::{MocHit, MocIndex, MocMetadata, DEFAULT_INDEX_DEPTH};
pub use moc::{
    degraded_cells_at_depth, is_in_moc, moc_from_fits_bytes, moc_from_skymap_bytes,
    position_to_cell, HpxMoc,
};

//! MOC parsing and inspection utilities.
//!
//! Adapted from boom PR #401 (`add-moc-spatial-query`):
//! <https://github.com/boom-astro/boom/pull/401>
//!
//! What we use here:
//!   - `moc_from_fits_bytes`: parse a precomputed MOC FITS
//!   - `moc_from_skymap_bytes`: threshold a HEALPix skymap at a credible level
//!   - `is_in_moc`: precise point-in-MOC check (used after a Valkey set hit)
//!   - `degraded_cells_at_depth`: list the cells of a MOC degraded to the
//!     meta-index depth, used during index population

use moc::deser::fits::multiordermap::from_fits_multiordermap;
use moc::deser::fits::skymap::from_fits_skymap;
use moc::deser::fits::{from_fits_ivoa, MocIdxType, MocQtyType, MocType};
use moc::moc::range::RangeMOC;
use moc::moc::{CellMOCIntoIterator, CellMOCIterator, HasMaxDepth};
use moc::qty::Hpx;
use std::io::{BufReader, Cursor};

/// Re-export `HasMaxDepth` so binaries can call `moc.depth_max()` without
/// importing the upstream `moc` crate directly (which would conflict with our
/// `boom_moc_index::moc` module path inside binaries).
pub use moc::moc::HasMaxDepth as MocHasMaxDepth;

/// Serialize a `HpxMoc` to IVOA MOC FITS bytes, suitable for caching in Valkey
/// and re-parsing with [`moc_from_fits_bytes`].
pub fn moc_to_fits_bytes(moc: &HpxMoc) -> anyhow::Result<Vec<u8>> {
    use moc::moc::{RangeMOCIntoIterator, RangeMOCIterator};
    let mut buf: Vec<u8> = Vec::new();
    moc.clone()
        .into_range_moc_iter()
        .to_fits_ivoa(None, None, &mut buf)
        .map_err(|e| anyhow::anyhow!("Failed to serialize MOC to FITS: {}", e))?;
    Ok(buf)
}

/// HEALPix MOC type used throughout the codebase.
pub type HpxMoc = RangeMOC<u64, Hpx<u64>>;

/// Parse a MOC from IVOA FITS bytes.
pub fn moc_from_fits_bytes(bytes: &[u8]) -> anyhow::Result<HpxMoc> {
    let reader = BufReader::new(Cursor::new(bytes));
    match from_fits_ivoa(reader) {
        Ok(MocIdxType::U64(MocQtyType::Hpx(MocType::Ranges(moc)))) => {
            Ok(RangeMOC::new(moc.depth_max(), moc.collect()))
        }
        Ok(MocIdxType::U64(MocQtyType::Hpx(MocType::Cells(cell_moc)))) => {
            let depth = cell_moc.depth_max();
            let ranges = cell_moc.into_cell_moc_iter().ranges().collect();
            Ok(RangeMOC::new(depth, ranges))
        }
        Ok(_) => anyhow::bail!("Unexpected MOC type in FITS data"),
        Err(e) => anyhow::bail!("Failed to parse MOC FITS: {}", e),
    }
}

/// Parse an *implicit-indexing* HEALPix skymap (single resolution, one row per
/// pixel in NESTED order) and threshold at the given cumulative credible level.
pub fn moc_from_implicit_skymap_bytes(bytes: &[u8], credible_level: f64) -> anyhow::Result<HpxMoc> {
    let reader = BufReader::new(Cursor::new(bytes));
    from_fits_skymap(
        reader,
        0.0,            // skip_value_le_this
        0.0,            // cumul_from
        credible_level, // cumul_to
        false,          // asc=false: accumulate from highest probability density
        false,          // strict
        false,          // no_split
        false,          // reverse_decent
    )
    .map_err(|e| anyhow::anyhow!("Failed to parse implicit skymap FITS: {}", e))
}

/// Parse a *multi-order* (NUNIQ) HEALPix skymap FITS, e.g. BAYESTAR/LVK
/// localizations, and threshold at the given cumulative credible level.
pub fn moc_from_multiorder_skymap_bytes(
    bytes: &[u8],
    credible_level: f64,
) -> anyhow::Result<HpxMoc> {
    let reader = BufReader::new(Cursor::new(bytes));
    from_fits_multiordermap(
        reader,
        0.0,            // cumul_from
        credible_level, // cumul_to
        false,          // asc
        false,          // strict
        false,          // no_split
        false,          // reverse_decent
    )
    .map_err(|e| anyhow::anyhow!("Failed to parse multi-order skymap FITS: {}", e))
}

/// Parse a HEALPix skymap FITS and threshold at the given cumulative credible
/// level. Auto-detects whether the input is an implicit single-resolution
/// skymap or a multi-order NUNIQ skymap (the BAYESTAR / LVK format).
pub fn moc_from_skymap_bytes(bytes: &[u8], credible_level: f64) -> anyhow::Result<HpxMoc> {
    // Try multi-order first; fall back to implicit. Multi-order parsing fails
    // fast on the header check when the input is not NUNIQ.
    match moc_from_multiorder_skymap_bytes(bytes, credible_level) {
        Ok(m) => Ok(m),
        Err(_) => moc_from_implicit_skymap_bytes(bytes, credible_level),
    }
}

/// Precise point-in-MOC check.
pub fn is_in_moc(moc: &HpxMoc, ra_deg: f64, dec_deg: f64) -> bool {
    let depth = moc.depth_max();
    let ra_rad = ra_deg.to_radians();
    let dec_rad = dec_deg.to_radians();
    let layer = cdshealpix::nested::get(depth);
    let cell = layer.hash(ra_rad, dec_rad);
    moc.contains_cell(depth, cell)
}

/// Return the list of HEALPix cells (NESTED, at `target_depth`) that the MOC
/// touches when degraded to that depth. This is what the meta-index uses to
/// populate the inverted-index Valkey sets.
pub fn degraded_cells_at_depth(moc: &HpxMoc, target_depth: u8) -> Vec<u64> {
    moc.degraded(target_depth)
        .flatten_to_fixed_depth_cells()
        .collect()
}

/// Compute the HEALPix NESTED cell of a sky position at the given depth.
/// This is what the alert side uses to query the meta-index.
pub fn position_to_cell(ra_deg: f64, dec_deg: f64, depth: u8) -> u64 {
    let layer = cdshealpix::nested::get(depth);
    layer.hash(ra_deg.to_radians(), dec_deg.to_radians())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Decode the multi-order skymap bundled at
    /// `tests/fixtures/igwn_gwalert_sample.json` (a captured igwn.gwalert
    /// payload with a base64-inline `event.skymap`). Used by tests that need
    /// real BAYESTAR-style FITS bytes without depending on external data.
    fn fixture_skymap_bytes() -> Vec<u8> {
        use base64::{engine::general_purpose::STANDARD, Engine as _};
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/igwn_gwalert_sample.json"
        );
        let payload = std::fs::read_to_string(path).expect("fixture payload missing");
        let json: serde_json::Value =
            serde_json::from_str(&payload).expect("fixture is valid JSON");
        let b64 = json
            .pointer("/event/skymap")
            .and_then(|v| v.as_str())
            .expect("fixture has /event/skymap");
        STANDARD
            .decode(b64.as_bytes())
            .expect("fixture skymap is valid base64")
    }

    #[test]
    fn parse_bundled_skymap() {
        let bytes = fixture_skymap_bytes();
        let moc = moc_from_skymap_bytes(&bytes, 0.95).expect("parse skymap");
        let coverage = moc.coverage_percentage();
        assert!(coverage > 0.0 && coverage < 1.0);
    }

    #[test]
    fn degraded_cells_grow_with_depth() {
        let bytes = fixture_skymap_bytes();
        let moc = moc_from_skymap_bytes(&bytes, 0.95).expect("parse skymap");
        let cells_d4 = degraded_cells_at_depth(&moc, 4);
        let cells_d6 = degraded_cells_at_depth(&moc, 6);
        assert!(cells_d6.len() >= cells_d4.len());
        assert!(!cells_d4.is_empty());
    }

    #[test]
    fn position_in_its_own_cell() {
        let ra = 120.0_f64;
        let dec = 30.0_f64;
        let depth = 6;
        let cell = position_to_cell(ra, dec, depth);
        let layer = cdshealpix::nested::get(depth);
        let (back_ra_rad, back_dec_rad) = layer.center(cell);
        let dist_deg = ((back_ra_rad.to_degrees() - ra).powi(2)
            + (back_dec_rad.to_degrees() - dec).powi(2))
        .sqrt();
        // Cell at depth 6 is ~1 deg across, so center within 1 deg of the input
        assert!(
            dist_deg < 1.0,
            "cell center too far from input: {}",
            dist_deg
        );
    }

    #[test]
    fn registered_moc_contains_query_position() {
        let bytes = fixture_skymap_bytes();
        let moc = moc_from_skymap_bytes(&bytes, 0.95).expect("parse skymap");
        // Pick the center of the first depth-6 cell the MOC covers; that
        // position must be inside the MOC by construction.
        let cells = degraded_cells_at_depth(&moc, 6);
        let first = *cells.first().expect("MOC covers at least one cell");
        let (ra_rad, dec_rad) = cdshealpix::nested::get(6).center(first);
        assert!(is_in_moc(&moc, ra_rad.to_degrees(), dec_rad.to_degrees()));
    }
}

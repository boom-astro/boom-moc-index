//! Meta-MOC inverted index in Valkey.
//!
//! ## Data model
//!
//! For each registered MOC we maintain three things in Valkey:
//!
//! - **Per-cell SETs (`mocidx:cell:{depth}:{cell_id}` → SET of MOC IDs)** ---
//!   the inverted index. Given an alert's HEALPix cell at the index depth,
//!   `SMEMBERS` returns every MOC that overlaps that cell. This is the hot
//!   path: one round-trip, sub-millisecond.
//!
//! - **Per-MOC FITS (`mocidx:fits:{moc_id}` → bytes)** --- the full MOC, used
//!   for the precise post-check after a Valkey hit. Optional storage location;
//!   the MOC bytes can equally live in an object store, with this key holding
//!   only a URI.
//!
//! - **Per-MOC metadata (`mocidx:meta:{moc_id}` → JSON)** --- type, source
//!   (LVK/Fermi/IceCube), trigger time, validity window, credible level. Read
//!   when a downstream consumer wants more than just the MOC ID.
//!
//! All MOC keys are written with a TTL matching the MOC's validity window
//! (e.g., 14 days for a Fermi-GBM trigger), so expired skymaps fall out
//! automatically without a sweeper.
//!
//! ## Index depth
//!
//! The index is parameterized by a single fixed HEALPix depth. Depth 6
//! (~0.84 deg² cells, 49,152 cells over the full sky) is the default; coarser
//! → fewer cells but more candidates per lookup; finer → tighter pre-filter
//! but more memory in Valkey.

use crate::moc::{degraded_cells_at_depth, is_in_moc, position_to_cell, HpxMoc};
use lru::LruCache;
use parking_lot::Mutex;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Default meta-index depth.
pub const DEFAULT_INDEX_DEPTH: u8 = 6;

/// Metadata stored alongside each registered MOC.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MocMetadata {
    /// MOC source (e.g. "LVK", "Fermi-GBM", "IceCube", "Swift-BAT").
    pub source: String,
    /// Trigger time as ISO-8601 UTC.
    pub trigger_time: String,
    /// Credible level used (e.g. 0.95 for the 95% region).
    pub credible_level: f64,
    /// Validity window in seconds.
    pub validity_seconds: u64,
    /// Sky coverage as a fraction (0..1).
    pub coverage_fraction: f64,
    /// MOC's native max depth.
    pub native_depth: u8,
}

/// A single hit returned by [`MocIndex::lookup`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MocHit {
    pub moc_id: String,
    pub metadata: Option<MocMetadata>,
}

/// Valkey-backed meta-index of HEALPix MOCs.
#[derive(Clone)]
pub struct MocIndex {
    conn: redis::aio::ConnectionManager,
    depth: u8,
    /// In-process LRU cache of parsed MOCs, keyed by moc_id. The MOC FITS
    /// itself is small but `from_fits_ivoa` parsing is the dominant cost on
    /// the precise-check path; caching the parsed `HpxMoc` collapses that
    /// cost to a single hash lookup per hit.
    moc_cache: Arc<Mutex<LruCache<String, Arc<HpxMoc>>>>,
}

impl MocIndex {
    /// Open a connection and return an index handle. Default in-process MOC
    /// cache holds 1024 parsed MOCs; tune via [`Self::with_cache_size`].
    pub async fn open(redis_url: &str, depth: u8) -> anyhow::Result<Self> {
        let client = redis::Client::open(redis_url)?;
        let conn = redis::aio::ConnectionManager::new(client).await?;
        Ok(Self {
            conn,
            depth,
            moc_cache: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(1024).unwrap()))),
        })
    }

    /// Override the in-process MOC cache size.
    pub fn with_cache_size(mut self, cap: usize) -> Self {
        let cap = NonZeroUsize::new(cap.max(1)).unwrap();
        self.moc_cache = Arc::new(Mutex::new(LruCache::new(cap)));
        self
    }

    pub fn depth(&self) -> u8 {
        self.depth
    }

    fn cell_key(&self, cell: u64) -> String {
        format!("mocidx:cell:{}:{}", self.depth, cell)
    }

    fn fits_key(moc_id: &str) -> String {
        format!("mocidx:fits:{}", moc_id)
    }

    fn meta_key(moc_id: &str) -> String {
        format!("mocidx:meta:{}", moc_id)
    }

    /// Register a MOC in the index.
    ///
    /// Writes:
    ///   - The full MOC FITS bytes under `mocidx:fits:{moc_id}` with TTL
    ///   - The metadata JSON under `mocidx:meta:{moc_id}` with TTL
    ///   - One `SADD` per overlapping cell into `mocidx:cell:{depth}:{cell}`
    ///     (each set also TTL'd to expire alongside the MOC)
    pub async fn register(
        &mut self,
        moc_id: &str,
        moc: &HpxMoc,
        fits_bytes: &[u8],
        metadata: &MocMetadata,
    ) -> anyhow::Result<usize> {
        let cells = degraded_cells_at_depth(moc, self.depth);
        let ttl = metadata.validity_seconds as usize;

        // FITS bytes
        let _: () = self
            .conn
            .set_ex(Self::fits_key(moc_id), fits_bytes, ttl as u64)
            .await?;

        // Metadata JSON
        let meta_json = serde_json::to_vec(metadata)?;
        let _: () = self
            .conn
            .set_ex(Self::meta_key(moc_id), meta_json, ttl as u64)
            .await?;

        // Cell sets (pipelined)
        let mut pipe = redis::pipe();
        for cell in &cells {
            let key = self.cell_key(*cell);
            pipe.sadd(&key, moc_id).ignore();
            pipe.expire(&key, ttl as i64).ignore();
        }
        let _: () = pipe.query_async(&mut self.conn).await?;

        Ok(cells.len())
    }

    /// Look up MOC IDs that overlap a given sky position.
    ///
    /// Returns the *coarse* candidate set (Valkey `SMEMBERS` on the alert's
    /// cell). Use [`Self::precise_check`] to filter to the MOCs that
    /// actually contain the position at the MOC's native resolution.
    pub async fn candidates(&mut self, ra_deg: f64, dec_deg: f64) -> anyhow::Result<Vec<String>> {
        let cell = position_to_cell(ra_deg, dec_deg, self.depth);
        let key = self.cell_key(cell);
        let ids: Vec<String> = self.conn.smembers(&key).await?;
        Ok(ids)
    }

    /// Get or fetch+parse a MOC by id. Hits the in-process LRU first,
    /// falling back to a Valkey GET + `moc_from_fits_bytes` on miss.
    async fn get_moc(&mut self, moc_id: &str) -> anyhow::Result<Option<Arc<HpxMoc>>> {
        if let Some(m) = self.moc_cache.lock().get(moc_id).cloned() {
            return Ok(Some(m));
        }
        let bytes: Option<Vec<u8>> = self.conn.get(Self::fits_key(moc_id)).await?;
        let Some(bytes) = bytes else { return Ok(None) };
        let moc = Arc::new(crate::moc::moc_from_fits_bytes(&bytes)?);
        self.moc_cache.lock().put(moc_id.to_string(), moc.clone());
        Ok(Some(moc))
    }

    /// Full lookup: candidate set from Valkey, then precise post-filter
    /// against the (in-process cached) parsed MOC for each candidate.
    pub async fn lookup(&mut self, ra_deg: f64, dec_deg: f64) -> anyhow::Result<Vec<MocHit>> {
        let candidate_ids = self.candidates(ra_deg, dec_deg).await?;
        if candidate_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut hits = Vec::new();
        for moc_id in &candidate_ids {
            let Some(moc) = self.get_moc(moc_id).await? else {
                continue;
            };
            if !is_in_moc(&moc, ra_deg, dec_deg) {
                continue;
            }
            let meta_bytes: Option<Vec<u8>> = self.conn.get(Self::meta_key(moc_id)).await?;
            let metadata = meta_bytes.and_then(|b| serde_json::from_slice(&b).ok());
            hits.push(MocHit {
                moc_id: moc_id.clone(),
                metadata,
            });
        }
        Ok(hits)
    }

    /// Look up *only* the candidate IDs (no precise check, no FITS fetch).
    /// Use this for the inner-loop benchmark where we measure just the
    /// Valkey set lookup latency.
    pub async fn lookup_candidates_only(
        &mut self,
        ra_deg: f64,
        dec_deg: f64,
    ) -> anyhow::Result<Vec<String>> {
        self.candidates(ra_deg, dec_deg).await
    }

    /// Drop all keys we own. Useful for tests and benchmarks.
    pub async fn flush_all(&mut self) -> anyhow::Result<()> {
        let mut iter: redis::AsyncIter<String> = self.conn.scan_match("mocidx:*").await?;
        let mut keys: Vec<String> = Vec::new();
        while let Some(k) = iter.next_item().await {
            keys.push(k);
        }
        drop(iter);
        if !keys.is_empty() {
            let _: () = self.conn.del(keys).await?;
        }
        Ok(())
    }
}

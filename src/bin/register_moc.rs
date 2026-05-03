//! Register one MOC (or skymap thresholded at a credible level) into the
//! Valkey-backed meta-index.
//!
//! Usage:
//!   register-moc --skymap path/to/skymap.fits --moc-id LVK-S250101a
//!
//! Or, with an existing MOC FITS:
//!   register-moc --moc path/to/region.moc --moc-id GBM-bn250101001

#[allow(unused_imports)]
use boom_moc_index::moc::MocHasMaxDepth; // required for `.depth_max()` below
use boom_moc_index::{moc, MocIndex, MocMetadata, DEFAULT_INDEX_DEPTH};
use clap::Parser;

#[derive(Parser)]
#[command(
    name = "register-moc",
    about = "Register a MOC into the Valkey meta-index"
)]
struct Args {
    /// Path to a HEALPix skymap FITS (will be thresholded at --credible-level)
    #[arg(long, conflicts_with = "moc_path")]
    skymap: Option<String>,

    /// Path to a precomputed MOC FITS
    #[arg(long = "moc", conflicts_with = "skymap")]
    moc_path: Option<String>,

    /// MOC identifier (used as the Valkey key suffix and returned by lookup)
    #[arg(long)]
    moc_id: String,

    /// Source label (e.g. LVK, Fermi-GBM, IceCube)
    #[arg(long, default_value = "unknown")]
    source: String,

    /// Trigger time (ISO-8601 UTC)
    #[arg(long, default_value = "1970-01-01T00:00:00Z")]
    trigger_time: String,

    /// Credible level used when thresholding a skymap (ignored if --moc is given)
    #[arg(long, default_value_t = 0.95)]
    credible_level: f64,

    /// Validity window in seconds (TTL on all Valkey keys)
    #[arg(long, default_value_t = 14 * 24 * 3600)]
    validity_seconds: u64,

    /// HEALPix depth used by the meta-index
    #[arg(long, default_value_t = DEFAULT_INDEX_DEPTH)]
    depth: u8,

    /// Valkey URL
    #[arg(long, default_value = "redis://127.0.0.1:6390")]
    redis_url: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    // Always cache the MOC (as IVOA FITS) in Valkey, regardless of whether
    // the input was a skymap or a precomputed MOC.
    let hpx_moc = match (&args.skymap, &args.moc_path) {
        (Some(p), None) => moc::moc_from_skymap_bytes(&std::fs::read(p)?, args.credible_level)?,
        (None, Some(p)) => moc::moc_from_fits_bytes(&std::fs::read(p)?)?,
        _ => anyhow::bail!("Pass exactly one of --skymap or --moc"),
    };
    let bytes = moc::moc_to_fits_bytes(&hpx_moc)?;

    let metadata = MocMetadata {
        source: args.source,
        trigger_time: args.trigger_time,
        credible_level: args.credible_level,
        validity_seconds: args.validity_seconds,
        coverage_fraction: hpx_moc.coverage_percentage(),
        native_depth: hpx_moc.depth_max(),
    };

    let mut idx = MocIndex::open(&args.redis_url, args.depth).await?;
    let n_cells = idx
        .register(&args.moc_id, &hpx_moc, &bytes, &metadata)
        .await?;

    println!(
        "Registered MOC {} (source={}, coverage={:.4}%, native_depth={}) into {} cells at index depth {} (TTL {}s)",
        args.moc_id,
        metadata.source,
        metadata.coverage_fraction * 100.0,
        metadata.native_depth,
        n_cells,
        args.depth,
        args.validity_seconds,
    );
    Ok(())
}

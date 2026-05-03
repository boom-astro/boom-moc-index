//! One-off lookup: which currently-active MOCs overlap this sky position?
//!
//! Usage:
//!   lookup --ra 213.04 --dec 60.91

use boom_moc_index::{MocIndex, DEFAULT_INDEX_DEPTH};
use clap::Parser;

#[derive(Parser)]
#[command(name = "lookup", about = "Look up overlapping MOCs for a sky position")]
struct Args {
    #[arg(long)]
    ra: f64,
    #[arg(long)]
    dec: f64,

    /// Skip the precise post-check; return only the Valkey candidate set
    #[arg(long, default_value_t = false)]
    candidates_only: bool,

    #[arg(long, default_value_t = DEFAULT_INDEX_DEPTH)]
    depth: u8,

    #[arg(long, default_value = "redis://127.0.0.1:6390")]
    redis_url: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let mut idx = MocIndex::open(&args.redis_url, args.depth).await?;

    if args.candidates_only {
        let ids = idx.lookup_candidates_only(args.ra, args.dec).await?;
        println!(
            "{} candidate MOCs at ({}, {}):",
            ids.len(),
            args.ra,
            args.dec
        );
        for id in ids {
            println!("  {}", id);
        }
    } else {
        let hits = idx.lookup(args.ra, args.dec).await?;
        println!("{} MOCs contain ({}, {}):", hits.len(), args.ra, args.dec);
        for h in hits {
            let src = h
                .metadata
                .as_ref()
                .map(|m| m.source.as_str())
                .unwrap_or("?");
            println!("  {}  source={}", h.moc_id, src);
        }
    }
    Ok(())
}

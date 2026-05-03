//! Benchmark harness: register N MOCs from ORIGIN observing scenarios, then
//! drive synthetic alert positions through the index and measure latency.
//!
//! Reports:
//!   - Index population time
//!   - Total cells touched
//!   - Per-lookup latency: p50/p90/p95/p99/p99.9 on:
//!     (a) candidate-only lookup (just the Valkey SMEMBERS)
//!     (b) full lookup (candidate + precise MOC post-check)
//!   - Sustained throughput (alerts/sec)
//!   - Hit rate (fraction of alerts that have ≥1 candidate / ≥1 hit)
//!
//! Usage:
//!   docker compose up -d
//!   cargo run --release --bin benchmark -- --n-mocs 10 --n-queries 100000

#[allow(unused_imports)]
use boom_moc_index::moc::MocHasMaxDepth; // required for `.depth_max()` below
use boom_moc_index::{moc, MocIndex, MocMetadata, DEFAULT_INDEX_DEPTH};
use clap::Parser;
use comfy_table::{Cell, Table};
use rand::Rng;
use std::time::Instant;

#[derive(Parser)]
#[command(name = "benchmark", about = "Drive load through the meta-MOC index")]
struct Args {
    /// Number of active MOCs to register
    #[arg(long, default_value_t = 10)]
    n_mocs: usize,

    /// Number of alert positions to look up
    #[arg(long, default_value_t = 100_000)]
    n_queries: usize,

    /// HEALPix depth used by the meta-index
    #[arg(long, default_value_t = DEFAULT_INDEX_DEPTH)]
    depth: u8,

    /// Credible level when thresholding source skymaps
    #[arg(long, default_value_t = 0.9)]
    credible_level: f64,

    /// Source directory of ORIGIN skymap FITS files
    #[arg(
        long,
        default_value = "/Users/mcoughlin/Code/ORIGIN/observing-scenarios/runs/O4HL/bgp/allsky"
    )]
    skymap_dir: String,

    /// Valkey URL
    #[arg(long, default_value = "redis://127.0.0.1:6390")]
    redis_url: String,

    /// Skip the precise post-check (measure only the Valkey set lookup)
    #[arg(long, default_value_t = false)]
    candidates_only: bool,

    /// Emit machine-readable JSON to stdout instead of the human table
    #[arg(long, default_value_t = false)]
    json: bool,
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn fmt_us(secs: f64) -> String {
    let us = secs * 1e6;
    if us < 1000.0 {
        format!("{:.1} µs", us)
    } else {
        format!("{:.2} ms", us / 1000.0)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();

    if !args.json {
        println!(
            "boom-moc-index benchmark: depth={}, n_mocs={}, n_queries={}, candidates_only={}",
            args.depth, args.n_mocs, args.n_queries, args.candidates_only
        );
    }

    let mut idx = MocIndex::open(&args.redis_url, args.depth).await?;
    idx.flush_all().await?;

    // -- Register MOCs --
    if !args.json {
        println!("\n=== Registering {} MOCs ===", args.n_mocs);
    }
    let mut total_cells = 0_usize;
    let mut total_coverage = 0.0_f64;
    let t = Instant::now();

    for i in 0..args.n_mocs {
        let path = format!("{}/{}.fits", args.skymap_dir, i);
        let raw = std::fs::read(&path).map_err(|e| anyhow::anyhow!("read {}: {}", path, e))?;
        let hpx_moc = moc::moc_from_skymap_bytes(&raw, args.credible_level)?;
        // Re-encode as IVOA MOC FITS so we can re-parse on the precise check.
        let bytes = moc::moc_to_fits_bytes(&hpx_moc)?;
        let coverage = hpx_moc.coverage_percentage();
        total_coverage += coverage;
        let metadata = MocMetadata {
            source: "ORIGIN-O4HL".to_string(),
            trigger_time: format!("2026-01-01T00:00:{:02}Z", i % 60),
            credible_level: args.credible_level,
            validity_seconds: 14 * 24 * 3600,
            coverage_fraction: coverage,
            native_depth: hpx_moc.depth_max(),
        };
        let moc_id = format!("ORIGIN-O4HL-{:06}", i);
        let n_cells = idx.register(&moc_id, &hpx_moc, &bytes, &metadata).await?;
        total_cells += n_cells;
    }
    let pop_time = t.elapsed();
    let mean_coverage = total_coverage / args.n_mocs as f64;

    if !args.json {
        println!(
            "  Registered in {:.2}s ({:.1} ms/MOC), mean coverage {:.4}%, total cells {} (avg {:.0}/MOC)",
            pop_time.as_secs_f64(),
            pop_time.as_secs_f64() * 1000.0 / args.n_mocs as f64,
            mean_coverage * 100.0,
            total_cells,
            total_cells as f64 / args.n_mocs as f64,
        );
    }

    // -- Generate uniform random sky positions --
    let mut rng = rand::rng();
    let positions: Vec<(f64, f64)> = (0..args.n_queries)
        .map(|_| {
            let ra = rng.random::<f64>() * 360.0;
            // uniform on sphere
            let u: f64 = rng.random::<f64>();
            let dec = (2.0 * u - 1.0).asin().to_degrees();
            (ra, dec)
        })
        .collect();

    // -- Lookup loop --
    if !args.json {
        println!("\n=== {} lookups ===", args.n_queries);
    }
    let mut latencies: Vec<f64> = Vec::with_capacity(args.n_queries);
    let mut n_with_candidates = 0_usize;
    let mut n_with_hits = 0_usize;

    let t_total = Instant::now();
    for &(ra, dec) in &positions {
        let t = Instant::now();
        if args.candidates_only {
            let ids = idx.lookup_candidates_only(ra, dec).await?;
            let dt = t.elapsed().as_secs_f64();
            latencies.push(dt);
            if !ids.is_empty() {
                n_with_candidates += 1;
            }
        } else {
            let hits = idx.lookup(ra, dec).await?;
            let dt = t.elapsed().as_secs_f64();
            latencies.push(dt);
            if !hits.is_empty() {
                n_with_hits += 1;
                n_with_candidates += 1; // by construction
            }
        }
    }
    let total_time = t_total.elapsed();
    let throughput = args.n_queries as f64 / total_time.as_secs_f64();

    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

    if args.json {
        let out = serde_json::json!({
            "implementation": "boom_moc_index_rust_valkey",
            "n_mocs": args.n_mocs,
            "n_queries": args.n_queries,
            "depth": args.depth,
            "candidates_only": args.candidates_only,
            "credible_level": args.credible_level,
            "load_time_seconds": pop_time.as_secs_f64(),
            "wall_seconds": total_time.as_secs_f64(),
            "throughput_per_second": throughput,
            "p50_seconds": percentile(&latencies, 50.0),
            "p90_seconds": percentile(&latencies, 90.0),
            "p95_seconds": percentile(&latencies, 95.0),
            "p99_seconds": percentile(&latencies, 99.0),
            "p99_9_seconds": percentile(&latencies, 99.9),
            "max_seconds": *latencies.last().unwrap_or(&0.0),
            "n_with_candidates": n_with_candidates,
            "n_with_hits": n_with_hits,
            "total_cells": total_cells,
            "mean_coverage_fraction": mean_coverage,
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
        return Ok(());
    }

    let mut t = Table::new();
    t.set_header(vec!["metric", "value"]);
    t.add_row(vec!["MOCs registered", &args.n_mocs.to_string()]);
    t.add_row(vec!["queries", &args.n_queries.to_string()]);
    t.add_row(vec![
        "wall time".to_string(),
        format!("{:.2}s", total_time.as_secs_f64()),
    ]);
    t.add_row(vec![
        "throughput".to_string(),
        format!("{:.0} alerts/s", throughput),
    ]);
    t.add_row(vec![
        "p50 latency".to_string(),
        fmt_us(percentile(&latencies, 50.0)),
    ]);
    t.add_row(vec![
        "p90 latency".to_string(),
        fmt_us(percentile(&latencies, 90.0)),
    ]);
    t.add_row(vec![
        "p95 latency".to_string(),
        fmt_us(percentile(&latencies, 95.0)),
    ]);
    t.add_row(vec![
        "p99 latency".to_string(),
        fmt_us(percentile(&latencies, 99.0)),
    ]);
    t.add_row(vec![
        "p99.9 latency".to_string(),
        fmt_us(percentile(&latencies, 99.9)),
    ]);
    t.add_row(vec![
        "max latency".to_string(),
        fmt_us(*latencies.last().unwrap_or(&0.0)),
    ]);
    t.add_row(vec![
        "≥1 candidate".to_string(),
        format!(
            "{}/{} ({:.2}%)",
            n_with_candidates,
            args.n_queries,
            n_with_candidates as f64 / args.n_queries as f64 * 100.0
        ),
    ]);
    if !args.candidates_only {
        t.add_row(vec![
            "≥1 precise hit".to_string(),
            format!(
                "{}/{} ({:.2}%)",
                n_with_hits,
                args.n_queries,
                n_with_hits as f64 / args.n_queries as f64 * 100.0
            ),
        ]);
    }
    println!("\n{}", t);
    let _ = Cell::new(""); // silence unused-import warning if needed

    Ok(())
}

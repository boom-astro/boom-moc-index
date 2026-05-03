# boom-moc-index

Streaming meta-index of HEALPix MOCs in Valkey, for sub-millisecond
skymap-overlap lookup at alert-broker scale.

Given an alert position `(ra, dec)`, returns every currently-active skymap
(LVK GW localization, GRB error region, neutrino track) that overlaps it.
The MOCs themselves live outside the alert database — only an inverted
index `cell → set of MOC IDs` is kept hot in Valkey, so a lookup is one
SET read regardless of how many MOCs are active.

## Architecture

```
                     ┌──────────────────┐
   GCN Kafka ───────▶│  gcn-listener    │── MOC ──▶ Valkey
   (igwn.gwalert,     │ (skymap → MOC    │           ┌─────────────────────────────┐
    swift.bat.guano,  │  at credible     │           │ mocidx:cell:{depth}:{cell}  │
    icecube, …)       │  level=0.95)     │           │   SET of MOC IDs            │
                     └──────────────────┘           │ mocidx:fits:{moc_id}        │
                                                     │   IVOA MOC FITS (TTL'd)     │
   alert ─── (ra, dec) ────▶  lookup  ─────────────▶│ mocidx:meta:{moc_id}        │
                              SMEMBERS                │   metadata JSON             │
                              + precise check         └─────────────────────────────┘
```

Three Valkey key families per registered MOC, all TTL'd to the validity
window (default 14 days):

- `mocidx:cell:{depth}:{cell}` — the inverted index. One SADD per
  HEALPix cell the MOC touches at the index depth (default depth 6 →
  ~0.84 deg² cells, 49 152 over the sky).
- `mocidx:fits:{moc_id}` — the IVOA MOC FITS, fetched on a hit for the
  precise point-in-MOC check.
- `mocidx:meta:{moc_id}` — source / trigger time / coverage / native
  depth as JSON.

A precise post-check uses an in-process LRU (`Arc<HpxMoc>`, 1024 entries
default) so repeat hits don't re-parse FITS bytes from Valkey.

## Quick start

```sh
docker compose up -d                                    # Valkey on :6390
cargo build --release

# Register one MOC from a HEALPix skymap, thresholded at the 90% region
./target/release/register-moc --skymap path/to/bayestar.fits \
    --moc-id LVK-S260101a --source LVK

# Lookup
./target/release/lookup --ra 213.04 --dec 60.91
./target/release/lookup --ra 213.04 --dec 60.91 --candidates-only
```

## Live GCN ingest

`gcn-listener` subscribes to GCN Kafka, decodes embedded skymaps from
each alert, builds a MOC at `credible_level=0.95`, and registers it.

```sh
cp .env.example .env             # then fill in GCN_CLIENT_ID / SECRET
cargo run --release --bin gcn-listener
```

Topics: `igwn.gwalert` (LVK base64-inline multi-order FITS),
`gcn.notices.swift.bat.guano`, `gcn.notices.einstein_probe.wxt.alert`,
`gcn.notices.icecube.{lvk_nu_track_search,gold_bronze_track_alerts}`.
Heartbeats are skipped silently.

Offline replay (no Kafka):

```sh
cargo run --bin gcn-listener -- \
    --replay-payload tests/fixtures/igwn_gwalert_sample.json \
    --replay-topic igwn.gwalert
```

### macOS note

We bypass `gcn-kafka`'s `set_gcn_auth` because librdkafka's built-in
OAUTHBEARER/OIDC token fetch is broken on macOS
([librdkafka #4761](https://github.com/confluentinc/librdkafka/issues/4761))
— vendored OpenSSL has no path to the macOS keychain. Token fetch goes
through a custom `ClientContext` that shells to system `curl` (which uses
SecureTransport).

## Benchmark vs. SkyPortal-style baseline

`comparison/skyportal_baseline.py` mirrors the SkyPortal cross-match
pattern: per-alert iteration over all in-process `mocpy` MOCs.
`comparison/run_comparison.sh` sweeps `N_MOCS ∈ {1,3,10,30,100,300}` at
2 000 queries each and writes JSON for the plotter.

Single-thread, candidates-only:

| N_MOCS | SkyPortal-style p50 | boom-moc-index p50 | speedup |
|-------:|--------------------:|-------------------:|--------:|
|      1 |              0.4 ms |             0.2 ms |   ~2×   |
|     10 |              0.3 ms |             0.2 ms |  ~1.5×  |
|    100 |              2.5 ms |             0.2 ms |   13×   |
|    300 |              7.4 ms |             0.2 ms |   30×   |

Crossover at N≈10. Below that, Python wins because there's no Valkey
round-trip. Above, boom-moc-index is flat (one SET read regardless of N)
while the per-alert iteration grows linearly.

## Layout

```
src/
  lib.rs             public API
  index.rs           MocIndex (Valkey ops, register / lookup / candidates)
  moc.rs             MOC parsing (IVOA FITS, NUNIQ multi-order skymaps)
  bin/
    register-moc     register one skymap or MOC FITS into the index
    lookup           one-off lookup at (ra, dec)
    benchmark        timing harness
    gcn-listener     live GCN Kafka → MocIndex pump
comparison/          SkyPortal-style baseline + sweep + plotter
tests/fixtures/      offline-replay payload for gcn-listener smoke tests
docker-compose.yaml  Valkey 8, no persistence, port 6390
```

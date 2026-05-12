# Dwell Analysis — Core Algorithm Plan

## Context

You want a Python application that finds **dwells** — locations where an entity (id, lat, lon, timestamp) lingers across consecutive events — and uses them to answer questions about where entities stop, who stops together, in what order, and which locations matter most under different importance lenses. The application is greenfield: no code exists today. Source spec: `/home/kihaji/projects/analytic/dwell-description.md`.

The shape is: a Plotly Dash UI mounted in a FastAPI app, deckgl-dash maps, Polars dataframes (DuckDB acceptable substitute), Python 3.12. Datasets live in a pre-stored catalog; users pick a dataset and parameters; the pipeline either runs or hits a cache. ≤ 100K events per dataset (in-memory Polars is fine). UTC throughout.

This plan focuses on the **core algorithm and data flow** — schemas, clustering, dwell extraction, co-dwell, transitions, sequences, Markov, scoring, caching, edge cases, and synthetic test data. UI and API specifics are intentionally out of scope here; they consume the cached Parquet artifacts produced by the pipeline.

---

## Locked Decisions (from Q&A)

| # | Decision | Choice |
|---|---|---|
| 1 | Spatial clustering | **Both** H3 hexbins AND HDBSCAN, user-selectable per run |
| 2 | Cluster scope | Per-entity, then merge into shared locations (H3 reduces to "cell IS location"; HDBSCAN merges via second-pass DBSCAN on per-entity centroids) |
| 3 | Consecutiveness | **Strict** — any out-of-cluster event or `gap > max_time_gap_s` breaks the dwell |
| 4 | Dwell qualification | `n_events ≥ min_dwell_events` AND `duration ≥ min_dwell_duration_s` AND no internal gap > `max_time_gap_s` |
| 5 | Lifecycle | Pre-stored dataset catalog; recompute (or cache-hit) on selection. No streaming |
| 6 | Co-dwell | Same location + time overlap ≥ `min_codwell_seconds` (default 60s) |
| 7 | Scoring | Independent per-metric columns + user-tunable weighted composite |
| 8 | HDBSCAN location polygon | Convex hull of all member events; H3 cell is the hex |
| 9 | Pattern analysis | All three precomputed: pairwise transitions, PrefixSpan sequences, first-order Markov |
| 10 | Scale | ≤ ~100K events; in-memory Polars |
| 11 | Day boundary | UTC (matches zulu input) |
| 12 | Volume metric | Expose both `volume_dwell` (events inside qualifying dwells) and `volume_total` (any presence) |
| 13 | Multi-day dwell attribution | `first_of_day` → start date; `last_of_day` → end date; intervening days → separate `continuous_presence_days` metric |
| 14 | Chain-origin formula | Expose three variants (distinct-next-count; count+entropy; total-out-transitions); composite uses **count + entropy** |
| 15 | HDBSCAN merge guard | Max-diameter cap: split any merged location whose hull diameter > **K × spatial_bound, K=10** via tighter sub-clustering |

---

## A. Domain Model (Polars schemas)

All timestamps are `pl.Datetime("us", "UTC")`. Coordinates `Float64` (WGS84). Internal numeric ids `UInt32`/`Int32` for join efficiency; original `id` denormalized onto exported tables for human readability.

### `events_raw` — input
| col | dtype | role |
|---|---|---|
| `id` | `Utf8` | entity id |
| `lat` | `Float64` | WGS84 |
| `lon` | `Float64` | WGS84 |
| `timestamp` | `Datetime(us, UTC)` | event time |

PK after dedupe: `(id, timestamp)`.

### `events` — preprocessed
Adds `entity_idx UInt32`, `event_idx UInt32` (global monotonic after sort), `lat_rad Float64`, `lon_rad Float64`, `speed_outlier Bool`.

### `events_loc` — events + location annotation
Adds `h3_cell UInt64` (H3 only, nullable), `per_entity_cluster Int32` (HDBSCAN only, -1=noise), `location_id Int32` (-1 reserved for "not in any location"), `location_method Categorical("H3","HDBSCAN")`.

### `locations`
| col | dtype |
|---|---|
| `location_id` | `Int32` (PK) |
| `method` | `Categorical` |
| `h3_cell` | `UInt64` nullable |
| `centroid_lat`, `centroid_lon` | `Float64` (WGS84) |
| `polygon_wkb` | `Binary` (shapely WKB) |
| `geom_kind` | `Categorical("polygon","linestring","point")` (degenerate hulls) |
| `area_m2` | `Float64` (0 for non-polygons) |
| `hull_diameter_m` | `Float64` (for merge-guard auditing) |

### `dwells`
| col | dtype |
|---|---|
| `dwell_id` | `UInt32` PK |
| `entity_idx`, `id` | `UInt32`, `Utf8` |
| `location_id` | `Int32` |
| `start_ts`, `end_ts` | `Datetime(us, UTC)` |
| `duration_s`, `mean_lat`, `mean_lon` | `Float64` |
| `n_events` | `UInt32` |
| `first_event_idx`, `last_event_idx` | `UInt32` |

### `co_dwells`
`co_dwell_id PK`, `entity_a < entity_b` enforced, `location_id`, `dwell_id_a`, `dwell_id_b`, `overlap_start`, `overlap_end`, `overlap_duration_s ≥ min_codwell_seconds`, `a_arrived_first Bool` (`dwells[a].start_ts ≤ dwells[b].start_ts`, tiebroken by smaller `entity_idx`).

### `transitions` (per-entity)
`entity_idx`, `prev_location_id`, `next_location_id`, `prev_dwell_id`, `next_dwell_id`, `gap_s = next.start_ts − prev.end_ts`. Aggregate `transitions_agg(prev_location_id, next_location_id, count, mean_gap_s)`.

### `sequences`
- `entity_dwell_sequence(entity_idx, position, location_id)` — per-entity ordered.
- `frequent_sequences(pattern_id, pattern List[Int32], length, support, support_ratio)` — PrefixSpan output.

### `markov_global(from_location_id, to_location_id, count, prob)` and `markov_per_entity(entity_idx, from, to, count, prob)`. Separate `absorbing_states` lists location_ids with zero observed outgoing transitions.

### Stats tables
- `entity_stats`: `entity_idx, id, n_events, n_dwells, n_unique_locations_dwelled, total/mean/min/max_dwell_seconds, n_speed_outliers`.
- `location_stats`: `location_id, n_events, n_dwells, n_unique_ids_dwelled, n_unique_ids_seen, first_of_day_count, last_of_day_count, continuous_presence_days, chain_origin_distinct_count, chain_origin_total_transitions, chain_origin_count_plus_entropy`.
- `per_id_per_location_stats`: counts and dwell statistics joined by `(entity_idx, location_id)`.
- `first_last_seen_day(entity_idx, utc_date, first_dwell_id?, first_location_id?, last_dwell_id?, last_location_id?, same_first_last Bool, continuous_presence Bool)` — last column True when the date is an intervening date of a multi-day dwell.

### `location_scores`
`location_id PK`, raw columns: `volume_dwell_raw`, `volume_total_raw`, `diversity_dwelled_raw`, `diversity_seen_raw`, `last_of_day_raw`, `first_of_day_raw`, `chain_origin_distinct_raw`, `chain_origin_count_plus_entropy_raw`, `chain_origin_total_raw`. Each gets a `<metric>_norm` column (percentile rank, see B.13). `composite_score` is recomputed on weight change.

---

## B. Pipeline (DAG, each stage writes a Parquet to the run cache)

Order:
`validate → cluster → merge_locations → annotate_events → dwells → location_geometry → location_stats → codwells → transitions → sequences → markov → first_last_day → score_raw → score_composite`

### B.1 Validation / Preprocessing
1. Coerce dtypes; ensure timestamps are UTC (`dt.replace_time_zone("UTC")` if naive, else `convert_time_zone`).
2. Drop rows where `lat ∉ [-90,90]`, `lon ∉ [-180,180]`, or any required column is null. Log to `rejected_rows.parquet`.
3. Duplicate `(id, timestamp)` policy (default `keep_first_then_dedupe_by_coord`): if duplicates share `(lat, lon)`, keep one silently; if they differ, keep the canonical first and log the others.
4. Sort by `(id, timestamp)`.
5. Speed-outlier flag (default ON): compute haversine to previous point and elapsed seconds; flag `speed_outlier = implied_speed > max_speed_mps` (default 150 m/s ≈ 540 km/h). **Do not drop** — strict consecutiveness needs untouched ordering — only surface in `entity_stats.n_speed_outliers` and the sidecar.
6. Add `entity_idx` (dense rank of `id` − 1), `event_idx` (row index after sort), `lat_rad`/`lon_rad`.

### B.2 Spatial clustering

Parameter `spatial_bound_m`:
- H3 → mapped to **nearest** H3 resolution by edge length via `h3.average_hexagon_edge_length(res, unit='m')`. Resolution recorded in `run_meta.json`. (Default resolution range 5–13.)
- HDBSCAN → meters in a local projected CRS (see below).

**B.2.i H3 path.** Vectorized `h3.latlng_to_cell` (integer API). The "per-entity clustering then merge" decision degenerates to "the cell IS the location" — running per-entity H3 then merging would be a no-op since `latlng_to_cell` is deterministic from coords. Assign `location_id` as dense ranks of unique cells across the whole dataset.

**B.2.ii HDBSCAN path.**

- *Coordinate handling.* Project once to a local **AEQD** (Azimuthal Equidistant) CRS centered on the dataset's centroid, via pyproj. Use `metric='euclidean'` with `algorithm='auto'` in `sklearn.cluster.HDBSCAN`. Rationale: meter-native parameters everywhere, negligible distortion within ~200 km of centroid. Haversine in sklearn is constrained to ball_tree with radian inputs and mixing radian `cluster_selection_epsilon` with meter user params invites bugs.
- *Globe-spanning fallback.* If the dataset's lat span > ~10° **or** the data straddles the antimeridian, switch to per-entity AEQD centered on each entity's median location; record `projection_mode = per_entity` in run_meta.
- *HDBSCAN params (defaults derived from user params):*
  - `min_cluster_size = max(min_dwell_events, 5)`
  - `min_samples = min_cluster_size`
  - `cluster_selection_epsilon = spatial_bound_m`
  - `cluster_selection_method = 'eom'`
  - `store_centers = 'centroid'`
- Run per entity, only if entity has `≥ min_cluster_size` events; else all events labelled `-1`. Noise (`-1`) is kept in `events_loc` and breaks dwells naturally (see B.4).

### B.3 Merge per-entity clusters → shared locations (HDBSCAN only)

1. Collect per-entity cluster centroids: `(entity_idx, per_entity_cluster, centroid_lat, centroid_lon, n_events_in_cluster)`. Drop noise.
2. Project centroids to the same AEQD CRS as B.2.ii.
3. Run `sklearn.cluster.DBSCAN(eps=spatial_bound_m, min_samples=1, metric='euclidean')` on the projected centroids. `min_samples=1` is deliberate — singleton centroids are valid locations (one entity has visited).
4. **Max-diameter guard.** For every resulting DBSCAN cluster, compute the convex-hull diameter (max pairwise Euclidean distance) of its **member events** (not just centroids). If `diameter > K * spatial_bound_m` (`K = 10` default):
   a. Mark the location for splitting.
   b. Re-cluster its centroids with **single-link agglomerative clustering** (`sklearn.cluster.AgglomerativeClustering(linkage='single', distance_threshold=spatial_bound_m, n_clusters=None)`).
   c. Replace the offending `location_id` with the resulting sub-labels.
   d. Iterate until no location exceeds the diameter cap or only singletons remain. Cap at 5 iterations; log a warning if cap reached.
5. Map `(entity_idx, per_entity_cluster) → location_id` back onto `events_loc`.

Why DBSCAN first + agglomerative split rather than agglomerative everywhere: DBSCAN handles the typical case (centroids cleanly grouped) more efficiently; the split path activates only for chained "strings of beads," which is the exact failure mode you cared about.

### B.4 Dwell extraction (strict run-length encoding)

Algorithm:
1. Sort `events_loc` by `(entity_idx, event_idx)`.
2. Build a `is_break` indicator that is True at the first row of every entity OR when `location_id` changes from the prior row OR when the current `location_id == -1` OR when `timestamp − prev_timestamp > max_time_gap_s`.
3. `run_id = cum_sum(is_break)` over the whole frame — every event gets a run id.
4. Drop runs whose `location_id == -1`.
5. Aggregate by `run_id` → first/min/max/count/mean.
6. Filter `n_events ≥ min_dwell_events AND duration_s ≥ min_dwell_duration_s`.

Polars sketch:
```python
prev_loc = pl.col("location_id").shift(1).over("entity_idx")
prev_ts  = pl.col("timestamp").shift(1).over("entity_idx")
gap_s    = (pl.col("timestamp") - prev_ts).dt.total_seconds()
is_break = (
    prev_loc.is_null()
    | (pl.col("location_id") != prev_loc)
    | (pl.col("location_id") == -1)
    | (gap_s > max_time_gap_s)
).cast(pl.UInt32)

ev = ev.with_columns(is_break=is_break, run_id=is_break.cum_sum())

dwells = (
  ev.filter(pl.col("location_id") != -1)
    .group_by(["entity_idx","run_id","location_id"], maintain_order=True)
    .agg(pl.first("id"), pl.len().alias("n_events"),
         pl.min("timestamp").alias("start_ts"),
         pl.max("timestamp").alias("end_ts"),
         pl.mean("lat").alias("mean_lat"),
         pl.mean("lon").alias("mean_lon"),
         pl.min("event_idx").alias("first_event_idx"),
         pl.max("event_idx").alias("last_event_idx"))
    .with_columns(duration_s=(pl.col("end_ts")-pl.col("start_ts")).dt.total_seconds())
    .filter((pl.col("n_events") >= min_dwell_events) &
            (pl.col("duration_s") >= min_dwell_duration_s))
    .with_row_index("dwell_id")
)
```

### B.5 Location geometry

- **H3**: `polygon = shapely.Polygon(swap_to_lon_lat(h3.cell_to_boundary(cell)))`; `area_m2 = h3.cell_area(res, 'm^2')`; `geom_kind = "polygon"`; cache once per unique cell.
- **HDBSCAN**: for each `location_id`, compute the convex hull of all member events in the AEQD projection, then unproject back to WGS84. Use the *same AEQD CRS used during clustering* for consistency. Shapely 2 returns `Polygon`/`LineString`/`Point` depending on uniqueness/collinearity — store `geom_kind` accordingly; `area_m2 = 0` for non-polygons; export layer may optionally buffer `spatial_bound_m/2` for visual continuity (not stored canonically). Persist `hull_diameter_m` for the merge-guard audit.

### B.6 Per-dwell stats — produced in B.4.

### B.7 Per-location stats

```python
events_at_loc = events_loc.filter(pl.col("location_id") != -1)
stats = (events_at_loc
  .group_by("location_id")
  .agg(pl.len().alias("n_events"),
       pl.col("entity_idx").n_unique().alias("n_unique_ids_seen"))
  .join(
    dwells.group_by("location_id").agg(
      pl.len().alias("n_dwells"),
      pl.col("entity_idx").n_unique().alias("n_unique_ids_dwelled"),
      pl.col("n_events").sum().alias("volume_dwell_raw")
    ),
    on="location_id", how="left")
  .with_columns(pl.col("n_dwells").fill_null(0),
                pl.col("n_unique_ids_dwelled").fill_null(0),
                pl.col("volume_dwell_raw").fill_null(0))
)
```
`volume_total_raw = n_events` (every event at the location regardless of dwell membership).

### B.8 Co-dwell detection

Equi-join on `location_id`, predicate-filter to overlap and `entity_a < entity_b`:

```python
a = dwells.select([...]).rename(lambda c: c+"_a")
b = dwells.select([...]).rename(lambda c: c+"_b")
co = (a.join(b, on="location_id", how="inner")
        .filter((pl.col("entity_idx_a") < pl.col("entity_idx_b"))
              & (pl.col("start_ts_a") < pl.col("end_ts_b"))
              & (pl.col("start_ts_b") < pl.col("end_ts_a")))
        .with_columns(overlap_start=pl.max_horizontal("start_ts_a","start_ts_b"),
                      overlap_end  =pl.min_horizontal("end_ts_a","end_ts_b"))
        .with_columns(overlap_duration_s=(pl.col("overlap_end")-pl.col("overlap_start")).dt.total_seconds())
        .filter(pl.col("overlap_duration_s") >= min_codwell_seconds))
co = co.with_columns(a_arrived_first=pl.col("start_ts_a") <= pl.col("start_ts_b"))
```

Threshold is **inclusive** (`>=`); set `min_codwell_seconds=0` to count any nonzero overlap (`>0` semantics would be a separate flag).

### B.9 Transitions

```python
dwells_sorted = dwells.sort(["entity_idx","start_ts"])
transitions = (dwells_sorted
  .with_columns(
    prev_location_id=pl.col("location_id").shift(1).over("entity_idx"),
    prev_dwell_id   =pl.col("dwell_id").shift(1).over("entity_idx"),
    prev_end_ts     =pl.col("end_ts").shift(1).over("entity_idx"))
  .filter(pl.col("prev_location_id").is_not_null())
  .with_columns(gap_s=(pl.col("start_ts")-pl.col("prev_end_ts")).dt.total_seconds())
  .rename({"location_id":"next_location_id","dwell_id":"next_dwell_id"}))
transitions_agg = transitions.group_by(["prev_location_id","next_location_id"]).agg(
    pl.len().alias("count"), pl.mean("gap_s").alias("mean_gap_s"))
```

`prev_location_id == next_location_id` is **kept** (entity left and returned; the dwells were already split by gap).

### B.10 Sequence mining (PrefixSpan)

Library: `prefixspan` (pure Python, ~400 LOC). Per-entity sequence = `location_id` ordered by dwell `start_ts`, with adjacent duplicates collapsed (avoid `[A,A,A]` patterns of dubious value — the dwells were already split for a reason but at the sequence level we want symbolic transitions).

Defaults:
- `min_support_ratio = 0.05` → `minsup = max(2, ceil(ratio * n_entities_with_dwells))`
- `minlen = 2`, `maxlen = 6`, `closed = True`

If `n_entities_with_dwells < 2` or no sequence meets `minsup`, output empty `frequent_sequences`.

### B.11 Markov chain (first-order)

Per-entity and global, row-normalized within `prev_location_id`. Absorbing states (`from_location_id` with no observed outgoing transitions) listed separately; no synthetic self-loop is added.

### B.12 First/last seen in a UTC day (locked semantics)

For each dwell:
- Contributes **one** "first" event on `start_ts.date()` (UTC).
- Contributes **one** "last" event on `end_ts.date()` (UTC).
- For every date `D` such that `start_ts.date() < D < end_ts.date()`, contributes one row to `first_last_seen_day` with `continuous_presence = True` and no first/last attribution.

Then within `(entity_idx, utc_date)`:
- `first_dwell_id` = `argmin(start_ts)` among dwells whose `start_ts.date() == D`.
- `last_dwell_id` = `argmax(end_ts)` among dwells whose `end_ts.date() == D`.
- `same_first_last = first_dwell_id == last_dwell_id` (NULL on a continuous-presence-only date).

Aggregate to per-location:
- `first_of_day_count` = count of `(entity, date)` where this location was the first-dwell's location.
- `last_of_day_count` = count of `(entity, date)` where this location was the last-dwell's location.
- `continuous_presence_days` = count of `(entity, date)` where a dwell at this location spanned an interior day.

### B.13 Scoring

**Raw metrics per location:**
1. `volume_dwell_raw` — sum of `dwells.n_events` at this location.
2. `volume_total_raw` — total `events_loc` rows at this location (transit included).
3. `diversity_dwelled_raw` — `n_unique_ids_dwelled`.
4. `diversity_seen_raw` — `n_unique_ids_seen`.
5. `last_of_day_raw` — `last_of_day_count`.
6. `first_of_day_raw` — `first_of_day_count`.
7. `continuous_presence_raw` — `continuous_presence_days`.
8. `chain_origin_distinct_raw` — count of distinct downstream locations.
9. `chain_origin_total_raw` — total outgoing transitions.
10. `chain_origin_count_plus_entropy_raw = n_distinct_next + λ * H_norm(out_distribution)`, with `λ = 0.5` default, `H_norm = Shannon entropy / log(n_distinct_next)` (0 when `n_distinct_next ≤ 1`).

**Normalization (default `percentile`):** `<metric>_norm = rank("average") / n_locations` per metric. Alternative `minmax` exposed via `normalization_method` param. Z-score rejected (doesn't bound to [0,1]).

**Composite:** `composite_score = sum_i w_i * <metric>_norm_i`, with `sum w_i = 1`. Default weights:

| metric | weight |
|---|---|
| `volume_dwell` | 0.20 |
| `diversity_dwelled` | 0.15 |
| `diversity_seen` | 0.10 |
| `last_of_day` | 0.15 |
| `first_of_day` | 0.15 |
| `continuous_presence` | 0.05 |
| `chain_origin_count_plus_entropy` | 0.20 |

(`volume_total`, `chain_origin_distinct`, `chain_origin_total` default to weight 0 — exposed for ad-hoc analysis.)

Weight changes recompute only `composite_score` from existing `<metric>_norm` columns — single Polars expression, cheap, written to `composite/<weights_hash>.parquet` inside the run cache.

---

## C. Edge Cases & Invariants

Each case lists expected behavior under this design.

| # | Scenario | Behavior |
|---|---|---|
| 1 | Exactly 2 events, duration == threshold | Qualifies (inclusive `>=`) |
| 2 | Duration < min_duration | Dropped at B.4 filter |
| 3 | Long gap inside same cluster (`gap > max_time_gap_s`) | Splits into two runs; each evaluated independently |
| 4 | HDBSCAN returns no clusters for an entity | Entity contributes 0 dwells; under HDBSCAN it also contributes 0 to `n_unique_ids_seen` because its events stay `location_id = -1`; under H3 it still contributes to `n_unique_ids_seen` (cells are deterministic) — document this asymmetry |
| 5 | Singleton merged location (only one entity ever clustered there) | Valid (DBSCAN `min_samples=1`); `diversity_dwelled_norm` correctly low |
| 6 | Dwell spans UTC midnight | `first_of_day` on start date, `last_of_day` on end date, intervening days flagged `continuous_presence = True` |
| 7 | Co-dwell overlap == threshold exactly | Counts (inclusive) |
| 8a | Duplicate `(id, ts)` same coords | Silent dedupe |
| 8b | Duplicate `(id, ts)` differing coords | First kept; others logged |
| 9 | Antimeridian crossing | H3 boundaries from h3-py are consistent; HDBSCAN AEQD falls back to per-entity-AEQD when `lon span > 180°` or bbox spans antimeridian; warning emitted |
| 10 | Near-pole (`|lat| > 85°`) | Continues; warning logged |
| 11 | Empty dataset | Pipeline short-circuits; empty Parquets with correct schemas; `run_status = "no_data"` in run_meta |
| 12 | Single-event entity | 0 dwells; `n_unique_ids_seen` contribution only under H3 |
| 13 | Transit-only entity (never `≥ min_dwell_events` consecutive at a location) | 0 dwells; same caveats as 12 |
| 14 | Hub with 100+ entities | Stats scale linearly; percentile normalization handles heavy tail |
| 15 | All events at one identical coordinate | H3: one cell. HDBSCAN: one cluster if `n ≥ min_cluster_size`, else noise; hull = `Point`; `area_m2 = 0` |
| 16 | Speed outlier (GPS teleport) | Flag set; not dropped; the jumped-to event breaks the dwell if in a different cluster (strict consecutiveness) |
| 17 | 3 consecutive events but `min_dwell_events = 4` | Dropped |
| 18 | H3 cell-boundary jitter (entity oscillates across adjacent cells) | Multiple short runs each fail thresholds → fewer/no dwells. Inherent to strict consecutiveness + H3; **mitigation deferred to v2** (a `cell_buffer_events` post-filter). Surface as `n_fragmented_runs` in `entity_stats` |
| 19 | Collinear/2-point hull | Shapely returns `LineString`/`Point`; stored with correct `geom_kind`; `area_m2 = 0` |
| 20 | Location never left (absorbing state) | Listed in `absorbing_states`; no `from_location_id` row in Markov |
| 21 | Co-dwell where A's dwell contains B's | `overlap = B's duration`; `a_arrived_first = True` |
| 22 | Co-dwell with identical microsecond start | `a_arrived_first` resolves by `entity_a < entity_b` ordering; documented |
| 23 | PrefixSpan with `n_entities_with_dwells < 2` or no qualifying patterns | Empty result, no error |
| 24 | Composite weights with all-zero or negative weights | API rejects with 400; positive weights with arbitrary scale are normalized to sum to 1 inside the engine |
| 25 | "String of beads" HDBSCAN merge (long corridor of close centroids) | Caught by the max-diameter guard (K=10): location split via single-link agglomerative re-clustering. Logged in `run_meta.json` |

**Enforced invariants** (test asserts; behind a `--strict` runtime flag too):

- `sum(dwells.n_events) ≤ events.height`.
- Every dwell satisfies `n_events ≥ min_dwell_events AND duration_s ≥ min_dwell_duration_s`.
- `duration_s == (end_ts − start_ts).total_seconds()` exactly.
- For every dwell, max internal `diff(timestamp) ≤ max_time_gap_s` (proves the gap rule).
- H3: every event in a dwell satisfies `h3.latlng_to_cell(lat, lon, res) == location.h3_cell`.
- HDBSCAN: every event in a dwell lies inside (or on) the location's hull within a small tolerance, or the hull is degenerate.
- `co_dwells` has no symmetric duplicates (enforced by `entity_a < entity_b`).
- Every `markov_global` row group sums to 1 in `prob` (within float tolerance).
- Every `frequent_sequences.support ≥ minsup`.
- For every location with `geom_kind == "polygon"`, `hull_diameter_m ≤ K * spatial_bound_m`.

---

## D. Precomputation & Caching

### Cache key

SHA-256 over a canonical JSON dict (sorted keys), first 16 hex chars → `params_hash`:

```
{ dataset_id, dataset_content_hash, engine_version,
  clustering_method, spatial_bound_m,
  h3_resolution,                            // null for HDBSCAN
  hdbscan_min_cluster_size_override, hdbscan_min_samples_override,
  hdbscan_cluster_selection_method, merge_eps_m, merge_diameter_K,
  min_dwell_events, min_dwell_duration_s, max_time_gap_s,
  min_codwell_seconds,
  prefixspan_min_support_ratio, prefixspan_minlen, prefixspan_maxlen,
  nan_policy, dedupe_policy,
  normalization_method, chain_origin_lambda,
  max_speed_mps }
```

**Weights are NOT part of the cache key** — they affect only `composite/<weights_hash>.parquet`.

### Layout

```
cache/<dataset_id>/<params_hash>/
  events.parquet
  events_loc.parquet
  locations.parquet
  dwells.parquet
  location_stats.parquet
  codwells.parquet
  transitions.parquet
  transitions_agg.parquet
  sequences_per_entity.parquet
  frequent_sequences.parquet
  markov_global.parquet
  markov_per_entity.parquet
  absorbing_states.parquet
  first_last_seen_day.parquet
  entity_stats.parquet
  per_id_per_location_stats.parquet
  location_scores.parquet         # raw + *_norm
  composite/
    <weights_hash>.parquet
  run_meta.json                   # full params, timings, row counts, warnings (projection_mode, merge-guard iterations, antimeridian, etc.)
  rejected_rows.parquet           # sidecar from B.1
  _SUCCESS                        # written atomically last
```

### Reuse rules

| Change | Invalidates |
|---|---|
| Weights only | Just `composite/<weights_hash>.parquet` |
| `normalization_method` | `location_scores.parquet` + `composite/*` |
| `min_codwell_seconds` | `codwells.parquet` only |
| `prefixspan_*` | `frequent_sequences.parquet` only |
| `chain_origin_lambda` | `location_scores.parquet` + `composite/*` |
| Clustering method or `spatial_bound_m` | Everything from `cluster` onward (events stay) |
| `dataset_content_hash` shift | Whole cache |

### Atomicity & concurrency

- Stage files write to `<stage>.parquet.tmp`, then rename.
- `_SUCCESS` is written last; readers check for it.
- A `filelock` on `<params_hash>/.lock` prevents two compute runs for the same params racing; the second caller blocks then re-checks `_SUCCESS`.
- Crash-leftover `.tmp` files cleaned at startup.

### Eviction

At ≤ 100K events per dataset, each run cache is < ~500 MB. No automatic eviction. Manual `DELETE /api/runs/<hash>` for ops.

---

## E. Library / Dependency Recommendations

| dep | min version | why |
|---|---|---|
| polars | ≥ 1.15 | dataframes, `over`, `cum_sum`, `dt.total_seconds`, `write_excel` |
| fastapi + uvicorn[standard] | latest | API host (mandated) |
| dash, dash-deck | latest | UI + map (mandated) |
| h3 | ≥ 4.1 | `latlng_to_cell`, `cell_to_boundary`, `cell_area`, `average_hexagon_edge_length` (v4 int API) |
| scikit-learn | ≥ 1.5 | `HDBSCAN`, `DBSCAN`, `AgglomerativeClustering` (single-link merge-guard split) |
| shapely | ≥ 2.0 | convex hull, WKB I/O, polygon ops |
| pyproj | ≥ 3.6 | AEQD projection construction & transforms |
| pyogrio | ≥ 0.10 | Shapefile + GeoJSON export (faster than fiona; Arrow-friendly) |
| geopandas | ≥ 1.0 | only as the input shape for pyogrio's writer (Shapefile path needs it) |
| xlsxwriter | latest | required by Polars `write_excel` |
| prefixspan | ≥ 0.6 | sequence mining (pure-Python, tiny) |
| filelock | ≥ 3.15 | per-params-hash compute lock |

**Excluded.** `hdbscan` standalone (sklearn covers it). `timezonefinder` (UTC-only). `fiona` (pyogrio replaces). `pandas` not direct (transitively via geopandas).

---

## F. Synthetic Data Generator Design

Module `dwell_analysis.synthdata`. Each `Scenario` is a deterministic generator (seeded) returning a `pl.DataFrame` matching `events_raw`. Scenarios are composable via `pl.concat` for end-to-end tests.

Scenarios → pinned C cases:

1. `BasicTwoDwellsTwoEntities` → C1
2. `BelowDurationThreshold` → C2
3. `LongGapInsideCluster` → C3
4. `PureNoiseEntity(jitter_radius_m=2000)` → C4
5. `SingletonLocation` → C5
6. `CrossMidnightDwell` → C6
7. `MultiDayDwell(span_days=3)` → C6 + continuous_presence
8. `ExactThresholdCoDwell` → C7
9. `DuplicateTimestampsSameCoord` → C8a
10. `DuplicateTimestampsDifferentCoord` → C8b
11. `AntimeridianCrosser` → C9
12. `NearPole(lat=87)` → C10
13. `EmptyDataset` → C11
14. `SingleEventEntity` → C12
15. `OnlyTransitEntity` → C13
16. `MegaHub(n_entities=120)` → C14
17. `IdenticalPointStack(n=10)` → C15
18. `SpeedOutlier(jump_m=50_000)` → C16
19. `JustShortOfMinEvents` → C17
20. `H3CellBoundaryJitter(amp_m=5)` → C18
21. `CollinearHull(n=5, collinear=True)` → C19
22. `AbsorbingTerminal` → C20
23. `CompleteContainmentCoDwell` → C21
24. `MicrosecondTiedArrivals` → C22
25. `MinimalSequencesForPrefixSpan` → C23
26. `ZeroOrNegativeWeights` (composite-side validator) → C24
27. `StringOfBeadsCorridor` (per-entity centroids along a road) → C25; verifies the diameter guard fires

---

## G. Verification Plan

### Per-stage unit tests
- **Preprocess**: dtype coercion, NaN drop logging, range filter, dedupe, speed-outlier flag.
- **H3**: known `(lat,lon) → cell` at known res; `polygon.contains(input_point)` roundtrip.
- **HDBSCAN**: two-Gaussians-plus-noise → assert 2 clusters per entity, noise count correct.
- **Merge**: two entities, one cluster each within bound → 1 merged location. Plus far cluster → 3 locations. String-of-beads → guard fires; final locations satisfy diameter ≤ K × spatial_bound.
- **Dwell extraction**: tiny golden table covering C1, C2, C3, C17.
- **Hull projection roundtrip**: known points → AEQD hull → unproject → assert points lie inside returned WGS84 hull within `1e-5°`.
- **Co-dwell**: 4-entity scenario covering no-overlap, partial, exact-threshold, full containment; assert exact overlap durations.
- **Transitions / Markov**: hand-built 3-location dataset; per-entity probs sum to 1; counts decompose to global.
- **PrefixSpan**: 5-sequence input with hand-enumerated frequent patterns at `minsup=2`.
- **First/last/continuous of day**: 3-day spanning dwell asserts: 1 first-of-day on start date, 1 last-of-day on end date, 1 `continuous_presence_days` for the middle date.
- **Scoring**: percentile normalization → mean ≈ 0.5 on uniform input; composite from `*_norm` columns matches direct linear combination.

### End-to-end test
Compose scenarios {1, 3, 6, 14, 25, 27} into a ~10K-row composite dataset. Run pipeline twice — once H3 (`res=9`), once HDBSCAN (`spatial_bound_m=100`). Assert: all invariants from C hold; cache fills with all stage files + `_SUCCESS`; rerunning with identical params is sub-second and reads cache.

### Performance smoke test
- 100K events: full pipeline ≤ 30s on a developer laptop.
- H3 should be 3–5× faster than HDBSCAN; assert HDBSCAN ≤ 60s as a soft guard.

### Runtime invariants behind `--strict`
All invariants from section C are checked as Polars `assert`-style filters; first failure raises with the offending rows.

---

## H. Critical Files (greenfield — to be created)

Recommended module paths under `/home/kihaji/projects/analytic/`:

- `src/dwell/pipeline.py` — DAG orchestration; cache key construction; `_SUCCESS` semantics; per-stage timing in `run_meta.json`.
- `src/dwell/preprocess.py` — B.1 (validation, dedupe, sort, speed-outlier flag).
- `src/dwell/clustering.py` — B.2 (H3 + HDBSCAN), AEQD projection helpers. Heaviest correctness file.
- `src/dwell/merge_locations.py` — B.3 + B.5 (DBSCAN merge, diameter guard with single-link split, hull/geometry).
- `src/dwell/dwells.py` — B.4 strict run-length encoding. Tiny, pivotal.
- `src/dwell/stats.py` — B.6, B.7 (per-dwell, per-location, `per_id_per_location_stats`).
- `src/dwell/codwells.py` — B.8.
- `src/dwell/transitions.py` — B.9.
- `src/dwell/sequences.py` — B.10 (PrefixSpan wrapper).
- `src/dwell/markov.py` — B.11.
- `src/dwell/first_last_day.py` — B.12 (3-bucket attribution).
- `src/dwell/scoring.py` — B.13 (raw, normalize, composite hook).
- `src/dwell/cache.py` — params hashing, atomic writes, lockfile, layout.
- `src/dwell/exports.py` — CSV, Excel (Polars `write_excel`), GeoJSON, Shapefile (pyogrio).
- `src/dwell/synthdata.py` — all scenarios in F.
- `src/dwell/schemas.py` — Polars schema constants for every table (A).
- `src/dwell/params.py` — Pydantic models for run params + weights validation.
- `tests/` — mirror module layout; one file per pipeline stage.
- `src/app/api.py`, `src/app/dash_app.py` — FastAPI + Dash mount (out of scope here; consume cached Parquets).

---

## I. Open Questions / Risks (for v1 sign-off)

1. **Per-entity HDBSCAN min_cluster_size policy.** Currently `max(min_dwell_events, 5)` — fixed regardless of entity event count. Sparse entities with few events miss stops they "really" had. Adaptive option `max(min_dwell_events, ceil(0.01 * n_events_for_entity))` proposed but not chosen; surfaced in `entity_stats` as a diagnostic. Decide v2.
2. **`location_id` stability across runs.** Both H3 and HDBSCAN re-assign dense integer location ids per run. Any UI bookmark or saved analysis must carry `(params_hash, location_id)` — make this explicit at the API boundary.
3. **H3 jitter mitigation (`cell_buffer_events`).** Deferred to v2 (you chose strict). The fragmentation impact is visible via `entity_stats.n_fragmented_runs` so users can see whether it's hurting them in practice before we add a config.
4. **Chain-origin λ default.** Currently 0.5. Surface as admin/config; revisit after looking at real data distributions.
5. **PrefixSpan `maxlen=6`.** Will miss longer routine chains if real datasets show 8–10-stop daily routines. The param is in the cache key so re-running with a higher cap is cheap to test.
6. **Dataset content hash.** For 100K rows hashing the input Parquet bytes is milliseconds. Note for future scaling — switch to (row_count, min/max timestamp, sampled hash) at 10M+.
7. **Concave shapes.** Convex hulls oversize L-shaped or split stops. Optional v2: alpha-shape with tunable α. Out of scope v1.
8. **`a_arrived_first` semantics with microsecond ties.** Resolved deterministically by `entity_idx`; documented. If users want "simultaneous arrival" detection, add `same_arrival_threshold_s` filter as a v2 derived view.
9. **Co-dwell scoring.** Spec asks for scores on dwell locations but not directly on co-dwell pairs. A "Sociality" score per location (mean overlap-time per co-dwelling entity, etc.) is a natural v2 addition.

---

## J. Verification — How to validate end-to-end before declaring done

1. Generate `composite_dataset.parquet` from `synthdata` scenarios {1, 3, 6, 14, 25, 27}.
2. Run the pipeline twice — once with H3 (`spatial_bound_m=100` → `res ≈ 9`), once with HDBSCAN (`spatial_bound_m=100`).
3. Assert all invariants from section C.
4. Re-run with identical params: must hit cache (single Parquet read; sub-second).
5. Change weights only: must recompute only `composite/<weights_hash>.parquet`.
6. Change `min_codwell_seconds`: must recompute only `codwells.parquet`.
7. Export each scenario's `locations` to GeoJSON and Shapefile (with and without buffer-on-degenerate); load back with geopandas, check geometry types and counts.
8. Spot-check a `MegaHub` location: `n_unique_ids_dwelled ≈ 120`, `chain_origin_count_plus_entropy_raw` close to `log(n_distinct_next)` if outbound is uniform.
9. Spot-check a `StringOfBeadsCorridor`: confirm `run_meta.json` records `merge_guard_split_iterations ≥ 1` and every resulting location's `hull_diameter_m ≤ K * spatial_bound_m`.
10. Run the performance smoke test (100K events ≤ 30s H3, ≤ 60s HDBSCAN).

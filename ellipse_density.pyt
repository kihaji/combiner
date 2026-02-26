"""
ArcGIS Pro Python Toolbox: Ellipse Density

Computes probability density surfaces from error ellipse data using analytical
Gaussian PDF evaluation. Outputs GeoTIFF rasters in EPSG:4326.

Ported from density_analytical.py and supporting modules. Self-contained —
requires only numpy, scipy, arcpy, and osgeo (GDAL), all available in
ArcGIS Pro's bundled Python environment.
"""

import os
import math
import warnings
import functools
from dataclasses import dataclass
from datetime import datetime

import numpy as np
from scipy import stats
import arcpy
from osgeo import gdal, osr


# =============================================================================
# Constants
# =============================================================================

METERS_PER_DEGREE_LAT = 111_320
MIN_CELLSIZE_M = 11.0  # ~0.0001 degrees at equator
DEFAULT_CONFIDENCE = 0.95
GDAL_OPTIONS = ["COMPRESS=LZW", "SPARSE_OK=TRUE"]

# Unit constants
METERS = "M"
KILOMETERS = "KM"
NAUTICAL_MILES = "NM"
DEGREES = "DEGREES"
RADIANS = "RADIANS"

# Studied parameter ranges from correlation study
STUDIED_MEAN_SEMI_MINOR = [30, 60, 100, 150, 250]
STUDIED_CV = [0.2, 0.4, 0.6, 0.8]
STUDIED_N_ELLIPSES = [5000, 10000, 20000]


# =============================================================================
# Embedded Correlation Study Data (trimmed from correlation_study_results.json)
# =============================================================================

CORRELATION_STUDY_DATA = {
    "mean_30": {
        "optimal_stats": {"optimal_cells_across": 5},
        "aggregated_results": [
            {"cells_across": 5, "mean_error_m": 1674.2, "std_error_m": 851.95, "mean_centroid_error_m": 1123.68},
            {"cells_across": 7, "mean_error_m": 1870.94, "std_error_m": 931.03, "mean_centroid_error_m": 1124.76},
            {"cells_across": 10, "mean_error_m": 1870.94, "std_error_m": 931.03, "mean_centroid_error_m": 1124.77},
            {"cells_across": 14, "mean_error_m": 1870.94, "std_error_m": 931.03, "mean_centroid_error_m": 1124.76},
            {"cells_across": 20, "mean_error_m": 1870.94, "std_error_m": 931.03, "mean_centroid_error_m": 1124.77},
        ],
    },
    "mean_60": {
        "optimal_stats": {"optimal_cells_across": 7},
        "aggregated_results": [
            {"cells_across": 5, "mean_error_m": 1791.16, "std_error_m": 835.33, "mean_centroid_error_m": 1153.72},
            {"cells_across": 7, "mean_error_m": 1768.13, "std_error_m": 831.82, "mean_centroid_error_m": 1154.75},
            {"cells_across": 10, "mean_error_m": 1798.06, "std_error_m": 903.35, "mean_centroid_error_m": 1155.16},
            {"cells_across": 14, "mean_error_m": 1839.41, "std_error_m": 909.5, "mean_centroid_error_m": 1155.39},
            {"cells_across": 20, "mean_error_m": 1839.41, "std_error_m": 909.5, "mean_centroid_error_m": 1155.39},
        ],
    },
    "mean_100": {
        "optimal_stats": {"optimal_cells_across": 5},
        "aggregated_results": [
            {"cells_across": 5, "mean_error_m": 1355.88, "std_error_m": 538.87, "mean_centroid_error_m": 1167.49},
            {"cells_across": 7, "mean_error_m": 1782.16, "std_error_m": 893.8, "mean_centroid_error_m": 1168.79},
            {"cells_across": 10, "mean_error_m": 1795.0, "std_error_m": 904.73, "mean_centroid_error_m": 1170.15},
            {"cells_across": 14, "mean_error_m": 1736.19, "std_error_m": 869.08, "mean_centroid_error_m": 1170.79},
            {"cells_across": 20, "mean_error_m": 1735.69, "std_error_m": 869.34, "mean_centroid_error_m": 1171.49},
        ],
    },
    "mean_150": {
        "optimal_stats": {"optimal_cells_across": 5},
        "aggregated_results": [
            {"cells_across": 5, "mean_error_m": 1266.58, "std_error_m": 552.52, "mean_centroid_error_m": 1170.38},
            {"cells_across": 7, "mean_error_m": 1323.86, "std_error_m": 542.93, "mean_centroid_error_m": 1173.44},
            {"cells_across": 10, "mean_error_m": 1348.84, "std_error_m": 536.78, "mean_centroid_error_m": 1175.55},
            {"cells_across": 14, "mean_error_m": 1353.76, "std_error_m": 536.8, "mean_centroid_error_m": 1176.1},
            {"cells_across": 20, "mean_error_m": 1349.95, "std_error_m": 538.02, "mean_centroid_error_m": 1177.1},
        ],
    },
    "mean_250": {
        "optimal_stats": {"optimal_cells_across": 7},
        "aggregated_results": [
            {"cells_across": 5, "mean_error_m": 1329.61, "std_error_m": 667.15, "mean_centroid_error_m": 1169.53},
            {"cells_across": 7, "mean_error_m": 1281.25, "std_error_m": 583.12, "mean_centroid_error_m": 1173.87},
            {"cells_across": 10, "mean_error_m": 1335.68, "std_error_m": 531.02, "mean_centroid_error_m": 1176.24},
            {"cells_across": 14, "mean_error_m": 1370.21, "std_error_m": 572.04, "mean_centroid_error_m": 1177.87},
            {"cells_across": 20, "mean_error_m": 1348.43, "std_error_m": 540.09, "mean_centroid_error_m": 1179.02},
        ],
    },
    "cv_0.2": {
        "optimal_stats": {"optimal_cells_across": 5},
        "aggregated_results": [
            {"cells_across": 5, "mean_error_m": 1208.8, "std_error_m": 692.27, "mean_centroid_error_m": 1156.26},
            {"cells_across": 7, "mean_error_m": 1217.89, "std_error_m": 689.63, "mean_centroid_error_m": 1157.7},
            {"cells_across": 10, "mean_error_m": 1210.16, "std_error_m": 695.41, "mean_centroid_error_m": 1158.77},
            {"cells_across": 14, "mean_error_m": 1214.02, "std_error_m": 696.2, "mean_centroid_error_m": 1159.55},
            {"cells_across": 20, "mean_error_m": 1214.92, "std_error_m": 696.68, "mean_centroid_error_m": 1159.98},
        ],
    },
    "cv_0.4": {
        "optimal_stats": {"optimal_cells_across": 7},
        "aggregated_results": [
            {"cells_across": 5, "mean_error_m": 1623.55, "std_error_m": 817.17, "mean_centroid_error_m": 1144.71},
            {"cells_across": 7, "mean_error_m": 1250.21, "std_error_m": 579.57, "mean_centroid_error_m": 1145.23},
            {"cells_across": 10, "mean_error_m": 1737.63, "std_error_m": 729.37, "mean_centroid_error_m": 1146.97},
            {"cells_across": 14, "mean_error_m": 1590.61, "std_error_m": 914.53, "mean_centroid_error_m": 1147.62},
            {"cells_across": 20, "mean_error_m": 1591.33, "std_error_m": 912.43, "mean_centroid_error_m": 1147.99},
        ],
    },
    "cv_0.6": {
        "optimal_stats": {"optimal_cells_across": 20},
        "aggregated_results": [
            {"cells_across": 5, "mean_error_m": 1577.91, "std_error_m": 441.55, "mean_centroid_error_m": 1120.98},
            {"cells_across": 7, "mean_error_m": 1565.92, "std_error_m": 1063.56, "mean_centroid_error_m": 1118.22},
            {"cells_across": 10, "mean_error_m": 1738.95, "std_error_m": 726.15, "mean_centroid_error_m": 1125.04},
            {"cells_across": 14, "mean_error_m": 1724.04, "std_error_m": 1158.76, "mean_centroid_error_m": 1120.75},
            {"cells_across": 20, "mean_error_m": 1564.15, "std_error_m": 901.35, "mean_centroid_error_m": 1122.84},
        ],
    },
    "cv_0.8": {
        "optimal_stats": {"optimal_cells_across": 20},
        "aggregated_results": [
            {"cells_across": 5, "mean_error_m": 1388.99, "std_error_m": 655.51, "mean_centroid_error_m": 1115.33},
            {"cells_across": 7, "mean_error_m": 1610.87, "std_error_m": 833.51, "mean_centroid_error_m": 1103.42},
            {"cells_across": 10, "mean_error_m": 1575.78, "std_error_m": 877.32, "mean_centroid_error_m": 1107.64},
            {"cells_across": 14, "mean_error_m": 1560.15, "std_error_m": 915.69, "mean_centroid_error_m": 1101.29},
            {"cells_across": 20, "mean_error_m": 1350.36, "std_error_m": 641.13, "mean_centroid_error_m": 1097.41},
        ],
    },
    "n_5000": {
        "optimal_stats": {"optimal_cells_across": 14},
        "aggregated_results": [
            {"cells_across": 5, "mean_error_m": 1054.74, "std_error_m": 441.88, "mean_centroid_error_m": 1001.81},
            {"cells_across": 7, "mean_error_m": 1106.66, "std_error_m": 491.26, "mean_centroid_error_m": 1004.26},
            {"cells_across": 10, "mean_error_m": 1086.22, "std_error_m": 546.98, "mean_centroid_error_m": 1005.11},
            {"cells_across": 14, "mean_error_m": 970.03, "std_error_m": 502.18, "mean_centroid_error_m": 1005.67},
            {"cells_across": 20, "mean_error_m": 971.95, "std_error_m": 501.46, "mean_centroid_error_m": 1005.94},
        ],
    },
    "n_10000": {
        "optimal_stats": {"optimal_cells_across": 5},
        "aggregated_results": [
            {"cells_across": 5, "mean_error_m": 983.52, "std_error_m": 324.25, "mean_centroid_error_m": 1011.06},
            {"cells_across": 7, "mean_error_m": 1680.12, "std_error_m": 979.29, "mean_centroid_error_m": 1013.94},
            {"cells_across": 10, "mean_error_m": 1176.55, "std_error_m": 615.42, "mean_centroid_error_m": 1015.37},
            {"cells_across": 14, "mean_error_m": 1173.18, "std_error_m": 618.68, "mean_centroid_error_m": 1015.72},
            {"cells_across": 20, "mean_error_m": 1175.18, "std_error_m": 618.36, "mean_centroid_error_m": 1016.16},
        ],
    },
    "n_20000": {
        "optimal_stats": {"optimal_cells_across": 5},
        "aggregated_results": [
            {"cells_across": 5, "mean_error_m": 1135.2, "std_error_m": 497.51, "mean_centroid_error_m": 1024.35},
            {"cells_across": 7, "mean_error_m": 1472.54, "std_error_m": 649.37, "mean_centroid_error_m": 1025.61},
            {"cells_across": 10, "mean_error_m": 1323.8, "std_error_m": 609.54, "mean_centroid_error_m": 1026.52},
            {"cells_across": 14, "mean_error_m": 1384.61, "std_error_m": 608.76, "mean_centroid_error_m": 1027.31},
            {"cells_across": 20, "mean_error_m": 1386.62, "std_error_m": 605.99, "mean_centroid_error_m": 1027.57},
        ],
    },
}


# =============================================================================
# Helper Functions
# =============================================================================


@functools.cache
def chi2_for_confidence(confidence):
    """Get chi-squared value for given confidence level with 2 degrees of freedom."""
    return stats.chi2.ppf(confidence, 2)


@functools.lru_cache(maxsize=128)
def meters_per_degree_lon(lat):
    """Compute meters per degree longitude at given latitude."""
    return METERS_PER_DEGREE_LAT * math.cos(math.radians(lat))


def precompute_covariances(semi_major, semi_minor, azimuth_deg, confidence):
    """
    Vectorized covariance matrix computation for multiple ellipses.

    Returns the three unique components (cov_00, cov_01, cov_11) of the
    symmetric 2x2 covariance matrix for each ellipse.
    """
    chi2 = stats.chi2.ppf(confidence, 2)
    lambda_major = (semi_major ** 2) / chi2
    lambda_minor = (semi_minor ** 2) / chi2
    # Rotation: azimuth is CW from north, convert to CCW from east
    theta = np.radians(90.0 - azimuth_deg)
    cos_t = np.cos(theta)
    sin_t = np.sin(theta)
    # Covariance matrix C = R @ D @ R.T
    cov_00 = lambda_major * cos_t**2 + lambda_minor * sin_t**2
    cov_01 = (lambda_major - lambda_minor) * cos_t * sin_t
    cov_11 = lambda_major * sin_t**2 + lambda_minor * cos_t**2
    return cov_00, cov_01, cov_11


# =============================================================================
# Grid Functions
# =============================================================================


def compute_cell_size(semi_minor_values, cells_across=10, basis="avg", mode="fixed"):
    """
    Compute cell size based on ellipse sizes and target resolution.

    Returns (cell_size_m, effective_cells_across).
    """
    effective_cells_across = cells_across
    if mode == "auto":
        mean = np.mean(semi_minor_values)
        cv = float(np.std(semi_minor_values) / mean) if mean > 0 else 0.0
        effective_cells_across = compute_adaptive_cells_across(cv)
    elif mode != "fixed":
        raise ValueError(f"Unknown mode: {mode}. Use 'fixed' or 'auto'.")

    if basis == "avg":
        ref_size = np.mean(semi_minor_values)
    elif basis == "p10":
        ref_size = np.percentile(semi_minor_values, 10)
    else:
        raise ValueError(f"Unknown basis: {basis}. Use 'avg' or 'p10'.")

    cell_size_m = (2 * ref_size) / effective_cells_across
    final_cell_size = float(max(MIN_CELLSIZE_M, cell_size_m))

    if mode == "auto" and final_cell_size > cell_size_m:
        warnings.warn(f"Cell size floored at MIN_CELLSIZE_M ({MIN_CELLSIZE_M}m). Requested {cell_size_m:.1f}m with cells_across={effective_cells_across}.")

    return final_cell_size, effective_cells_across


def compute_adaptive_cells_across(cv):
    """Map coefficient of variation (CV) to recommended cells_across value (5-14 range)."""
    if cv < 0.3:
        return max(5, min(7, int(5 + (cv / 0.3) * 2)))
    elif cv < 0.5:
        return max(7, min(10, int(7 + ((cv - 0.3) / 0.2) * 3)))
    else:
        normalized_cv = min(1.0, cv)
        return max(10, min(14, int(10 + ((normalized_cv - 0.5) / 0.5) * 4)))


@dataclass
class GridSpec:
    """Grid specification for density computation."""
    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float
    n_lat: int
    n_lon: int
    cell_size_m: float
    cells_across_effective: int = 10


def compute_adaptive_grid(lats, lons, semi_major, semi_minor, cells_across=10, cell_size_basis="avg", cell_size_mode="fixed", cutoff_sigma=3.0, max_dim=2000, min_cells_across=4):
    """
    Compute grid specification with configurable cell sizing.

    Standalone function equivalent to Observations.compute_adaptive_grid().
    """
    n = len(lats)
    if n == 0:
        raise ValueError("No observations")

    lat_min, lat_max = float(lats.min()), float(lats.max())
    lon_min, lon_max = float(lons.min()), float(lons.max())
    center_lat = (lat_min + lat_max) / 2

    cell_size_m, effective_cells_across = compute_cell_size(semi_minor, cells_across, cell_size_basis, cell_size_mode)
    original_cell_size_m = cell_size_m

    # Add padding based on max semi-major to ensure largest ellipses at edges aren't truncated
    max_semi_major = float(semi_major.max())
    padding_m = max_semi_major * cutoff_sigma
    padding_lat = padding_m / METERS_PER_DEGREE_LAT
    padding_lon = padding_m / meters_per_degree_lon(center_lat)
    lat_min -= padding_lat
    lat_max += padding_lat
    lon_min -= padding_lon
    lon_max += padding_lon

    lat_extent_m = (lat_max - lat_min) * METERS_PER_DEGREE_LAT
    lon_extent_m = (lon_max - lon_min) * meters_per_degree_lon(center_lat)
    n_lat = max(10, int(lat_extent_m / cell_size_m))
    n_lon = max(10, int(lon_extent_m / cell_size_m))

    if n_lat > max_dim or n_lon > max_dim:
        scale = max_dim / max(n_lat, n_lon)
        n_lat = int(n_lat * scale)
        n_lon = int(n_lon * scale)
        cell_size_m = original_cell_size_m / scale
        ref_size = (np.mean(semi_minor) if cell_size_basis == "avg" else np.percentile(semi_minor, 10))
        actual_cells_across = (2 * ref_size) / cell_size_m
        if actual_cells_across < min_cells_across:
            warnings.warn(f"Grid resolution reduced: {actual_cells_across:.1f} cells across ellipse diameter (target: {cells_across}, min: {min_cells_across}). Consider increasing max_dim beyond {max_dim}.")

    return GridSpec(lat_min, lat_max, lon_min, lon_max, n_lat, n_lon, cell_size_m, effective_cells_across)


# =============================================================================
# Density Kernel (numpy vectorized, replacing numba JIT)
# =============================================================================


def compute_density_grid(lats, lons, semi_major, semi_minor, azimuth, confidence, cov_00, cov_01, cov_11, grid, weights=None, cutoff_sigma=3.0, progress_callback=None):
    """
    Compute combined probability density on a grid using precision-weighted summation.

    Per-ellipse vectorized numpy implementation (no numba required).

    Args:
        lats, lons: Observation coordinates (degrees)
        semi_major, semi_minor: Ellipse axes (meters)
        azimuth: Azimuth (degrees CW from north)
        confidence: Confidence levels (0-1)
        cov_00, cov_01, cov_11: Pre-computed covariance components
        grid: GridSpec from compute_adaptive_grid()
        weights: Optional per-observation weights (from dwell detection)
        cutoff_sigma: Only compute density within this many sigma
        progress_callback: Optional callable(percent_int) for progress reporting

    Returns:
        2D density grid array with shape (n_lat, n_lon)
    """
    n = len(lats)
    if n == 0:
        return np.zeros((grid.n_lat, grid.n_lon), dtype=np.float64)

    # Create coordinate arrays
    lat_arr = np.linspace(grid.lat_min, grid.lat_max, grid.n_lat)
    lon_arr = np.linspace(grid.lon_min, grid.lon_max, grid.n_lon)
    density = np.zeros((grid.n_lat, grid.n_lon), dtype=np.float64)

    center_lat = (grid.lat_min + grid.lat_max) / 2
    m_per_deg_lon = meters_per_degree_lon(center_lat)

    # Pre-compute grid coordinates in meters (1D arrays)
    lon_coords_m = (lon_arr - grid.lon_min) * m_per_deg_lon
    lat_coords_m = (lat_arr - grid.lat_min) * METERS_PER_DEGREE_LAT

    cell_size_lon_m = (lon_arr[1] - lon_arr[0]) * m_per_deg_lon if len(lon_arr) > 1 else 1.0
    cell_size_lat_m = (lat_arr[1] - lat_arr[0]) * METERS_PER_DEGREE_LAT if len(lat_arr) > 1 else 1.0

    cutoff_mahal_sq = cutoff_sigma ** 2

    # Vectorized coordinate conversion
    centers_x_m = (lons - grid.lon_min) * m_per_deg_lon
    centers_y_m = (lats - grid.lat_min) * METERS_PER_DEGREE_LAT
    bbox_radii_m = semi_major * cutoff_sigma

    # Vectorized determinant and validity
    det_cov = cov_00 * cov_11 - cov_01**2
    valid_mask = det_cov > 0

    inv_det = np.where(valid_mask, 1.0 / det_cov, 0.0)
    cov_inv_00 = cov_11 * inv_det
    cov_inv_01 = -cov_01 * inv_det
    cov_inv_11 = cov_00 * inv_det

    sqrt_det = np.where(valid_mask, np.sqrt(det_cov), 1.0)
    norm_factor = 1.0 / (2.0 * np.pi * sqrt_det)
    precision = 1.0 / sqrt_det
    base_weights = norm_factor * precision * confidence

    if weights is not None:
        base_weights = base_weights * weights

    norm_weights = np.where(valid_mask, base_weights, 0.0)

    # Get indices of valid ellipses
    valid_indices = np.where(valid_mask)[0]
    n_valid = len(valid_indices)

    if n_valid == 0:
        return density

    # Progress tracking
    last_pct = -1
    report_interval = max(1, n_valid // 20)  # report every 5%

    # Loop over valid ellipses, vectorize inner grid operations
    for count, idx in enumerate(valid_indices):
        center_y = centers_y_m[idx]
        center_x = centers_x_m[idx]
        bbox_r = bbox_radii_m[idx]

        # Compute bounding box indices
        lat_min_idx = max(0, int((center_y - bbox_r) / cell_size_lat_m) - 1)
        lat_max_idx = min(grid.n_lat, int((center_y + bbox_r) / cell_size_lat_m) + 2)
        lon_min_idx = max(0, int((center_x - bbox_r) / cell_size_lon_m) - 1)
        lon_max_idx = min(grid.n_lon, int((center_x + bbox_r) / cell_size_lon_m) + 2)

        if lat_min_idx >= lat_max_idx or lon_min_idx >= lon_max_idx:
            continue

        # Extract coordinate slices for bounding box
        dy = lat_coords_m[lat_min_idx:lat_max_idx] - center_y  # shape (n_bbox_lat,)
        dx = lon_coords_m[lon_min_idx:lon_max_idx] - center_x  # shape (n_bbox_lon,)

        # Broadcast to 2D: dy along rows, dx along columns
        dy_2d = dy[:, np.newaxis]  # (n_bbox_lat, 1)
        dx_2d = dx[np.newaxis, :]  # (1, n_bbox_lon)

        # Mahalanobis distance squared
        inv00 = cov_inv_00[idx]
        inv01 = cov_inv_01[idx]
        inv11 = cov_inv_11[idx]
        mahal_sq = dx_2d**2 * inv00 + 2.0 * dx_2d * dy_2d * inv01 + dy_2d**2 * inv11

        # Mask and accumulate
        mask = mahal_sq <= cutoff_mahal_sq
        contribution = np.where(mask, norm_weights[idx] * np.exp(-0.5 * mahal_sq), 0.0)

        # Row flip to match coordinate convention: grid row 0 = lat_max
        flipped_lat_start = grid.n_lat - lat_max_idx
        flipped_lat_end = grid.n_lat - lat_min_idx
        density[flipped_lat_start:flipped_lat_end, lon_min_idx:lon_max_idx] += contribution[::-1]

        # Progress reporting
        if progress_callback is not None and count % report_interval == 0:
            pct = int(100 * count / n_valid)
            if pct != last_pct:
                progress_callback(pct)
                last_pct = pct

    if progress_callback is not None:
        progress_callback(100)

    return density


# =============================================================================
# Dwell Detection
# =============================================================================

DEFAULT_REFERENCE_RATE_SEC = 60.0


@dataclass
class DwellConfig:
    """Configuration parameters for dwell detection."""
    min_observations: int = 3
    min_dwell_time_sec: float = 300.0
    max_dwell_radius_m: float = 100.0
    max_time_gap_sec: float = 1800.0
    per_device: bool = True
    reference_rate_sec: float = DEFAULT_REFERENCE_RATE_SEC


@dataclass
class DwellPeriod:
    """Represents a single detected dwell period."""
    dwell_id: int
    device_id: object
    observation_indices: object
    start_time_sec: float
    end_time_sec: float
    duration_sec: float
    centroid_lat: float
    centroid_lon: float
    n_observations: int


@dataclass
class DwellResult:
    """Complete result of dwell detection."""
    config: DwellConfig
    dwells: list
    dwell_membership: object
    observation_weights: object
    n_dwell_observations: int
    n_transit_observations: int
    total_dwell_time_sec: float


def _haversine_distance_m(lat1, lon1, lat2, lon2):
    """Flat-earth distance approximation in meters (accurate for dwell scales)."""
    center_lat = (lat1 + lat2) / 2
    m_per_deg_lon = meters_per_degree_lon(center_lat)
    dlat_m = (lat2 - lat1) * METERS_PER_DEGREE_LAT
    dlon_m = (lon2 - lon1) * m_per_deg_lon
    return np.sqrt(dlat_m ** 2 + dlon_m ** 2)


def _detect_dwells_for_device(indices, timestamps_sec, lats, lons, device_id, config, next_dwell_id):
    """Detect dwells for a single device's observations."""
    n = len(indices)
    if n == 0:
        return [], next_dwell_id

    sort_order = np.argsort(timestamps_sec)
    sorted_indices = indices[sort_order]
    sorted_times = timestamps_sec[sort_order]
    sorted_lats = lats[sort_order]
    sorted_lons = lons[sort_order]

    dwells = []
    current_cluster_sorted_positions = []

    def finalize_cluster():
        nonlocal next_dwell_id
        if len(current_cluster_sorted_positions) < config.min_observations:
            return
        pos = np.array(current_cluster_sorted_positions)
        cluster_times = sorted_times[pos]
        cluster_lats = sorted_lats[pos]
        cluster_lons = sorted_lons[pos]
        duration = cluster_times[-1] - cluster_times[0]
        if duration < config.min_dwell_time_sec:
            return
        centroid_lat = float(np.mean(cluster_lats))
        centroid_lon = float(np.mean(cluster_lons))
        dwell = DwellPeriod(dwell_id=next_dwell_id, device_id=device_id, observation_indices=sorted_indices[pos].copy(),
                            start_time_sec=float(cluster_times[0]), end_time_sec=float(cluster_times[-1]), duration_sec=float(duration),
                            centroid_lat=centroid_lat, centroid_lon=centroid_lon, n_observations=len(pos))
        dwells.append(dwell)
        next_dwell_id += 1

    for i in range(n):
        if len(current_cluster_sorted_positions) == 0:
            current_cluster_sorted_positions.append(i)
            continue
        time_gap = sorted_times[i] - sorted_times[current_cluster_sorted_positions[-1]]
        if time_gap > config.max_time_gap_sec:
            finalize_cluster()
            current_cluster_sorted_positions = [i]
            continue
        pos = np.array(current_cluster_sorted_positions)
        centroid_lat = float(np.mean(sorted_lats[pos]))
        centroid_lon = float(np.mean(sorted_lons[pos]))
        dist = _haversine_distance_m(sorted_lats[i], sorted_lons[i], centroid_lat, centroid_lon)
        if dist > config.max_dwell_radius_m:
            finalize_cluster()
            current_cluster_sorted_positions = [i]
            continue
        current_cluster_sorted_positions.append(i)

    finalize_cluster()
    return dwells, next_dwell_id


def _compute_dwell_weights(n_observations, dwells, config):
    """Compute per-observation weights based on detected dwells."""
    dwell_membership = np.full(n_observations, -1, dtype=np.int64)
    weights = np.ones(n_observations, dtype=np.float64)
    for dwell in dwells:
        dwell_membership[dwell.observation_indices] = dwell.dwell_id
        time_per_obs = dwell.duration_sec / dwell.n_observations
        weight = time_per_obs / config.reference_rate_sec
        weights[dwell.observation_indices] = weight
    return dwell_membership, weights


def detect_dwells(n_observations, lats, lons, timestamps, device_ids=None, config=None):
    """
    Detect dwell periods and compute observation weights.

    Standalone function that operates on raw numpy arrays.

    Args:
        n_observations: Number of observations
        lats, lons: Coordinate arrays (degrees)
        timestamps: datetime64 array
        device_ids: Optional list of device ID strings
        config: DwellConfig (uses defaults if None)

    Returns:
        DwellResult with detected dwells and per-observation weights
    """
    if config is None:
        config = DwellConfig()

    if n_observations == 0:
        return DwellResult(config=config, dwells=[], dwell_membership=np.array([], dtype=np.int64),
                           observation_weights=np.array([], dtype=np.float64), n_dwell_observations=0, n_transit_observations=0, total_dwell_time_sec=0.0)

    timestamps_sec = timestamps.astype('datetime64[s]').astype(np.float64)
    all_indices = np.arange(n_observations, dtype=np.int64)
    all_dwells = []
    next_dwell_id = 0

    if config.per_device and device_ids is not None:
        unique_devices = list(set(device_ids))
        for dev_id in unique_devices:
            device_mask = np.array([d == dev_id for d in device_ids], dtype=bool)
            device_indices = all_indices[device_mask]
            if len(device_indices) == 0:
                continue
            found_dwells, next_dwell_id = _detect_dwells_for_device(device_indices, timestamps_sec[device_mask], lats[device_mask], lons[device_mask], dev_id, config, next_dwell_id)
            all_dwells.extend(found_dwells)
    else:
        found_dwells, next_dwell_id = _detect_dwells_for_device(all_indices, timestamps_sec, lats, lons, None, config, next_dwell_id)
        all_dwells.extend(found_dwells)

    dwell_membership, weights = _compute_dwell_weights(n_observations, all_dwells, config)
    n_dwell_obs = int(np.sum(dwell_membership >= 0))

    return DwellResult(config=config, dwells=all_dwells, dwell_membership=dwell_membership, observation_weights=weights,
                       n_dwell_observations=n_dwell_obs, n_transit_observations=n_observations - n_dwell_obs,
                       total_dwell_time_sec=sum(d.duration_sec for d in all_dwells))


# =============================================================================
# Error Estimation
# =============================================================================


@dataclass
class ErrorEstimate:
    """Error estimate result for density peak location."""
    expected_peak_error_m: float
    peak_error_std_m: float
    expected_centroid_error_m: float
    centroid_error_std_m: float
    confidence_interval_95_m: tuple
    factors: dict
    low_confidence: bool
    warning_message: object


def _find_nearest_values(target, values):
    """Find two nearest values in a sorted list and compute interpolation weight."""
    values = sorted(values)
    if target <= values[0]:
        return values[0], values[0], 0.0
    if target >= values[-1]:
        return values[-1], values[-1], 0.0
    for i in range(len(values) - 1):
        if values[i] <= target <= values[i + 1]:
            span = values[i + 1] - values[i]
            weight = (target - values[i]) / span if span > 0 else 0.0
            return values[i], values[i + 1], weight
    return values[-1], values[-1], 0.0


def _extract_optimal_errors(config_data):
    """Extract error metrics from the optimal cells_across configuration."""
    if "optimal_stats" in config_data and config_data["optimal_stats"]:
        optimal_cells = config_data["optimal_stats"].get("optimal_cells_across", 10)
    else:
        optimal_cells = 10
    for result in config_data.get("aggregated_results", []):
        if result["cells_across"] == optimal_cells:
            return (result["mean_error_m"], result["std_error_m"], result["mean_centroid_error_m"])
    if config_data.get("aggregated_results"):
        r = config_data["aggregated_results"][0]
        return (r["mean_error_m"], r["std_error_m"], r["mean_centroid_error_m"])
    return (1500.0, 500.0, 1200.0)


def _interpolate_errors(study_data, mean_semi_minor, cv, n_observations):
    """Interpolate error estimates from study data based on input parameters."""
    errors = []
    err_weights = []

    # 1. mean_semi_minor study (primary factor)
    lower_mean, upper_mean, mean_weight = _find_nearest_values(mean_semi_minor, STUDIED_MEAN_SEMI_MINOR)
    for studied_mean, w in [(lower_mean, 1 - mean_weight), (upper_mean, mean_weight)]:
        if w > 0:
            key = f"mean_{int(studied_mean)}"
            if key in study_data:
                peak_err, peak_std, centroid_err = _extract_optimal_errors(study_data[key])
                scale_factor = mean_semi_minor / studied_mean if studied_mean > 0 else 1.0
                errors.append((peak_err * scale_factor, peak_std * scale_factor, centroid_err * scale_factor))
                err_weights.append(w * 2.0)

    # 2. CV study (secondary)
    lower_cv, upper_cv, cv_weight = _find_nearest_values(cv, STUDIED_CV)
    for studied_cv, w in [(lower_cv, 1 - cv_weight), (upper_cv, cv_weight)]:
        if w > 0:
            key = f"cv_{studied_cv:.1f}"
            if key in study_data:
                peak_err, peak_std, centroid_err = _extract_optimal_errors(study_data[key])
                scale_factor = mean_semi_minor / 100.0
                errors.append((peak_err * scale_factor, peak_std * scale_factor, centroid_err * scale_factor))
                err_weights.append(w)

    # 3. n_ellipses study (tertiary)
    lower_n, upper_n, n_weight = _find_nearest_values(n_observations, STUDIED_N_ELLIPSES)
    for studied_n, w in [(int(lower_n), 1 - n_weight), (int(upper_n), n_weight)]:
        if w > 0:
            key = f"n_{int(studied_n)}"
            if key in study_data:
                peak_err, peak_std, centroid_err = _extract_optimal_errors(study_data[key])
                scale_factor = mean_semi_minor / 100.0
                errors.append((peak_err * scale_factor, peak_std * scale_factor, centroid_err * scale_factor))
                err_weights.append(w * 0.5)

    if not errors:
        return (1500.0 * mean_semi_minor / 100.0, 500.0 * mean_semi_minor / 100.0, 1200.0 * mean_semi_minor / 100.0)

    total_weight = sum(err_weights)
    peak_err = sum(e[0] * w for e, w in zip(errors, err_weights)) / total_weight
    peak_std = sum(e[1] * w for e, w in zip(errors, err_weights)) / total_weight
    centroid_err = sum(e[2] * w for e, w in zip(errors, err_weights)) / total_weight

    extrapolation_factor = 1.0
    if mean_semi_minor < STUDIED_MEAN_SEMI_MINOR[0] or mean_semi_minor > STUDIED_MEAN_SEMI_MINOR[-1]:
        extrapolation_factor *= 1.5
    if cv < STUDIED_CV[0] or cv > STUDIED_CV[-1]:
        extrapolation_factor *= 1.3
    if n_observations < STUDIED_N_ELLIPSES[0] or n_observations > STUDIED_N_ELLIPSES[-1]:
        extrapolation_factor *= 1.2
    peak_std *= extrapolation_factor

    return (peak_err, peak_std, centroid_err)


def estimate_error(n_observations, semi_minor_mean_m, semi_minor_cv, cell_size_m):
    """
    Estimate expected error for density peak location using embedded study data.

    Returns ErrorEstimate with expected error bounds and confidence intervals.
    """
    study_data = CORRELATION_STUDY_DATA
    peak_error, peak_std, centroid_error = _interpolate_errors(study_data, semi_minor_mean_m, semi_minor_cv, n_observations)
    centroid_std = peak_std * 0.8
    ci_low = max(0, peak_error - 1.96 * peak_std)
    ci_high = peak_error + 1.96 * peak_std

    low_confidence = peak_error > cell_size_m
    warning_message = None
    if low_confidence:
        warning_message = f"Estimated peak error ({peak_error:.0f}m) exceeds cell size ({cell_size_m:.0f}m). Density peak location may not be reliable. Consider using more observations or smaller ellipses."

    return ErrorEstimate(
        expected_peak_error_m=round(peak_error, 1), peak_error_std_m=round(peak_std, 1),
        expected_centroid_error_m=round(centroid_error, 1), centroid_error_std_m=round(centroid_std, 1),
        confidence_interval_95_m=(round(ci_low, 1), round(ci_high, 1)),
        factors={"n_observations": n_observations, "semi_minor_mean_m": round(semi_minor_mean_m, 1), "semi_minor_cv": round(semi_minor_cv, 3), "cell_size_m": round(cell_size_m, 1)},
        low_confidence=low_confidence, warning_message=warning_message)


# =============================================================================
# GeoTIFF Writer (GDAL)
# =============================================================================


def write_geotiff_gdal(output_path, data, origin, cell_size_deg):
    """
    Write density array to GeoTIFF file using GDAL.

    Args:
        output_path: Full path to output .tif file
        data: 2D density array (origin at bottom-left)
        origin: (lon_min, lat_min) tuple
        cell_size_deg: Cell size in degrees
    """
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)

    if data.dtype == np.float64:
        arr_type = gdal.GDT_Float64
    elif data.dtype == np.float32:
        arr_type = gdal.GDT_Float32
    else:
        arr_type = gdal.GDT_Int32

    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    # Flip vertically: GDAL expects origin at top-left
    rdata = data[::-1]

    driver = gdal.GetDriverByName("GTiff")
    out = driver.Create(output_path, rdata.shape[1], rdata.shape[0], 1, arr_type, GDAL_OPTIONS)
    out.SetProjection(srs.ExportToWkt())
    # GeoTransform: (top_left_x, x_pixel_size, x_rotation, top_left_y, y_rotation, -y_pixel_size)
    out.SetGeoTransform((origin[0], cell_size_deg, 0, origin[1] + rdata.shape[0] * cell_size_deg, 0, -cell_size_deg))
    band = out.GetRasterBand(1)
    band.WriteRaster(0, 0, rdata.shape[1], rdata.shape[0], rdata.tobytes())
    band.SetNoDataValue(0)
    band.FlushCache()
    band.ComputeStatistics(False)

    # Clean up GDAL handles
    band = None
    driver = None
    out = None


# =============================================================================
# Data Reader (arcpy.da.SearchCursor)
# =============================================================================


def read_input_data(input_data, lat_field, lon_field, major_field, minor_field, azimuth_field, timestamp_field=None, device_id_field=None):
    """
    Read input data using arcpy.da.SearchCursor.

    Returns dict of numpy arrays: lats, lons, semi_major, semi_minor, azimuth,
    and optionally timestamps, device_ids.
    """
    fields = [lat_field, lon_field, major_field, minor_field, azimuth_field]
    has_timestamp = timestamp_field is not None and timestamp_field != ""
    has_device_id = device_id_field is not None and device_id_field != ""
    if has_timestamp:
        fields.append(timestamp_field)
    if has_device_id:
        fields.append(device_id_field)

    rows_lat = []
    rows_lon = []
    rows_major = []
    rows_minor = []
    rows_azimuth = []
    rows_timestamp = []
    rows_device = []

    with arcpy.da.SearchCursor(input_data, fields) as cursor:
        for row in cursor:
            lat_val, lon_val, major_val, minor_val, az_val = row[0], row[1], row[2], row[3], row[4]
            # Skip rows with nulls in required fields
            if any(v is None for v in (lat_val, lon_val, major_val, minor_val, az_val)):
                continue
            rows_lat.append(float(lat_val))
            rows_lon.append(float(lon_val))
            rows_major.append(float(major_val))
            rows_minor.append(float(minor_val))
            rows_azimuth.append(float(az_val))
            if has_timestamp:
                rows_timestamp.append(row[5])
            if has_device_id:
                idx = 6 if has_timestamp else 5
                rows_device.append(row[idx])

    result = {
        "lats": np.array(rows_lat, dtype=np.float64),
        "lons": np.array(rows_lon, dtype=np.float64),
        "semi_major": np.array(rows_major, dtype=np.float64),
        "semi_minor": np.array(rows_minor, dtype=np.float64),
        "azimuth": np.array(rows_azimuth, dtype=np.float64),
    }

    if has_timestamp and rows_timestamp:
        # Convert datetime objects to datetime64
        result["timestamps"] = np.array(rows_timestamp, dtype='datetime64[ns]')
    if has_device_id and rows_device:
        result["device_ids"] = [str(d) for d in rows_device]

    return result


# =============================================================================
# ArcGIS Pro Python Toolbox
# =============================================================================


class Toolbox:
    def __init__(self):
        self.label = "Ellipse Density"
        self.alias = "ellipse_density"
        self.tools = [EllipseDensityTool]


class EllipseDensityTool:
    def __init__(self):
        self.label = "Ellipse Density"
        self.description = "Compute probability density surface from error ellipse observations using analytical Gaussian PDF evaluation."
        self.canRunInBackground = True

    def getParameterInfo(self):
        # 0 - input_data
        p_input = arcpy.Parameter(displayName="Input Data", name="input_data", datatype=["DEFeatureClass", "DETable", "GPFeatureLayer", "GPTableView"], parameterType="Required", direction="Input")

        # 1 - lat_field
        p_lat = arcpy.Parameter(displayName="Latitude Field", name="lat_field", datatype="Field", parameterType="Required", direction="Input")
        p_lat.value = "lat"
        p_lat.parameterDependencies = [p_input.name]
        p_lat.filter.list = ["Double", "Float"]

        # 2 - lon_field
        p_lon = arcpy.Parameter(displayName="Longitude Field", name="lon_field", datatype="Field", parameterType="Required", direction="Input")
        p_lon.value = "lon"
        p_lon.parameterDependencies = [p_input.name]
        p_lon.filter.list = ["Double", "Float"]

        # 3 - major_field
        p_major = arcpy.Parameter(displayName="Semi-Major Axis Field", name="major_field", datatype="Field", parameterType="Required", direction="Input")
        p_major.value = "semi_major"
        p_major.parameterDependencies = [p_input.name]
        p_major.filter.list = ["Double", "Float"]

        # 4 - minor_field
        p_minor = arcpy.Parameter(displayName="Semi-Minor Axis Field", name="minor_field", datatype="Field", parameterType="Required", direction="Input")
        p_minor.value = "semi_minor"
        p_minor.parameterDependencies = [p_input.name]
        p_minor.filter.list = ["Double", "Float"]

        # 5 - azimuth_field
        p_azimuth = arcpy.Parameter(displayName="Azimuth Field", name="azimuth_field", datatype="Field", parameterType="Required", direction="Input")
        p_azimuth.value = "azimuth"
        p_azimuth.parameterDependencies = [p_input.name]
        p_azimuth.filter.list = ["Double", "Float"]

        # 6 - output_geotiff
        p_output = arcpy.Parameter(displayName="Output GeoTIFF", name="output_geotiff", datatype="DEFile", parameterType="Required", direction="Output")
        p_output.filter.list = ["tif"]

        # 7 - cells_across
        p_cells = arcpy.Parameter(displayName="Cells Across Ellipse", name="cells_across", datatype="GPLong", parameterType="Optional", direction="Input", category="Grid Resolution")
        p_cells.value = 10
        p_cells.filter.type = "Range"
        p_cells.filter.list = [2, 50]

        # 8 - cell_size_basis
        p_basis = arcpy.Parameter(displayName="Cell Size Basis", name="cell_size_basis", datatype="GPString", parameterType="Optional", direction="Input", category="Grid Resolution")
        p_basis.value = "avg"
        p_basis.filter.type = "ValueList"
        p_basis.filter.list = ["avg", "p10"]

        # 9 - cell_size_mode
        p_mode = arcpy.Parameter(displayName="Cell Size Mode", name="cell_size_mode", datatype="GPString", parameterType="Optional", direction="Input", category="Grid Resolution")
        p_mode.value = "fixed"
        p_mode.filter.type = "ValueList"
        p_mode.filter.list = ["fixed", "auto"]

        # 10 - confidence_level
        p_conf = arcpy.Parameter(displayName="Confidence Level (%)", name="confidence_level", datatype="GPLong", parameterType="Optional", direction="Input", category="Ellipse Parameters")
        p_conf.value = 95
        p_conf.filter.type = "Range"
        p_conf.filter.list = [50, 99]

        # 11 - axis_units
        p_units = arcpy.Parameter(displayName="Axis Units", name="axis_units", datatype="GPString", parameterType="Optional", direction="Input", category="Ellipse Parameters")
        p_units.value = "M"
        p_units.filter.type = "ValueList"
        p_units.filter.list = ["M", "KM", "NM"]

        # 12 - is_semi_axis
        p_semi = arcpy.Parameter(displayName="Values Are Semi-Axes", name="is_semi_axis", datatype="GPBoolean", parameterType="Optional", direction="Input", category="Ellipse Parameters")
        p_semi.value = True

        # 13 - azimuth_units
        p_az_units = arcpy.Parameter(displayName="Azimuth Units", name="azimuth_units", datatype="GPString", parameterType="Optional", direction="Input", category="Ellipse Parameters")
        p_az_units.value = "DEGREES"
        p_az_units.filter.type = "ValueList"
        p_az_units.filter.list = ["DEGREES", "RADIANS"]

        # 14 - cutoff_sigma
        p_cutoff = arcpy.Parameter(displayName="Cutoff Sigma", name="cutoff_sigma", datatype="GPDouble", parameterType="Optional", direction="Input", category="Grid Resolution")
        p_cutoff.value = 3.0
        p_cutoff.filter.type = "Range"
        p_cutoff.filter.list = [1.0, 6.0]

        # 15 - max_grid_dimension
        p_maxdim = arcpy.Parameter(displayName="Max Grid Dimension", name="max_grid_dimension", datatype="GPLong", parameterType="Optional", direction="Input", category="Grid Resolution")
        p_maxdim.value = 2000
        p_maxdim.filter.type = "Range"
        p_maxdim.filter.list = [100, 10000]

        # 16 - estimate_error
        p_error = arcpy.Parameter(displayName="Estimate Error", name="estimate_error", datatype="GPBoolean", parameterType="Optional", direction="Input", category="Analysis Options")
        p_error.value = False

        # 17 - enable_dwell
        p_dwell = arcpy.Parameter(displayName="Enable Dwell Detection", name="enable_dwell", datatype="GPBoolean", parameterType="Optional", direction="Input", category="Dwell Detection")
        p_dwell.value = False

        # 18 - timestamp_field
        p_ts = arcpy.Parameter(displayName="Timestamp Field", name="timestamp_field", datatype="Field", parameterType="Optional", direction="Input", category="Dwell Detection")
        p_ts.parameterDependencies = [p_input.name]
        p_ts.enabled = False

        # 19 - device_id_field
        p_dev = arcpy.Parameter(displayName="Device ID Field", name="device_id_field", datatype="Field", parameterType="Optional", direction="Input", category="Dwell Detection")
        p_dev.parameterDependencies = [p_input.name]
        p_dev.enabled = False

        # 20 - min_dwell_observations
        p_dwell_obs = arcpy.Parameter(displayName="Min Dwell Observations", name="min_dwell_observations", datatype="GPLong", parameterType="Optional", direction="Input", category="Dwell Detection")
        p_dwell_obs.value = 3
        p_dwell_obs.enabled = False

        # 21 - min_dwell_time_sec
        p_dwell_time = arcpy.Parameter(displayName="Min Dwell Time (sec)", name="min_dwell_time_sec", datatype="GPDouble", parameterType="Optional", direction="Input", category="Dwell Detection")
        p_dwell_time.value = 300.0
        p_dwell_time.enabled = False

        # 22 - max_dwell_radius_m
        p_dwell_radius = arcpy.Parameter(displayName="Max Dwell Radius (m)", name="max_dwell_radius_m", datatype="GPDouble", parameterType="Optional", direction="Input", category="Dwell Detection")
        p_dwell_radius.value = 100.0
        p_dwell_radius.enabled = False

        # 23 - max_time_gap_sec
        p_time_gap = arcpy.Parameter(displayName="Max Time Gap (sec)", name="max_time_gap_sec", datatype="GPDouble", parameterType="Optional", direction="Input", category="Dwell Detection")
        p_time_gap.value = 1800.0
        p_time_gap.enabled = False

        # 24 - reference_rate_sec
        p_ref_rate = arcpy.Parameter(displayName="Reference Rate (sec)", name="reference_rate_sec", datatype="GPDouble", parameterType="Optional", direction="Input", category="Dwell Detection")
        p_ref_rate.value = 60.0
        p_ref_rate.enabled = False

        return [p_input, p_lat, p_lon, p_major, p_minor, p_azimuth, p_output,
                p_cells, p_basis, p_mode, p_conf, p_units, p_semi, p_az_units,
                p_cutoff, p_maxdim, p_error, p_dwell, p_ts, p_dev,
                p_dwell_obs, p_dwell_time, p_dwell_radius, p_time_gap, p_ref_rate]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        """Enable/disable dwell fields based on enable_dwell toggle."""
        dwell_enabled = parameters[17].value  # enable_dwell
        dwell_params = [18, 19, 20, 21, 22, 23, 24]  # timestamp_field through reference_rate_sec
        for idx in dwell_params:
            parameters[idx].enabled = bool(dwell_enabled)

    def updateMessages(self, parameters):
        """Validate that timestamp_field is set when dwell is enabled."""
        if parameters[17].value:  # enable_dwell
            if not parameters[18].altered or not parameters[18].value:
                parameters[18].setErrorMessage("Timestamp field is required when dwell detection is enabled.")

    def execute(self, parameters, messages):
        # Extract parameter values
        input_data = parameters[0].valueAsText
        lat_field = parameters[1].valueAsText
        lon_field = parameters[2].valueAsText
        major_field = parameters[3].valueAsText
        minor_field = parameters[4].valueAsText
        azimuth_field = parameters[5].valueAsText
        output_path = parameters[6].valueAsText
        cells_across = parameters[7].value or 10
        cell_size_basis = parameters[8].valueAsText or "avg"
        cell_size_mode = parameters[9].valueAsText or "fixed"
        confidence_level = parameters[10].value or 95
        axis_units = parameters[11].valueAsText or "M"
        is_semi_axis = parameters[12].value if parameters[12].value is not None else True
        azimuth_units = parameters[13].valueAsText or "DEGREES"
        cutoff_sigma = parameters[14].value or 3.0
        max_grid_dim = parameters[15].value or 2000
        do_error = parameters[16].value or False
        enable_dwell = parameters[17].value or False
        timestamp_field = parameters[18].valueAsText if enable_dwell else None
        device_id_field = parameters[19].valueAsText if enable_dwell else None
        min_dwell_obs = parameters[20].value or 3
        min_dwell_time = parameters[21].value or 300.0
        max_dwell_radius = parameters[22].value or 100.0
        max_time_gap = parameters[23].value or 1800.0
        reference_rate = parameters[24].value or 60.0

        # Ensure output has .tif extension
        if not (output_path.endswith(".tif") or output_path.endswith(".tiff")):
            output_path = output_path + ".tif"

        # ---- Step 1: Read input data ----
        messages.addMessage("Reading input data...")
        arcpy.SetProgressor("step", "Reading input data...", 0, 100, 1)
        data = read_input_data(input_data, lat_field, lon_field, major_field, minor_field, azimuth_field, timestamp_field, device_id_field)

        n_obs = len(data["lats"])
        if n_obs == 0:
            messages.addErrorMessage("No valid observations after reading input data and filtering nulls.")
            return

        messages.addMessage(f"Read {n_obs} observations.")

        # ---- Step 2: Unit conversions ----
        semi_major = data["semi_major"].copy()
        semi_minor = data["semi_minor"].copy()
        azimuth_arr = data["azimuth"].copy()

        if axis_units == "KM":
            semi_major *= 1000.0
            semi_minor *= 1000.0
        elif axis_units == "NM":
            semi_major *= 1852.0
            semi_minor *= 1852.0

        if not is_semi_axis:
            semi_major /= 2.0
            semi_minor /= 2.0

        if azimuth_units == "RADIANS":
            azimuth_arr = np.degrees(azimuth_arr)

        conf_decimal = confidence_level / 100.0
        confidence_arr = np.full(n_obs, conf_decimal, dtype=np.float64)

        # ---- Step 3: Pre-compute covariances ----
        messages.addMessage("Computing covariance matrices...")
        cov_00, cov_01, cov_11 = precompute_covariances(semi_major, semi_minor, azimuth_arr, confidence_arr)

        # ---- Step 4: Dwell detection (optional) ----
        obs_weights = None
        dwell_result = None
        if enable_dwell:
            messages.addMessage("Detecting dwell periods...")
            dwell_cfg = DwellConfig(min_observations=min_dwell_obs, min_dwell_time_sec=min_dwell_time, max_dwell_radius_m=max_dwell_radius,
                                    max_time_gap_sec=max_time_gap, per_device=(device_id_field is not None), reference_rate_sec=reference_rate)
            dwell_result = detect_dwells(n_obs, data["lats"], data["lons"], data.get("timestamps"), data.get("device_ids"), dwell_cfg)
            obs_weights = dwell_result.observation_weights
            messages.addMessage(f"Dwell detection: {len(dwell_result.dwells)} dwells found, {dwell_result.n_dwell_observations} dwell obs, {dwell_result.n_transit_observations} transit obs.")

        # ---- Step 5: Compute grid ----
        messages.addMessage("Computing grid specification...")
        grid = compute_adaptive_grid(data["lats"], data["lons"], semi_major, semi_minor, cells_across, cell_size_basis, cell_size_mode, cutoff_sigma, max_grid_dim)
        messages.addMessage(f"Grid: {grid.n_lat} x {grid.n_lon} cells, cell size {grid.cell_size_m:.1f}m, effective cells_across={grid.cells_across_effective}")

        # ---- Step 6: Compute density ----
        messages.addMessage("Computing density grid...")
        arcpy.SetProgressor("step", "Computing density...", 0, 100, 1)

        def progress_cb(pct):
            arcpy.SetProgressorPosition(pct)
            arcpy.SetProgressorLabel(f"Computing density... {pct}%")

        density = compute_density_grid(data["lats"], data["lons"], semi_major, semi_minor, azimuth_arr, confidence_arr,
                                       cov_00, cov_01, cov_11, grid, weights=obs_weights, cutoff_sigma=cutoff_sigma, progress_callback=progress_cb)

        # ---- Step 7: Write GeoTIFF ----
        messages.addMessage(f"Writing GeoTIFF to {output_path}...")
        cell_size_deg = grid.cell_size_m / METERS_PER_DEGREE_LAT
        origin = (grid.lon_min, grid.lat_min)
        write_geotiff_gdal(output_path, density, origin, cell_size_deg)
        messages.addMessage("GeoTIFF written successfully.")

        # ---- Step 8: Error estimation (optional) ----
        if do_error:
            messages.addMessage("Estimating error...")
            semi_minor_mean = float(np.mean(semi_minor))
            semi_minor_cv = float(np.std(semi_minor) / semi_minor_mean) if semi_minor_mean > 0 else 0.0
            err = estimate_error(n_obs, semi_minor_mean, semi_minor_cv, grid.cell_size_m)
            messages.addMessage(f"  Expected peak error: {err.expected_peak_error_m:.1f} m (+/- {err.peak_error_std_m:.1f} m)")
            messages.addMessage(f"  Expected centroid error: {err.expected_centroid_error_m:.1f} m")
            messages.addMessage(f"  95% CI: ({err.confidence_interval_95_m[0]:.1f}, {err.confidence_interval_95_m[1]:.1f}) m")
            if err.low_confidence and err.warning_message:
                messages.addWarningMessage(err.warning_message)

        # ---- Step 9: Summary ----
        nonzero = np.count_nonzero(density)
        total_cells = grid.n_lat * grid.n_lon
        max_density = float(density.max())
        messages.addMessage(f"Summary: {n_obs} observations, {grid.n_lat}x{grid.n_lon} grid ({total_cells} cells), {nonzero} non-zero cells, max density={max_density:.6g}")

    def postExecute(self, parameters):
        return

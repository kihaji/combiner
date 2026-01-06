"""
Datetime Parsing Exploration with Polars

This script creates test data with various datetime formats and demonstrates
parsing them to polars.Datetime with UTC timezone using built-in Polars methods.
"""

import polars as pl
from dataclasses import dataclass


# Top 10 most common datetime formats in data files (from research):
# 1. ISO 8601: YYYY-MM-DD (date only)
# 2. ISO 8601: YYYY-MM-DDTHH:mm:ssZ (UTC)
# 3. ISO 8601: YYYY-MM-DDTHH:mm:ss.sssZ (with milliseconds)
# 4. ISO 8601: YYYY-MM-DDTHH:mm:ss±HH:mm (with timezone offset)
# 5. US format: MM/DD/YYYY
# 6. European format: DD/MM/YYYY  (ambiguous with US - we'll use MM/DD/YYYY)
# 7. Alternative ISO-like: YYYY/MM/DD
# 8. Unix timestamp (seconds)
# 9. Unix timestamp (milliseconds)
# 10. Basic ISO: YYYYMMDD

# Additional common formats found in the wild:
# - YYYY-MM-DD HH:MM:SS (space separator instead of T)
# - MM-DD-YYYY, DD-MM-YYYY (with dashes)
# - Month DD, YYYY (e.g., "January 15, 2024")
# - DD Month YYYY (e.g., "15 January 2024")


# =============================================================================
# DATETIME FORMAT DEFINITIONS
# =============================================================================
# All supported datetime format patterns. Order matters for pl.coalesce() -
# more specific formats should come before less specific ones to avoid
# incorrect matches.
#
# TODO(human): Review and organize these formats by priority for your use case.
# Consider: Which formats are most common in your data? Should ambiguous formats
# (like US vs European date order) be handled differently?

DATETIME_FORMATS: list[str] = [
    # --- ISO 8601 with timezone ---
    "%Y-%m-%dT%H:%M:%S%.f%:z",   # 2024-03-15T14:30:00.123+00:00
    "%Y-%m-%dT%H:%M:%S%:z",      # 2024-03-15T14:30:00+00:00
    "%Y-%m-%dT%H:%M:%S%.fZ",     # 2024-03-15T14:30:00.123Z
    "%Y-%m-%dT%H:%M:%SZ",        # 2024-03-15T14:30:00Z

    # --- ISO 8601 without timezone (treated as UTC) ---
    "%Y-%m-%dT%H:%M:%S%.f",      # 2024-03-15T14:30:00.123
    "%Y-%m-%dT%H:%M:%S",         # 2024-03-15T14:30:00
    "%Y-%m-%dT%H:%M",            # 2024-03-15T14:30

    # --- Space-separated datetime ---
    "%Y-%m-%d %H:%M:%S%:z",      # 2024-03-15 14:30:00+00:00
    "%Y-%m-%d %H:%M:%S%.f",      # 2024-03-15 14:30:00.123
    "%Y-%m-%d %H:%M:%S",         # 2024-03-15 14:30:00
    "%Y-%m-%d",                  # 2024-03-15

    # --- US format MM/DD/YYYY ---
    "%m/%d/%Y %H:%M:%S",         # 03/15/2024 14:30:00
    "%m/%d/%Y %I:%M:%S %p",      # 03/15/2024 02:30:00 PM
    "%m/%d/%Y %I:%M %p",         # 03/15/2024 2:30 PM
    "%m/%d/%Y",                  # 03/15/2024

    # --- ISO with slashes ---
    "%Y/%m/%d %H:%M:%S",         # 2024/03/15 14:30:00
    "%Y/%m/%d",                  # 2024/03/15

    # --- European DD/MM/YYYY (ambiguous with US!) ---
    "%d/%m/%Y %H:%M:%S",         # 15/03/2024 14:30:00
    "%d/%m/%Y",                  # 15/03/2024
    "%d-%m-%Y %H:%M:%S",         # 15-03-2024 14:30:00
    "%d-%m-%Y",                  # 15-03-2024

    # --- US with dashes ---
    "%m-%d-%Y %H:%M:%S",         # 03-15-2024 14:30:00
    "%m-%d-%Y",                  # 03-15-2024

    # --- Dot separators ---
    "%Y.%m.%d %H:%M:%S",         # 2024.03.15 14:30:00
    "%Y.%m.%d",                  # 2024.03.15

    # --- Text month formats ---
    "%B %d, %Y %H:%M:%S",        # March 15, 2024 14:30:00
    "%B %d, %Y",                 # March 15, 2024
    "%d %B %Y %H:%M:%S",         # 15 March 2024 14:30:00
    "%d %B %Y",                  # 15 March 2024
    "%b %d, %Y",                 # Mar 15, 2024
    "%d %b %Y",                  # 15 Mar 2024
    "%d-%b-%Y %H:%M:%S",         # 15-Mar-2024 14:30:00
    "%d-%b-%Y",                  # 15-Mar-2024

    # --- Compact formats ---
    "%Y%m%dT%H%M%SZ",            # 20240315T143000Z
    "%Y%m%dT%H%M%S%:z",          # 20240315T143000+00:00
    "%Y%m%d%H%M%S",              # 20240315143000
    "%Y%m%d",                    # 20240315

    # --- Space with compact timezone ---
    "%Y-%m-%d %H:%M:%S%z",       # 2024-03-15 14:30:00+0000
    "%Y-%m-%d %H:%M:%S %z",      # 2024-03-15 14:30:00 +0000

    # --- RFC 2822 (email/HTTP dates) ---
    "%a, %d %b %Y %H:%M:%S%:z",  # Fri, 15 Mar 2024 14:30:00+00:00
    "%a, %d %b %Y %H:%M:%S %z",  # Fri, 15 Mar 2024 14:30:00 +0000

    # --- Mixed 12-hour time ---
    "%Y-%m-%d %I:%M %p",         # 2024-03-15 2:30 PM
]


@dataclass
class ParseResult:
    """Result of datetime parsing with logging of failures."""
    parsed_df: pl.DataFrame
    unparseable: pl.DataFrame
    success_count: int
    failure_count: int


def _build_datetime_parsers(
    column: str,
    formats: list[str] | None = None,
) -> list[pl.Expr]:
    """
    Build a list of datetime parsing expressions from format strings.

    Args:
        column: Name of the column to parse
        formats: List of format strings (defaults to DATETIME_FORMATS)

    Returns:
        List of Polars expressions for use with pl.coalesce()
    """
    if formats is None:
        formats = DATETIME_FORMATS

    return [
        pl.col(column).str.to_datetime(
            format=fmt,
            strict=False,
            time_unit="us",
            time_zone="UTC",
        )
        for fmt in formats
    ]


def create_test_dataframe() -> pl.DataFrame:
    """
    Create a test DataFrame with ~50 rows containing various datetime formats.
    All datetimes represent roughly the same time for validation purposes.
    """

    test_data = [
        # ISO 8601 variants (most common)
        "2024-03-15T14:30:00Z",                    # ISO with Z
        "2024-03-15T14:30:00.000Z",                # ISO with ms and Z
        "2024-03-15T14:30:00.123456Z",             # ISO with microseconds
        "2024-03-15T14:30:00+00:00",               # ISO with +00:00
        "2024-03-15T14:30:00-05:00",               # ISO with negative offset
        "2024-03-15T14:30:00+05:30",               # ISO with positive offset (India)
        "2024-03-15T14:30:00.123+00:00",           # ISO ms with offset
        "2024-03-15T09:30:00-05:00",               # Different timezone
        "2024-03-15",                              # Date only ISO
        "2024-03-15T14:30:00",                     # ISO without timezone

        # Space separator variants (very common in databases)
        "2024-03-15 14:30:00",                     # Space instead of T
        "2024-03-15 14:30:00.123",                 # Space with ms
        "2024-03-15 14:30:00.123456",              # Space with microseconds
        "2024-03-15 14:30:00 UTC",                 # Space with UTC text
        "2024-03-15 14:30:00+0000",                # Space with compact offset
        "2024-03-15 14:30:00 +0000",               # Space with spaced offset

        # US format MM/DD/YYYY (common in US data)
        "03/15/2024",                              # US date only
        "03/15/2024 14:30:00",                     # US with time
        "03/15/2024 02:30:00 PM",                  # US with 12-hour time
        "3/15/2024",                               # US without leading zero
        "3/15/2024 2:30 PM",                       # US minimal

        # Alternative separators
        "2024/03/15",                              # ISO-like with slashes
        "2024/03/15 14:30:00",                     # ISO-like slash with time
        "15-03-2024",                              # European with dashes (DD-MM-YYYY)
        "15/03/2024",                              # European with slashes
        "15/03/2024 14:30:00",                     # European with time

        # Compact formats
        "20240315",                                # Basic ISO date
        "20240315143000",                          # Basic ISO datetime
        "20240315T143000Z",                        # Basic ISO with T and Z

        # Text month formats
        "March 15, 2024",                          # US text month
        "March 15, 2024 14:30:00",                 # US text with time
        "15 March 2024",                           # European text month
        "15 March 2024 14:30:00",                  # European text with time
        "Mar 15, 2024",                            # Abbreviated month
        "15 Mar 2024",                             # Abbreviated European
        "15-Mar-2024",                             # Dashed abbreviated
        "15-Mar-2024 14:30:00",                    # Dashed abbreviated with time

        # Unix timestamps
        "1710510600",                              # Unix seconds (will need special handling)
        "1710510600000",                           # Unix milliseconds

        # RFC 2822 / Email format
        "Fri, 15 Mar 2024 14:30:00 +0000",         # RFC 2822
        "Fri, 15 Mar 2024 14:30:00 GMT",           # RFC 2822 with GMT

        # Other formats found in the wild
        "2024.03.15",                              # Dot separator
        "2024.03.15 14:30:00",                     # Dot with time
        "03-15-2024",                              # US with dashes
        "03-15-2024 14:30:00",                     # US dashes with time

        # Edge cases / potentially problematic
        "2024-3-15",                               # ISO without leading zeros
        "2024-03-15T14:30",                        # ISO without seconds
        "2024-03-15 2:30 PM",                      # Mixed format

        # Invalid/malformed for testing error handling
        "not a date",                              # Completely invalid
        "2024-13-45",                              # Invalid month/day
        "",                                        # Empty string
        "N/A",                                     # Common null representation
    ]

    # Add row identifiers
    return pl.DataFrame({
        "row_id": list(range(len(test_data))),
        "datetime_str": test_data,
    })


def parse_datetimes_multi_format(
    df: pl.DataFrame,
    col: str = "datetime_str",
    formats: list[str] | None = None,
) -> ParseResult:
    """
    Parse datetime strings in multiple formats using Polars built-in methods.

    Strategy: Use pl.coalesce() to try multiple format patterns, falling back
    through each until one succeeds. Uses strict=False to return null on failure.

    Args:
        df: DataFrame containing datetime strings
        col: Name of the column containing datetime strings
        formats: Optional list of format strings (defaults to DATETIME_FORMATS)

    Returns:
        ParseResult with parsed DataFrame and logging info
    """
    # Preprocess: normalize text-based timezone names to numeric offsets
    norm_col = "_normalized_dt"
    df = df.with_columns(
        pl.col(col)
        .str.replace(r" UTC$", "+00:00")
        .str.replace(r" GMT$", "+00:00")
        .str.replace(r"Z$", "+00:00")
        .alias(norm_col)
    )

    # Build parsing expressions from format list and coalesce them
    parsers = _build_datetime_parsers(norm_col, formats)
    parsing_expr = pl.coalesce(*parsers).alias("parsed_datetime")

    # Apply parsing and clean up
    result_df = (
        df.with_columns(parsing_expr)
        .drop(norm_col)
        .with_columns(pl.col("parsed_datetime").alias("datetime_utc"))
    )

    # Identify unparseable values
    unparseable_df = result_df.filter(
        pl.col("parsed_datetime").is_null()
    ).select(["row_id", col])

    success_count = result_df.filter(pl.col("parsed_datetime").is_not_null()).height
    failure_count = result_df.filter(pl.col("parsed_datetime").is_null()).height

    return ParseResult(
        parsed_df=result_df,
        unparseable=unparseable_df,
        success_count=success_count,
        failure_count=failure_count,
    )


def parse_with_unix_timestamps(df: pl.DataFrame, col: str = "datetime_str") -> pl.DataFrame:
    """
    Extended parsing that also handles Unix timestamps.

    Unix timestamps require special handling because they're numeric strings
    that need to be converted differently than datetime format strings.
    """

    # First, try to detect and parse Unix timestamps
    # Unix seconds: 10 digits (roughly 1970-2038 range)
    # Unix milliseconds: 13 digits

    # Use str.to_integer with strict=False to safely attempt conversion
    result_df = df.with_columns([
        # Check if it looks like a Unix timestamp and convert
        pl.when(
            pl.col(col).str.contains(r"^\d{10}$")  # Unix seconds
        ).then(
            pl.col(col).str.to_integer(strict=False)
        ).otherwise(None).alias("_unix_s"),

        pl.when(
            pl.col(col).str.contains(r"^\d{13}$")  # Unix milliseconds
        ).then(
            pl.col(col).str.to_integer(strict=False)
        ).otherwise(None).alias("_unix_ms"),
    ])

    # Convert Unix timestamps to datetime
    result_df = result_df.with_columns([
        pl.from_epoch(pl.col("_unix_s"), time_unit="s").alias("_from_unix_s"),
        pl.from_epoch(pl.col("_unix_ms"), time_unit="ms").alias("_from_unix_ms"),
    ])

    # Coalesce Unix parsed values
    result_df = result_df.with_columns(
        pl.coalesce(
            pl.col("_from_unix_s"),
            pl.col("_from_unix_ms"),
        ).alias("unix_parsed")
    ).drop(["_unix_s", "_unix_ms", "_from_unix_s", "_from_unix_ms"])

    return result_df


def comprehensive_parse(df: pl.DataFrame, col: str = "datetime_str") -> ParseResult:
    """
    Comprehensive datetime parsing combining format-based and Unix timestamp parsing.

    This is the main entry point for parsing arbitrary datetime strings.
    """

    # First parse Unix timestamps
    df_with_unix = parse_with_unix_timestamps(df, col)

    # Convert Unix parsed to UTC timezone to match format-parsed datetimes
    df_with_unix = df_with_unix.with_columns(
        pl.when(pl.col("unix_parsed").is_not_null())
        .then(pl.col("unix_parsed").dt.replace_time_zone("UTC"))
        .otherwise(None)
        .alias("unix_parsed")
    )

    # Then parse format-based datetimes
    format_result = parse_datetimes_multi_format(df_with_unix, col)

    # Combine: prefer format-based, fall back to Unix
    # Both are now datetime[μs, UTC] so coalesce will work
    final_df = format_result.parsed_df.with_columns(
        pl.coalesce(
            pl.col("parsed_datetime"),
            pl.col("unix_parsed"),
        ).alias("datetime_utc")
    ).drop(["parsed_datetime", "unix_parsed"])

    # Recalculate unparseable
    unparseable_df = final_df.filter(
        pl.col("datetime_utc").is_null()
    ).select([
        "row_id",
        col,
    ])

    success_count = final_df.filter(pl.col("datetime_utc").is_not_null()).height
    failure_count = final_df.filter(pl.col("datetime_utc").is_null()).height

    return ParseResult(
        parsed_df=final_df,
        unparseable=unparseable_df,
        success_count=success_count,
        failure_count=failure_count,
    )


def log_unparseable(result: ParseResult) -> None:
    """Log unparseable values for debugging and notification."""
    if result.failure_count > 0:
        print("\n" + "=" * 60)
        print("UNPARSEABLE DATETIME VALUES")
        print("=" * 60)
        print(f"Total failures: {result.failure_count}")
        print("\nFailed values:")
        for row in result.unparseable.iter_rows(named=True):
            value = row["datetime_str"]
            if value == "":
                value = "<empty string>"
            print(f"  Row {row['row_id']}: {repr(value)}")
        print("=" * 60)


def main():
    """Main demonstration of datetime parsing."""

    print("=" * 70)
    print("POLARS DATETIME PARSING EXPLORATION")
    print("=" * 70)

    # Create test data
    print("\n1. Creating test DataFrame with various datetime formats...")
    test_df = create_test_dataframe()
    print(f"   Created {test_df.height} test rows")

    # Display sample of test data
    print("\n2. Sample of test data:")
    print(test_df.head(10))

    # Parse datetimes
    print("\n3. Parsing datetimes using comprehensive parser...")
    result = comprehensive_parse(test_df)

    print(f"\n   Parsing Results:")
    print(f"   - Successfully parsed: {result.success_count}")
    print(f"   - Failed to parse: {result.failure_count}")
    print(f"   - Success rate: {result.success_count / test_df.height * 100:.1f}%")

    # Log failures
    log_unparseable(result)

    # Show parsed results
    print("\n4. Parsed DataFrame (showing successful parses):")
    successful = result.parsed_df.filter(pl.col("datetime_utc").is_not_null())
    print(successful.select(["row_id", "datetime_str", "datetime_utc"]).head(20))

    # Show schema
    print("\n5. DataFrame Schema:")
    print(result.parsed_df.schema)

    # Demonstrate that all are now UTC
    print("\n6. Verify UTC timezone on parsed values:")
    sample = successful.select(["datetime_str", "datetime_utc"]).head(5)
    print(sample)

    return result


if __name__ == "__main__":
    result = main()

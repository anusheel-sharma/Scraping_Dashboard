"""
Transformation of data tables using PySpark
=====================================================

Loads parquet tables
Generates column for relevant ratios
Performs data quality checks and removal of outliers
Outputs final two parquet tables to be read by Dashboard
"""

from pyspark.sql import SparkSession, functions as F, DataFrame
from typing import List
from pathlib import Path

# =============================== 1. CONFIG =====================================
FUNDAMENTAL_RATIOS = [
    ("Capex", "Revenue", "Capex/Revenue", True),
    ("OCF", "Revenue", "OCF/Revenue", False),
    ("R&D", "Revenue", "R&D/Revenue", False),
    ("GrossProfit", "Revenue", "GP Margin", False),
    ("OpProfit", "Revenue", "OP Margin", False),
    ("Pretax Income", "Revenue", "Pretax Margin", False)
]

COLUMNS_TO_CAP = [
    "Capex/Revenue %", "OCF/Revenue %",
    "R&D/Revenue %", "GP Margin %", "OP Margin %",
    'Pretax Margin %'
]

# =============================== 2. CORE FUNCTIONS =====================================


def add_ratio_pct(
        df: DataFrame,
        numer: str,
        denom: str,
        out_col: str,
        decimals: int = 2,
        make_positive: bool = False
) -> DataFrame:
    """
    Adds a numer/denom ratio (x 100) column to the relevant df

    Args:
        df (DataFrame): Spark DataFrame
        numer (str): Column name for the numerator
        denom (str): Column name for the denominator
        out_col (str): Column name for the new ratio column
        decimals (int): Number of decimal places for rounding
        make_positive (bool): If True, take the absolute value of numerator (e.g. for Capex which is negative)

    Returns:
        DataFrame: Original DataFrame with additional column
    """

    # Turn numerator positive or keep the same
    num_expr = F.abs(F.col(numer)) if make_positive else F.col(numer)

    # Calculate ratio and x 100, rounded
    ratio = num_expr / F.col(denom)
    ratio_pct = F.round(ratio * 100, decimals)

    return df.withColumn(f"{out_col} %", ratio_pct)

def sanity_filter_fundamentals(df: DataFrame) -> DataFrame:
    """
    Remove rows with impossible values for revenue, capex, GP Margin, OP Margin
    Do not apply margin filter to Pretax margin to allow companies with exceptional events 
    such as from gain of sale of business (e.g. PSA)
    """

    return(
        df
        .fillna({"Capex": 0, "Revenue": -1})
        .filter("Revenue > 0")
        .filter("Capex <= 0")
        .filter("`GP Margin %` IS NULL OR `GP Margin %` < 100")
        .filter("`OP Margin %` IS NULL OR `OP Margin %` < 100")
    )

def winsorise(
        df: DataFrame,
        cols: List[str],
        lower: float = 0.01,
        upper: float = 0.99
) -> DataFrame:
    """
    For each column in cols, add:
    - <col>_capped - numeric, capped at [lower, upper] quantiles
    - <col>_was_capped - 1/0 flag

    Returns new DataFrame
    """

    out = df
    for c in cols:
        # Calculate lower and upper percentiles
        q_lo, q_hi = out.approxQuantile(c, [lower, upper], 0.001)

        out = (
            out.withColumn(
                f"{c}_capped",
                F.when(F.col(c) < q_lo, q_lo)
                .when(F.col(c) > q_hi, q_hi)
                .otherwise(F.col(c))
            )
            .withColumn(
                f"{c}_was_capped",
                F.when(
                    (F.col(c) < q_lo) | (F.col(c) > q_hi),
                    1
                )
                .otherwise(0)
            )
        )

    return out

def sanity_filter_prices(df: DataFrame) -> DataFrame:
    """
    Create two flag columns over price & short sale volume data.
    Check 1 ensure short volume / total volume is less than 1 or that short volume is above 0.
    Check 2 ensures closing price is above 0, and daily high is always above daily low

    Returns DataFrame with two flag colummns - flag_bad_price and flag_bad_short_vol
    """

    df = (
        df
        .withColumn(
            "flag_bad_short_vol",
            (F.col('shortvolume') < 0) | (F.col('ShortVolRatio') > 1)
        )
        .withColumn(
            "flag_bad_price",
            (F.col("Adj Close") <= 0) | (F.col("High") < F.col("Low"))
        )
    )

    return df

def save_spark_to_parquet(
        sdf: DataFrame,
        out_path: str | Path,
        compression: str = "snappy",
        overwrite: bool = True,
        coalesce: int | None = None
) -> None:

    """
    Save *PySpark df* to a Parquet file.

    Parameters
    ----------
    sdf : pyspark.sql.DataFrame
        The table you want to persist.
    out_path : str or Path
        Where to write the file, e.g. 'data/commodities.parquet'.
    compression : {'snappy','zstd','brotli','gzip',None}
        Codec passed to DataFrameWriter.option
    overwrite : bool
        If False and the file exists, raise FileExistsError.
    coalesce: int | None
        If set, call ``sdf.coalesce(coalesce)`` before writing so you control
        how many part‑files land on disk.  Use ``1`` for a single file.
    """

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)  # Creates parent directors if don't exist

    if out_path.exists() and not overwrite:
        raise FileExistsError(f"{out_path} already exists (set overwrite=True to replace)")

    writer = sdf
    if coalesce is not None:
        writer = writer.coalesce(coalesce)

    (
        writer
        .write
        .option("compression", compression)
        .mode("overwrite" if overwrite else "errorifexists")
        .parquet(str(out_path))
    )

    print(f"✅  Saved {sdf.count():,} rows  ➜  {out_path}")

# =============================== 3. DATA PROCESSING =====================================

# Create Spark session
spark = (
    SparkSession
    .builder
    .appName("Commodity+ShortVol ETL")
    .master("local[*]")                 # Use all CPU cores
    .getOrCreate()
)

# Read parquet files for prices, short sale volume and fundamentals
ROOT = Path(__file__).resolve().parents[2]   # .../Scraping Dashboard
raw_path = ROOT / "data" / "raw"
shortvol_df = spark.read.parquet(f"{raw_path}/CNMS_shortvol_2020to2025-07-07.parquet")
prices_df   = spark.read.parquet(f"{raw_path}/equity_commodity_data.parquet")
fundamental_df   = spark.read.parquet(f"{raw_path}/equity_fundamental_data.parquet")

print(f"Short‑vol rows : {shortvol_df.count():,}")
print(f"Price rows : {prices_df.count():,}")
print(f"Fundamentals rows : {fundamental_df.count():,}")

# ============ 3.1 Process Fundamental Data ===============================

# Generate ratios using fundamental specs list
for numer, denom, out, pos in FUNDAMENTAL_RATIOS:
    fundamental_df = add_ratio_pct(fundamental_df, numer, denom, out, make_positive=pos)

# Perform clean up and winsorisation
fundamental_df = sanity_filter_fundamentals(fundamental_df)
fundamental_final = winsorise(fundamental_df, COLUMNS_TO_CAP)
print(f"Fundamental row count: {fundamental_final.count()}")

# ============ 3.2 Process Price and Short Sale Volume Data ===============================

# Normalise short sale volume column names prior to merging
shortvol_df = shortvol_df.withColumnRenamed("symbol", "Ticker")
shortvol_df = shortvol_df.withColumnRenamed("date", "Date")
shortvol_df = shortvol_df.withColumn("Ticker", F.upper("Ticker"))

# Merge price and short sale volume files
prices_short = (
    prices_df.alias("p")
    .join(
        shortvol_df.alias("s"),
        on=["Date", "Ticker"],
        how="left"
    )
    .drop("Close")              # We use adjusted close instead
)

# Calculate short_vol ratio when volume > 0
prices_short = (
    prices_short.withColumn(
        "ShortVolRatio",
        F.when(F.col("totalvolume") > 0, F.col("shortvolume") / F.col("totalvolume"))
        .otherwise(None)
    )
)

# Perform quality checks
prices_short = sanity_filter_prices(prices_short)

# Drop rows where NAs for all YF price columns
prices_short = prices_short.dropna(how='all', subset=['Adj Close', 'High', 'Low', 'Open', 'Volume'])

# Count flagged rows
bad_price_count = prices_short.filter("flag_bad_price = TRUE").count()
bad_short_vol = (
    prices_short.agg(F.sum(F.col("flag_bad_short_vol").cast("int")).alias("true_count"))
        .first()["true_count"]
)

print("rows where flag_bad_short_vol is true:", bad_short_vol)
print("rows where flag_bad_price is true:", bad_price_count)
print(f"Price row count: {prices_short.count()}")


# =============================== 4. EXPORT FILES =====================================
OUT_PATH = ROOT / "data" / "processed"
save_spark_to_parquet(prices_short, OUT_PATH / "prices_processed.parquet")
save_spark_to_parquet(fundamental_final, OUT_PATH / "fundamentals_processed.parquet")
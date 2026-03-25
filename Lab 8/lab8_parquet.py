"""
lab8_parquet.py
BD-1004 | Big Data | NYU Center for Data Science
Lab 8: Parquet — Encoding, Compression, Metadata, and Spark

Run section by section during the walkthrough:
    spark-submit --deploy-mode client lab8_parquet.py

Dataset: transactions.csv — 500,000 rows, 12 columns
Engineered so every Parquet encoding type appears naturally in a real column.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sum as spark_sum, to_timestamp
import os, time

spark = SparkSession.builder \
    .appName("lab8_parquet") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

USER = os.environ["USER"]
BASE = f"hdfs:///user/{USER}/parquet-lab"

print("""
╔══════════════════════════════════════════════════════════╗
║          Lab 8: Parquet — Encoding & Compression         ║
╚══════════════════════════════════════════════════════════╝
""")


# ══════════════════════════════════════════════════════════════
# SECTION 1: Load the CSV and understand what we're working with
# ══════════════════════════════════════════════════════════════
print("─" * 60)
print("SECTION 1: The raw CSV")
print("─" * 60)

df = spark.read.csv(
    f"{BASE}/transactions.csv",
    header=True,
    inferSchema=True
)
df = df.withColumn("event_time", to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))

df.printSchema()
df.show(5, truncate=False)
print(f"Total rows : {df.count():,}")
print(f"Columns    : {len(df.columns)}")

# Show cardinality — this is key to understanding WHY each column
# compresses differently
print("\nCardinality per column (unique value count):")
for c in df.columns:
    n = df.select(c).distinct().count()
    print(f"  {c:<20} {n:>10,} unique values")


# ══════════════════════════════════════════════════════════════
# SECTION 2: How Parquet encodes each column before compression
#
# Parquet does TWO things before writing:
#   1. ENCODING  — structural tricks that shrink the raw bytes
#   2. COMPRESSION — run the encoded bytes through a codec (snappy/gzip/zstd)
#
# Encoding is chosen per-column automatically based on the data.
# ══════════════════════════════════════════════════════════════
print("\n" + "─" * 60)
print("SECTION 2: Parquet encoding types — one column at a time")
print("─" * 60)

print("""
┌─────────────────────┬──────────────────────────────────────────────────────┐
│ Encoding            │ What it does                                         │
├─────────────────────┼──────────────────────────────────────────────────────┤
│ Dictionary          │ Replace repeated strings with a small integer ID.    │
│                     │ Store the dictionary once at the top of the chunk.   │
│                     │ Best for: low-cardinality strings (region, status)   │
├─────────────────────┼──────────────────────────────────────────────────────┤
│ RLE (Run-Length)    │ Instead of storing 0,0,0,0,0 five times, store       │
│                     │ "0 repeated 5 times". Best for: boolean columns,     │
│                     │ skewed categoricals with long runs of the same value │
├─────────────────────┼──────────────────────────────────────────────────────┤
│ Bit-packing         │ If values only range 1–10, you only need 4 bits per  │
│                     │ value instead of 64. Best for: small integer ranges  │
├─────────────────────┼──────────────────────────────────────────────────────┤
│ Delta               │ Store the difference between consecutive values,     │
│                     │ not the values themselves. 1,2,3,4,5 → 1, +1,+1,+1  │
│                     │ Best for: timestamps, sequential IDs                 │
├─────────────────────┼──────────────────────────────────────────────────────┤
│ Plain               │ Raw bytes, no tricks. Used when values are           │
│                     │ high-cardinality floats with no pattern (amount, lat)│
└─────────────────────┴──────────────────────────────────────────────────────┘
""")

print("Now let's see each encoding in our actual data:\n")

# --- Dictionary encoding ---
print("── Dictionary encoding: 'region' column ──")
print("Only 5 unique values across 500,000 rows.")
print("Parquet stores: {'North':0, 'South':1, 'East':2, 'West':3, 'Central':4}")
print("Then writes:    0,0,1,2,0,0,3,1,4,0,... (integers, not strings)")
print("Cost: ~1 byte per row instead of 5-7 bytes per string.\n")
df.groupBy("region").count().orderBy("count", ascending=False).show()

print("── Dictionary encoding: 'status' column ──")
print("3 unique values, heavily skewed toward 'completed'.")
print("Dictionary maps strings to 0,1,2. The skew helps RLE on top.\n")
df.groupBy("status").count().orderBy("count", ascending=False).show()

# --- RLE ---
print("── RLE (Run-Length Encoding): 'is_flagged' column ──")
print("90% of rows are 0. In sorted/chunked storage this becomes long runs:")
print("  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0...")
print("RLE encodes this as: '0 × 450000, 1 × 49999' — nearly nothing.\n")
df.groupBy("is_flagged").count().orderBy("is_flagged").show()

print("── RLE: 'quarter' column ──")
print("Values 1–4, written in time order → long runs: 1,1,1,...,2,2,2,...,3...")
print("RLE encodes each run as a single (value, count) pair.\n")
df.groupBy("quarter").count().orderBy("quarter").show()

# --- Bit-packing ---
print("── Bit-packing: 'quantity' column ──")
print("Values 1–10 only. Max value = 10 → needs only 4 bits (not 64 bits).")
print("500,000 × 64-bit integers = 4 MB raw")
print("500,000 × 4-bit packed    = 250 KB — 16× smaller before any compression.\n")
df.select("quantity").describe().show()

# --- Delta encoding ---
print("── Delta encoding: 'transaction_id' column ──")
print("Sequential: 1, 2, 3, 4, 5, ...")
print("Delta stores: first_value=1, then deltas=[+1, +1, +1, +1, ...]")
print("Constant delta compresses to almost nothing.\n")
df.select("transaction_id").orderBy("transaction_id").show(5)

print("── Delta encoding: 'event_time' column ──")
print("Timestamps spaced ~60s apart.")
print("Delta stores: first_timestamp, then [+60s, +60s, +60s, ...]")
print("Constant delta compresses to almost nothing.\n")
df.select("event_time").orderBy("event_time").show(5)

# --- Plain ---
print("── Plain encoding: 'amount' and 'lat'/'lon' columns ──")
print("High-cardinality floats — no pattern, no repetition.")
print("Parquet falls back to raw bytes. Compression still helps a little.")
print("amount: 183,145 unique values out of 500,000 rows")
print("lat/lon: ~475,000 unique values — nearly every row is different.\n")
df.select("amount", "lat", "lon").describe().show()


# ══════════════════════════════════════════════════════════════
# SECTION 3: Write with different compression codecs, compare sizes
# ══════════════════════════════════════════════════════════════
print("─" * 60)
print("SECTION 3: Compression codecs")
print("─" * 60)

print("""
Encoding shrinks the raw column bytes.
Compression (snappy/gzip/zstd) then compresses those encoded bytes further.
They are independent and compose.

  Raw CSV bytes
      → Encoding (Dictionary / RLE / Delta / Bit-packing)
          → Compression codec (snappy / gzip / zstd / none)
              → bytes on disk
""")

codecs = ["none", "snappy", "gzip", "zstd"]
for codec in codecs:
    df.write \
      .mode("overwrite") \
      .option("compression", codec) \
      .parquet(f"{BASE}/transactions_{codec}.parquet")
    print(f"  Written: transactions_{codec}.parquet")

print("""
Now check the sizes on HDFS:

    hdfs dfs -du -h {BASE}/

You should see something like:
    47.6 MB   transactions.csv
    21.8 MB   transactions_none.parquet    (encoding only, no codec)
    17.9 MB   transactions_snappy.parquet
    12.9 MB   transactions_gzip.parquet
    14.3 MB   transactions_zstd.parquet

Key observations:
  • Even with compression=none, Parquet is less than half the CSV size
    → That's encoding doing the work (Dictionary, Delta, Bit-packing)
  • gzip gets the best ratio; snappy is the fastest to read back
  • zstd is the modern sweet spot: near-gzip ratio, near-snappy speed
""".format(BASE=BASE))


# ══════════════════════════════════════════════════════════════
# SECTION 4: Read the file footer — metadata & column statistics
# ══════════════════════════════════════════════════════════════
print("─" * 60)
print("SECTION 4: Metadata — what's in the file footer")
print("─" * 60)

print("""
Every Parquet file ends with a footer containing:
  - Schema (column names, types, nullability)
  - Row group metadata (how many rows, byte offsets)
  - Column chunk statistics (min, max, null_count per chunk)

The reader loads the footer FIRST — before reading any data —
so it knows exactly what's in the file and what it can skip.
""")

print("Use parquet-tools to read the footer without Spark:")
print("""
    # Pull a part file from HDFS to inspect locally
    hdfs dfs -get \\
      {BASE}/transactions_snappy.parquet/part-00000-*.parquet \\
      sample.parquet

    # Print the schema
    parquet-tools schema sample.parquet

    # Print row group and column chunk metadata
    parquet-tools meta sample.parquet

    # Preview rows
    parquet-tools show --limit 5 sample.parquet
""".format(BASE=BASE))

print("The same stats are available via SparkSQL:")
df_snappy = spark.read.parquet(f"{BASE}/transactions_snappy.parquet")
df_snappy.createOrReplaceTempView("txn")

spark.sql("""
    SELECT
        COUNT(*)                    AS total_rows,
        MIN(transaction_id)         AS first_txn,
        MAX(transaction_id)         AS last_txn,
        ROUND(MIN(amount), 2)       AS min_amount,
        ROUND(MAX(amount), 2)       AS max_amount,
        ROUND(AVG(amount), 2)       AS avg_amount,
        MIN(event_time)             AS earliest,
        MAX(event_time)             AS latest,
        COUNT(DISTINCT region)      AS unique_regions,
        COUNT(DISTINCT status)      AS unique_statuses
    FROM txn
""").show(truncate=False)

print("""
These min/max values are exactly what Parquet stores per column chunk.
When you filter, Spark reads these from the footer — without
decompressing any data — and skips chunks that can't match.
""")


# ══════════════════════════════════════════════════════════════
# SECTION 5: CSV vs Parquet — run the same Spark job on both
# ══════════════════════════════════════════════════════════════
print("─" * 60)
print("SECTION 5: CSV vs Parquet — same query, real difference")
print("─" * 60)

print("Query: total revenue and transaction count by region and category\n")

# On CSV
start = time.time()
result_csv = spark.read.csv(
    f"{BASE}/transactions.csv", header=True, inferSchema=True
).groupBy("region", "category") \
 .agg(
     count("*").alias("num_transactions"),
     spark_sum("amount").alias("total_revenue")
 ).orderBy("region", "category")
result_csv.collect()
t_csv = time.time() - start

# On Parquet (snappy)
start = time.time()
result_parquet = spark.read.parquet(f"{BASE}/transactions_snappy.parquet") \
 .groupBy("region", "category") \
 .agg(
     count("*").alias("num_transactions"),
     spark_sum("amount").alias("total_revenue")
 ).orderBy("region", "category")
result_parquet.collect()
t_parquet = time.time() - start

result_parquet.show(30)

print(f"CSV     : {t_csv:.2f}s  (parse text, infer types, read all bytes)")
print(f"Parquet : {t_parquet:.2f}s  (typed binary, only read region+category+amount chunks)")
print(f"Speedup : {t_csv/t_parquet:.1f}x")

print("""
Why is Parquet faster?
  1. Binary format — no text parsing, no type inference
  2. Column pruning — only 3 of 12 column chunks are read from HDFS
  3. Compressed — fewer bytes transferred from disk
""")


# ══════════════════════════════════════════════════════════════
# SECTION 6: Column pruning — prove Spark skips columns on disk
# ══════════════════════════════════════════════════════════════
print("─" * 60)
print("SECTION 6: Column pruning")
print("─" * 60)

print("Same aggregation, but we explicitly select only the columns we need.")
print("The physical plan will show ReadSchema with 3 columns, not 12.\n")

spark.read.parquet(f"{BASE}/transactions_snappy.parquet") \
     .select("region", "category", "amount") \
     .groupBy("region", "category") \
     .agg(spark_sum("amount").alias("total_revenue")) \
     .explain()

print("""
Look for in the output above:
  ReadSchema: struct<region:string,category:string,amount:double>

NOT the full 12-column schema. The other 9 column chunks
(transaction_id, event_time, status, quantity, is_flagged, etc.)
are never read from HDFS at all.
""")

# Timing comparison
start = time.time()
spark.read.parquet(f"{BASE}/transactions_snappy.parquet") \
     .groupBy("region").agg(avg("amount")).collect()
t_all = time.time() - start

start = time.time()
spark.read.parquet(f"{BASE}/transactions_snappy.parquet") \
     .select("region", "amount") \
     .groupBy("region").agg(avg("amount")).collect()
t_pruned = time.time() - start

print(f"Read all 12 columns : {t_all:.2f}s")
print(f"Read 2 columns only : {t_pruned:.2f}s")
print(f"Speedup             : {t_all/t_pruned:.1f}x\n")


# ══════════════════════════════════════════════════════════════
# SECTION 7: Predicate pushdown — Spark skips entire row groups
# ══════════════════════════════════════════════════════════════
print("─" * 60)
print("SECTION 7: Predicate pushdown")
print("─" * 60)

print("""
The footer stores min/max per column per row group.
If a filter cannot match any value in a row group, Spark skips it entirely.

Example: filter amount > 1900
  Row Group 0: max(amount) = 1999.87 → might have values > 1900 → READ
  Row Group 1: max(amount) = 982.44  → impossible to have > 1900 → SKIP ✓
  Row Group 2: max(amount) = 1954.10 → might have values > 1900 → READ

For this to work, data needs locality — values must be clustered by range.
We'll write one UNSORTED and one SORTED file and compare.
""")

# Write unsorted — amount is random, every row group has max near 2000
df.write.mode("overwrite").option("compression", "snappy") \
  .parquet(f"{BASE}/transactions_unsorted.parquet")

# Write sorted by amount — low values in early row groups, high in late
df.orderBy("amount") \
  .write.mode("overwrite").option("compression", "snappy") \
  .parquet(f"{BASE}/transactions_sorted_amount.parquet")

print("Filtering amount > 1900:\n")

start = time.time()
n = spark.read.parquet(f"{BASE}/transactions_unsorted.parquet") \
         .filter(col("amount") > 1900).count()
t = time.time() - start
print(f"  Unsorted: {n:,} rows matched in {t:.2f}s  (no row groups skipped)")

start = time.time()
n = spark.read.parquet(f"{BASE}/transactions_sorted_amount.parquet") \
         .filter(col("amount") > 1900).count()
t = time.time() - start
print(f"  Sorted  : {n:,} rows matched in {t:.2f}s  (early row groups skipped)")

print("\nPhysical plan (look for PushedFilters):")
spark.read.parquet(f"{BASE}/transactions_sorted_amount.parquet") \
     .filter(col("amount") > 1900) \
     .explain()

print("""
PushedFilters: [IsNotNull(amount), GreaterThan(amount,1900.0)]
means Spark pushed the filter INTO the Parquet reader.
The check happens at the row group level — before any decompression.

Check the Spark History Server after this job:
  https://dataproc.hpc.nyu.edu/sparkhistory/
  → SQL tab → physical plan → PushedFilters
  → Stages tab → fewer tasks for the sorted file = row groups skipped
""")


# ══════════════════════════════════════════════════════════════
# SECTION 8: Partitioned datasets — partition pruning on HDFS
# ══════════════════════════════════════════════════════════════
print("─" * 60)
print("SECTION 8: Partitioned datasets")
print("─" * 60)

print("""
For large datasets you split into a directory tree by column values.
Spark skips entire directories that don't match your filter.

  /user/$USER/parquet-lab/transactions_partitioned/
    region=North/
      category=Electronics/
        part-0.parquet
      category=Clothing/
        part-0.parquet
    region=South/
      ...

A filter on region + category opens exactly ONE directory.
All others are never touched — not even listed.
""")

df.write \
  .mode("overwrite") \
  .option("compression", "snappy") \
  .partitionBy("region", "category") \
  .parquet(f"{BASE}/transactions_partitioned")

print("Written. Explore the layout:")
print(f"  hdfs dfs -ls {BASE}/transactions_partitioned/")
print(f"  hdfs dfs -ls {BASE}/transactions_partitioned/region=North/\n")

df_part = spark.read.parquet(f"{BASE}/transactions_partitioned")

# Read one partition slice
north_elec = df_part.filter(
    (col("region") == "North") & (col("category") == "Electronics")
)
print(f"North + Electronics: {north_elec.count():,} rows")
north_elec.show(5)

print("\nPhysical plan (look for PartitionFilters):")
north_elec.explain()

print("""
PartitionFilters: [(region = North), (category = Electronics)]
means Spark pruned the partition directories without reading any data.
Only region=North/category=Electronics/*.parquet was opened.
""")

# Revenue by region using the partitioned dataset
print("Total revenue by region (uses partition pruning per region):")
df_part.groupBy("region") \
       .agg(spark_sum("amount").alias("total_revenue"),
            count("*").alias("num_transactions")) \
       .orderBy("total_revenue", ascending=False) \
       .show()


# ══════════════════════════════════════════════════════════════
# DONE
# ══════════════════════════════════════════════════════════════
print("─" * 60)
print("Lab 8 complete.")
print("─" * 60)
print(f"""
Files written to HDFS under {BASE}/:
  transactions.csv                 — original (47.6 MB)
  transactions_none.parquet        — Parquet, no codec
  transactions_snappy.parquet      — Parquet + snappy
  transactions_gzip.parquet        — Parquet + gzip
  transactions_zstd.parquet        — Parquet + zstd
  transactions_unsorted.parquet    — for predicate pushdown demo
  transactions_sorted_amount.parquet — sorted, shows row group skipping
  transactions_partitioned/        — Hive-style partitioned dataset

Check sizes:
  hdfs dfs -du -h {BASE}/

Check your job:
  https://dataproc.hpc.nyu.edu/sparkhistory/
""")

spark.stop()

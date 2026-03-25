# Lab 8: Parquet — Encoding, Compression & Spark

**BD-1004 | Big Data | NYU Center for Data Science**

---

## What is Parquet?

CSV stores data **row by row**. Every time you read one column, you pay the cost of reading every other column too.

Parquet stores data **column by column**. If your query only needs `region` and `amount` out of 12 columns, Parquet reads only those two. The other 10 are never touched.

```
CSV (row-oriented):
  ROW 1 → txn_id | region | category | status | amount | quantity | ...
  ROW 2 → txn_id | region | category | status | amount | quantity | ...
  ROW 3 → txn_id | region | category | status | amount | quantity | ...
  (to get amount you must read every field of every row)

Parquet (columnar):
  txn_id  column → 1, 2, 3, 4, 5 ...
  region  column → North, South, North, East ...
  amount  column → 1249.99, 89.50, 34.20 ...   ← read ONLY this
  (every other column chunk is skipped entirely)
```

On top of columnar storage, Parquet applies **encoding** to each column before writing — structural tricks that shrink the bytes based on the shape of the data. Then a **compression codec** (snappy/gzip/zstd) runs on the encoded bytes to shrink further.

```
Raw column bytes
    → Encoding   (Dictionary / RLE / Delta / Bit-packing)
        → Codec  (snappy / gzip / zstd)
            → bytes on disk
```

---

## Encoding Types

Parquet picks the best encoding **per column automatically** based on the data. You never specify it.

### Dictionary Encoding

Replace repeated string values with small integer IDs. Store the mapping once.

```
Raw     :  North | South | North | North | East | South | North
Dict    :  { North:0, South:1, East:2, West:3 }
Encoded :  0     | 1     | 0     | 0     | 2    | 1     | 0

"Electronics" as string = 11 bytes × 5,000 rows = 55,000 bytes
"Electronics" as int ID =  1 byte  × 5,000 rows =  5,000 bytes
```

**Best for:** low-cardinality strings — `region` (4 values), `category` (4), `status` (3)
**Not for:** high-cardinality values where every row is different

**Where you write it** — in your PySpark script:
```python
# Dictionary encoding is automatic — just write to Parquet
# Parquet sees low-cardinality strings and picks Dictionary on its own
df = spark.read.csv("encoding_dictionary.csv", header=True, inferSchema=True)
df.write.mode("overwrite").option("compression", "none").parquet("output/dictionary.parquet")
```

**How to verify it was applied** — on the master node:
```bash
# Pull a part file locally
hdfs dfs -get hdfs:///user/$USER/lab8/parquet/encoding_dictionary.parquet/part-00000-*.parquet sample.parquet

# Inspect encodings
parquet-tools inspect sample.parquet
# Look for:  region → RLE_DICTIONARY
#            amount → PLAIN
```

---

### RLE — Run-Length Encoding

Instead of storing the same value N times, store the pair `(value, count)` once.

```
Raw :  0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0 ...
RLE :  (0, 9), (1, 1), (0, 7) ...

is_fraud column: 95% zeros across 5,000 rows
RLE stores this as: (0, 4750), (1, 250)
→ 2 entries instead of 5,000
```

**Best for:** boolean columns, heavily skewed categoricals — `is_fraud` (95% False), `status` (60% "completed")
**Not for:** random values with no repeated runs

**Where you write it** — in your PySpark script:
```python
# RLE is automatic — Parquet detects repeated values and applies it
df = spark.read.csv("encoding_rle.csv", header=True, inferSchema=True)
df.write.mode("overwrite").option("compression", "none").parquet("output/rle.parquet")
```

**How to verify it was applied** — on the master node:
```bash
hdfs dfs -get hdfs:///user/$USER/lab8/parquet/encoding_rle.parquet/part-00000-*.parquet sample.parquet
parquet-tools inspect sample.parquet
# Look for:  is_fraud → RLE, BIT_PACKED   (near-zero compressed size)
#            amount   → PLAIN             (large compressed size — contrast)
```

---

### Delta Encoding

Store the first value, then store only the **difference** between consecutive values.

```
Sequential IDs:
  Raw  :  1,  2,  3,  4,  5,  6 ...
  Delta:  1, +1, +1, +1, +1, +1 ...
  → constant delta → stored as: first=1, delta=+1 (2 numbers, not 5,000)

Timestamps (1 event per minute):
  Raw  :  09:00, 09:01, 09:02, 09:03 ...
  Delta:  09:00,   +60,   +60,   +60 ...
  → same trick → near-zero storage
```

**Best for:** sequential IDs, ordered timestamps — `transaction_id`, `event_time`
**Not for:** random values where differences between rows are also random

**Where you write it** — in your PySpark script:
```python
# Delta is automatic — Parquet detects sequential integers and timestamps
from pyspark.sql.functions import to_timestamp
df = spark.read.csv("encoding_delta.csv", header=True, inferSchema=True)
df = df.withColumn("event_time", to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))
df.write.mode("overwrite").option("compression", "none").parquet("output/delta.parquet")
```

**How to verify it was applied** — on the master node:
```bash
hdfs dfs -get hdfs:///user/$USER/lab8/parquet/encoding_delta.parquet/part-00000-*.parquet sample.parquet
parquet-tools inspect sample.parquet
# Look for:  event_id   → DELTA_BINARY_PACKED
#            event_time → DELTA_BINARY_PACKED
#            page_views → RLE, BIT_PACKED
```

---

### Bit-Packing

A standard integer takes 64 bits. If values only go up to 10, you only need 4 bits. Parquet packs multiple small values into each 64-bit word.

```
rating column: values 1–5
  Standard int64: 64 bits per value
  3-bit packed  :  3 bits per value → pack 21 ratings into one 64-bit word
  Space saved   : 95% per value

is_returned column: 0 or 1
  1-bit packing → 64 values per 64-bit word → 98% space saved
```

**Best for:** integer columns with a small known range — `rating` (1–5), `weekday` (1–7), `quantity` (1–10)
**Not for:** large integers or floating point numbers

**Where you write it** — in your PySpark script:
```python
# Bit-packing is automatic — Parquet detects small integer ranges
df = spark.read.csv("encoding_bitpack.csv", header=True, inferSchema=True)
df.write.mode("overwrite").option("compression", "none").parquet("output/bitpack.parquet")
```

**How to verify it was applied** — on the master node:
```bash
hdfs dfs -get hdfs:///user/$USER/lab8/parquet/encoding_bitpack.parquet/part-00000-*.parquet sample.parquet
parquet-tools inspect sample.parquet
# Look for:  rating      → RLE, BIT_PACKED
#            is_returned → RLE, BIT_PACKED   (1-bit, smallest possible)
#            weekday     → RLE, BIT_PACKED
```

---

### Plain Encoding

No encoding. Raw bytes written as-is. Parquet falls back to Plain when no encoding can help.

```
lat = 40.712843, 34.051729, -12.043817, 51.509865 ...
→ Every value is different
→ Dictionary? No. RLE? No. Delta? No. Bit-pack? No.
→ Plain: just write the bytes.
```

The compression codec still runs on top, but without structural encoding first, gains are minimal.

**Best for:** high-cardinality floats — `lat`, `lon`, `amount`, sensor readings
**This is the contrast** — in step5 you will see Parquet barely shrinks this data at all vs 3–4x for structured columns.

**Where you write it** — in your PySpark script:
```python
# Plain is the fallback — nothing special to write
df = spark.read.csv("encoding_plain.csv", header=True, inferSchema=True)
df.write.mode("overwrite").option("compression", "none").parquet("output/plain.parquet")
```

**How to verify it was applied** — on the master node:
```bash
hdfs dfs -get hdfs:///user/$USER/lab8/parquet/encoding_plain.parquet/part-00000-*.parquet sample.parquet
parquet-tools inspect sample.parquet
# Look for:  lat, lon, temperature → PLAIN
# Notice:    compression space_saved is near 0% — codec barely helped either
```

---

## Compression Codecs

After encoding, Parquet runs a codec on each column chunk independently.

| Codec | Write speed | Read speed | Ratio | Best for |
|---|---|---|---|---|
| **none** | Fastest | Fastest | 1× | See encoding alone, debug |
| **snappy** | Fast | Fast | ~2–3× | Hot data, frequent queries |
| **gzip** | Slow | Medium | ~3–5× | Cold storage, size matters |
| **zstd** | Medium | Fast | ~3–5× | Best balance, modern default |

**Where you write it** — in your PySpark script:
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

spark = SparkSession.builder.appName("convert").getOrCreate()

# Read the CSV
df = spark.read.csv("hdfs:///user/$USER/lab8/data/transactions.csv",
                    header=True, inferSchema=True)
df = df.withColumn("event_time", to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))

# Write as Parquet — pick your codec
df.write.mode("overwrite").option("compression", "snappy") \
  .parquet("hdfs:///user/$USER/lab8/parquet/transactions_snappy.parquet")

df.write.mode("overwrite").option("compression", "gzip") \
  .parquet("hdfs:///user/$USER/lab8/parquet/transactions_gzip.parquet")

df.write.mode("overwrite").option("compression", "zstd") \
  .parquet("hdfs:///user/$USER/lab8/parquet/transactions_zstd.parquet")

df.write.mode("overwrite").option("compression", "none") \
  .parquet("hdfs:///user/$USER/lab8/parquet/transactions_none.parquet")
```

**Submit it:**
```bash
spark-submit --deploy-mode client step7_to_parquet.py
```

**Check sizes on HDFS after:**
```bash
hdfs dfs -du -h hdfs:///user/$USER/lab8/data/transactions.csv
hdfs dfs -du -h hdfs:///user/$USER/lab8/parquet/transactions_none.parquet/
hdfs dfs -du -h hdfs:///user/$USER/lab8/parquet/transactions_snappy.parquet/
hdfs dfs -du -h hdfs:///user/$USER/lab8/parquet/transactions_gzip.parquet/
hdfs dfs -du -h hdfs:///user/$USER/lab8/parquet/transactions_zstd.parquet/
```

Expected:
```
46.5 MB   transactions.csv            ← original
21.8 MB   transactions_none           ← encoding only, no codec
17.9 MB   transactions_snappy
12.9 MB   transactions_gzip
14.3 MB   transactions_zstd
```

---

## Setup

```bash
# 1. SSH into Dataproc
ssh $USER@dataproc.hpc.nyu.edu

# 2. Clone the repo
git clone https://github.com/Akkey01/lab8-parquet.git
cd lab8-parquet

# 3. Upload all data to HDFS
hdfs dfs -mkdir -p /user/$USER/lab8/data
hdfs dfs -put data/ /user/$USER/lab8/

# 4. Confirm
hdfs dfs -ls /user/$USER/lab8/data/

# 5. Install parquet-tools (used to inspect file footers)
pip3 install parquet-tools --user
```

---

## Running the lab

```bash
# Part 1 — Encoding types (before & after for each)
spark-submit --deploy-mode client step1_dictionary.py
spark-submit --deploy-mode client step2_rle.py
spark-submit --deploy-mode client step3_delta.py
spark-submit --deploy-mode client step4_bitpack.py
spark-submit --deploy-mode client step5_plain.py

# Part 2 — The main act: CSV vs Parquet on 500k rows
spark-submit --deploy-mode client step6_csv_job.py       # ← write down the time
spark-submit --deploy-mode client step7_to_parquet.py    # ← convert + check sizes
spark-submit --deploy-mode client step8_parquet_job.py   # ← same job, compare
```

After each encoding step (1–5), run these two commands to see before/after sizes:
```bash
hdfs dfs -du -h hdfs:///user/$USER/lab8/data/<encoding_name>.csv
hdfs dfs -du -h hdfs:///user/$USER/lab8/parquet/<encoding_name>.parquet/
```

After step7, see all codec sizes at once:
```bash
hdfs dfs -du -h hdfs:///user/$USER/lab8/parquet/
```

---

## Files

```
data/
  encoding_dictionary.csv    5,000 rows   region(4), category(4), payment(3)
  encoding_rle.csv           5,000 rows   is_fraud(95% False), status(skewed)
  encoding_delta.csv         5,000 rows   sequential IDs, evenly spaced timestamps
  encoding_bitpack.csv       5,000 rows   rating(1–5), items(1–10), weekday(1–7)
  encoding_plain.csv         5,000 rows   GPS coords, sensor readings (all unique floats)
  transactions.csv         500,000 rows   main dataset — all encoding types present

step1_dictionary.py     Before & after: low-cardinality strings
step2_rle.py            Before & after: skewed booleans and categoricals
step3_delta.py          Before & after: sequential IDs and timestamps
step4_bitpack.py        Before & after: small integer ranges
step5_plain.py          Before & after: high-cardinality floats (the contrast)
step6_csv_job.py        Spark job on transactions.csv — write down the time
step7_to_parquet.py     Convert to Parquet, all 4 codecs, hdfs du -h
step8_parquet_job.py    Same Spark job on Parquet — compare
```

---

## What to look for after each step

| Step | Run this after | What to see |
|---|---|---|
| step1 | `hdfs dfs -du -h .../encoding_dictionary*` | CSV ~177KB → Parquet ~65KB (0.37x) |
| step2 | `hdfs dfs -du -h .../encoding_rle*` | CSV ~151KB → Parquet ~63KB (0.42x) |
| step3 | `hdfs dfs -du -h .../encoding_delta*` | CSV ~197KB → Parquet ~52KB (0.26x) |
| step4 | `hdfs dfs -du -h .../encoding_bitpack*` | CSV ~78KB → Parquet ~40KB (0.51x) |
| step5 | `hdfs dfs -du -h .../encoding_plain*` | CSV ~254KB → Parquet ~249KB **(0.98x — barely anything)** |
| step6 | Write down the CSV time | Baseline for comparison |
| step7 | `hdfs dfs -du -h /user/$USER/lab8/` | All 4 codecs side by side |
| step8 | Compare to step6 time | `ReadSchema` shows 3 cols not 12 |

---

## Encoding quick reference

| Encoding | Best for | Example columns |
|---|---|---|
| Dictionary | Low-cardinality strings | region, category, status |
| RLE | Skewed booleans, repeated values | is_fraud (95% False), status |
| Delta | Sequential IDs, ordered timestamps | transaction_id, event_time |
| Bit-packing | Small integer ranges | rating (1–5), weekday (1–7) |
| Plain | High-cardinality floats | lat, lon, amount, sensor data |

---

## Most common mistake

```bash
# WRONG
spark-submit client step6_csv_job.py

# CORRECT
spark-submit --deploy-mode client step6_csv_job.py
```

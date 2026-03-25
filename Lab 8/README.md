# Lab 8: Parquet — Encoding, Compression, Metadata & Spark

**BD-1004 | Big Data | NYU Center for Data Science**

---

## Table of Contents
1. [Why Parquet?](#1-why-parquet)
2. [Parquet's Physical Layout](#2-parquets-physical-layout)
3. [Encoding Types](#3-encoding-types)
4. [Compression Codecs](#4-compression-codecs)
5. [Metadata & Statistics](#5-metadata--statistics)
6. [Predicate Pushdown](#6-predicate-pushdown)
7. [Partitioned Datasets](#7-partitioned-datasets)
8. [Setup & Running the Lab](#8-setup--running-the-lab)
9. [Common Mistakes & Tips](#9-common-mistakes--tips)

---

## 1. Why Parquet?

When you work with large datasets, your bottleneck is almost never CPU — it's **I/O**: how fast you can read bytes off disk or over the network. Parquet is a file format engineered specifically to minimize that I/O.

| Property | What it means |
|---|---|
| **Columnar** | Stores all values of a column together, not row-by-row |
| **Encoded** | Each column uses the best structural encoding for its data type |
| **Compressed** | Each column chunk is compressed independently |
| **Self-describing** | Schema and statistics are embedded in the file footer |
| **Splittable** | Row groups can be read in parallel across executors |

**The result in this lab:** A 47.6 MB CSV shrinks to 12.9 MB Parquet with gzip — without losing a single byte of data.

---

## 2. Parquet's Physical Layout

```
┌─────────────────────────────────────────────────────────────┐
│  Magic bytes: PAR1                                          │
├──────────────────────────┬──────────────────────────────────┤
│  Row Group 0             │  Column chunk: transaction_id   │
│  (rows 0 – ~122k)        │  Column chunk: event_time       │
│                          │  Column chunk: region           │
│                          │  Column chunk: amount           │
│                          │  Column chunk: is_flagged       │
│                          │  ... (12 columns total)         │
├──────────────────────────┼──────────────────────────────────┤
│  Row Group 1             │  Column chunk: transaction_id   │
│  (rows ~122k – ~244k)    │  ...                            │
├──────────────────────────┴──────────────────────────────────┤
│  File Footer                                                │
│    - Schema (column names, types, nullability)              │
│    - Row group offsets and byte sizes                       │
│    - Column statistics: min, max, null_count per chunk      │
├─────────────────────────────────────────────────────────────┤
│  Footer length (4 bytes) + Magic bytes: PAR1                │
└─────────────────────────────────────────────────────────────┘
```

| Term | Definition |
|---|---|
| **Row Group** | A horizontal slice of the table; default ~128 MB. Each is independent and can be read in parallel. |
| **Column Chunk** | One column's data within one row group. The unit of encoding and compression. |
| **Page** | Smallest unit inside a column chunk (~1 MB). Can be data, dictionary, or index pages. |
| **Footer** | Metadata at the end of the file. Always read first — tells the reader exactly what's inside and what it can skip. |

---

## 3. Encoding Types

Encoding is applied **per column, before compression**. Parquet picks the best encoding automatically based on the column's data.

### Dictionary Encoding

**How it works:** Replace repeated string values with small integer IDs. Store the string→integer mapping (the dictionary) once at the top of the column chunk.

```
Raw:        North | South | North | North | East | South | Central | North
Dictionary: {North:0, South:1, East:2, Central:3, West:4}
Encoded:    0     | 1     | 0     | 0     | 2    | 1     | 3       | 0
```

**Best for:** Low-cardinality string columns.

**In our dataset:** `region` (5 values), `category` (6), `status` (3), `payment_type` (4).

**Why it compresses well:** `"Electronics"` is 11 bytes. After dictionary encoding it's 1 byte (the integer ID 0). A 91% reduction before any codec runs.

---

### RLE — Run-Length Encoding

**How it works:** Instead of storing a repeated value N times, store the pair (value, count).

```
Raw:     0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0 ...
RLE:     (0, 9), (1, 1), (0, 6) ...
```

**Best for:** Columns with long runs of the same value — booleans, skewed categoricals, time-ordered low-cardinality columns.

**In our dataset:**
- `is_flagged` — 90% zeros. RLE encodes this as essentially one entry: `(0, 450001)`.
- `quarter` — ordered timestamps mean long runs of `1,1,1,...,2,2,2,...` as the year progresses.
- `status` — 60% "completed" with local clustering.

---

### Bit-packing

**How it works:** Use only as many bits as the maximum value requires, not the full 64 bits of an integer.

```
quantity values: 1 through 10
Max value = 10 → needs ceil(log2(10)) = 4 bits per value

64-bit integer: 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0111  (7)
4-bit packed:                                                                            0111  (7)

Space saved: 60 out of 64 bits = 93.75% per value
```

**Best for:** Integer columns with a small known range.

**In our dataset:** `quantity` (1–10), `is_flagged` (0–1), `quarter` (1–4).

> **Note:** RLE and bit-packing are actually combined in Parquet into a single hybrid encoding called **RLE/Bit-packing**. Values below a threshold are bit-packed in groups; long runs are RLE-encoded. The encoder switches between modes dynamically.

---

### Delta Encoding

**How it works:** Store the first value, then store only the *differences* between consecutive values.

```
transaction_id: 1,    2,    3,    4,    5,    6, ...
Deltas:         1,   +1,   +1,   +1,   +1,   +1, ...
→ Store: first=1, delta=+1 (constant → compresses to 2 numbers total)

event_time:    2022-01-01 00:01:00,  00:02:00,  00:03:00, ...
Deltas:        base_timestamp,        +60s,      +60s, ...
→ Constant delta → nearly zero storage
```

**Best for:** Sequential IDs, monotonically increasing timestamps, anything where consecutive differences are small or constant.

**In our dataset:** `transaction_id` (1 to 500,000 sequential), `event_time` (one event per minute in order).

---

### Plain Encoding

**How it works:** Raw bytes, no tricks. Parquet falls back to this when the column has no exploitable pattern.

**Best for:** High-cardinality floats that are essentially random.

**In our dataset:** `amount` (183k unique values), `lat` (472k unique values), `lon` (475k unique values).

Plain encoding still benefits from the compression codec (snappy/gzip/zstd) applied on top, but the gains are modest compared to columns with structure.

---

### Column encoding summary for our dataset

| Column | Encoding | Why |
|---|---|---|
| `transaction_id` | Delta | Sequential integers — constant delta |
| `event_time` | Delta | Ordered timestamps — constant 60s delta |
| `region` | Dictionary | 5 unique values across 500k rows |
| `category` | Dictionary | 6 unique values |
| `status` | Dictionary + RLE | 3 values, 60% "completed" |
| `payment_type` | Dictionary | 4 unique values |
| `amount` | Plain | 183k unique floats — no pattern |
| `quantity` | Bit-packing | Values 1–10, needs only 4 bits |
| `is_flagged` | RLE | 90% zeros — extremely long runs |
| `quarter` | RLE | Ordered data → long runs of 1,1,1,...,2,2,2,... |
| `lat` / `lon` | Plain | ~475k unique floats — no pattern |

---

## 4. Compression Codecs

After encoding, Parquet applies a compression codec to each column chunk independently.

| Codec | Write speed | Read speed | Compression ratio | Best for |
|---|---|---|---|---|
| **none** | Fastest | Fastest | 1× | Debugging; already-compressed data |
| **snappy** | Fast | Fast | ~2–3× | Default for most pipelines |
| **gzip** | Slow | Medium | ~3–5× | Cold storage; size matters more than speed |
| **zstd** | Medium | Fast | ~3–5× | Modern default; near-gzip ratio, near-snappy speed |
| **lz4** | Fastest | Fastest | ~1.5–2× | Real-time / streaming pipelines |

### What you'll see in this lab

```
CSV                          47.6 MB   (baseline)
Parquet, none                21.8 MB   (encoding only — no codec)
Parquet, snappy              17.9 MB
Parquet, gzip                12.9 MB
Parquet, zstd                14.3 MB
```

**Key takeaway:** Even with `compression=none`, Parquet is less than half the CSV size. That's encoding doing the work — not the codec. The codec squeezes further on top.

---

## 5. Metadata & Statistics

Every Parquet file ends with a **footer** containing:

```
Footer contents:
  Schema
    - column: transaction_id  INT64
    - column: event_time      TIMESTAMP_MICROS
    - column: region          BYTE_ARRAY (String, Dictionary)
    - column: amount          DOUBLE
    - ...

  Row Group 0  (rows 0–122,879)
    Column chunk: transaction_id
      compression      : SNAPPY
      encodings        : DELTA_BINARY_PACKED, RLE
      compressed_size  : 2,273,849 bytes
      uncompressed_size: 4,272,637 bytes
      statistics:
        min: 1
        max: 122880
        null_count: 0

    Column chunk: region
      compression      : SNAPPY
      encodings        : PLAIN_DICTIONARY, RLE, BIT_PACKED
      compressed_size  : 189,960 bytes     ← tiny! only 5 unique values
      uncompressed_size: 189,834 bytes
      statistics:
        min: "Central"
        max: "West"
        null_count: 0

    Column chunk: lat
      compression      : SNAPPY
      encodings        : PLAIN
      compressed_size  : 4,261,343 bytes   ← large, ~475k unique floats
      uncompressed_size: 4,261,016 bytes
      ...
```

### What the footer tells us

1. **Which encoding was actually used** — you can verify that `region` got `PLAIN_DICTIONARY` and `transaction_id` got `DELTA_BINARY_PACKED`
2. **Per-column compressed vs uncompressed sizes** — shows exactly how much each encoding helped
3. **Min / max per column per row group** — this is what enables predicate pushdown

### Reading the footer with parquet-tools

```bash
# Pull a part file locally
hdfs dfs -get \
  /user/$USER/parquet-lab/transactions_snappy.parquet/part-00000-*.parquet \
  sample.parquet

parquet-tools schema sample.parquet   # schema only
parquet-tools meta   sample.parquet   # full footer: row groups, stats, encodings
parquet-tools show   sample.parquet --limit 5  # preview rows
```

---

## 6. Predicate Pushdown

When you filter in Spark, the check can happen at three different levels depending on the file format:

```
CSV:      Read all bytes → parse every row → apply filter in Spark memory
Parquet:  Read footer → skip row groups → decompress remaining → apply filter
```

### How row group skipping works

The footer stores `min` and `max` per column per row group. Before decompressing anything:

```
Filter: amount > 1900

Row Group 0: max(amount) = 1999.87  → could contain values > 1900 → READ
Row Group 1: max(amount) =  982.44  → impossible → SKIP ✓
Row Group 2: max(amount) = 1954.10  → could contain values > 1900 → READ
Row Group 3: max(amount) =  745.22  → impossible → SKIP ✓
```

**This only works when data has locality.** If `amount` values are random across all row groups, every group's max will be near 2000 — nothing can be skipped. Sort by `amount` before writing and row groups at the low end get completely skipped.

### Verifying in Spark

```python
df.filter(col("amount") > 1900).explain()
```

Look for:
```
PushedFilters: [IsNotNull(amount), GreaterThan(amount,1900.0)]
```
This confirms Spark has pushed the filter into the Parquet reader — the check happens at the row group level, before any bytes are decompressed.

---

## 7. Partitioned Datasets

For very large datasets, split into a **directory tree** partitioned by column values:

```
/user/$USER/parquet-lab/transactions_partitioned/
  region=North/
    category=Electronics/  ← only this directory is opened for North+Electronics
      part-0.parquet
    category=Clothing/
      part-0.parquet
  region=South/
    category=Electronics/
      part-0.parquet
    ...
```

When Spark reads with `filter("region = 'North' AND category = 'Electronics'")`, it skips all other directories without opening them. This is **partition pruning** — the coarser-grained version of predicate pushdown.

### Partition column guidelines

| Rule | Reason |
|---|---|
| Partition on low-cardinality columns | `region` (5 values) → 5 directories. `customer_id` (500k values) → 500k tiny files → kills NameNode. |
| Partition on your most common filter columns | If you always filter by date, partition by `year`/`month` |
| Aim for files ≥ 128 MB per partition | Too many small files hurts HDFS NameNode memory |

### Verifying partition pruning in Spark

```python
df_part.filter((col("region") == "North") & (col("category") == "Electronics")) \
       .explain()
```

Look for:
```
PartitionFilters: [isnotnull(region#...), (region#... = North), ...]
```

---

## 8. Setup & Running the Lab

```bash
# 1. SSH into Dataproc master node
ssh $USER@dataproc.hpc.nyu.edu

# 2. Clone the lab repo
git clone https://github.com/<your-repo>/lab8-parquet.git
cd lab8-parquet

# 3. Upload data to HDFS
hdfs dfs -mkdir -p /user/$USER/parquet-lab
hdfs dfs -put transactions.csv  /user/$USER/parquet-lab/
hdfs dfs -ls /user/$USER/parquet-lab/

# 4. Run the lab
spark-submit --deploy-mode client lab8_parquet.py
```

After each section completes, check file sizes:
```bash
hdfs dfs -du -h /user/$USER/parquet-lab/
```

Check the job in Spark History Server:
```
https://dataproc.hpc.nyu.edu/sparkhistory/
→ SQL tab    → physical plan → PushedFilters, PartitionFilters, ReadSchema
→ Stages tab → fewer tasks on filtered/partitioned reads = skipping in action
```

---

## 9. Common Mistakes & Tips

### Mistake 1: Missing `--deploy-mode` flag
```bash
# WRONG
spark-submit client lab8_parquet.py

# CORRECT
spark-submit --deploy-mode client lab8_parquet.py
```

### Mistake 2: Reading all columns when you only need a few
```python
# WRONG — reads all 12 column chunks from HDFS
df.groupBy("region").agg(avg("amount")).show()

# CORRECT — only 2 column chunks read from HDFS
df.select("region", "amount").groupBy("region").agg(avg("amount")).show()
```

### Mistake 3: Expecting predicate pushdown to help on random data
Pushdown only skips row groups when data has locality. If `amount` is random across all row groups, every group's max is ~2000 and nothing is skipped. Sort before writing if you plan to filter on that column.

### Mistake 4: Partitioning on a high-cardinality column
```python
# DANGEROUS — creates one directory per customer = millions of tiny files
df.write.partitionBy("customer_id").parquet(...)

# CORRECT
df.write.partitionBy("region", "category").parquet(...)
```

### Mistake 5: Calling `.collect()` on a large result
```python
# WRONG — pulls all data to the driver
rows = df.collect()

# CORRECT — preview with show(), save results back to HDFS
df.show(20)
df.write.parquet(f"{BASE}/results/")
```

### Tip: Always verify with `.explain()`
Before running an expensive job, call `.explain()` and confirm:
- `ReadSchema` lists only the columns you need (column pruning is working)
- `PushedFilters` is present (predicate pushdown is working)
- `PartitionFilters` is present (partition pruning is working)

### Tip: Check encodings with parquet-tools
```bash
parquet-tools meta sample.parquet
# encodings: PLAIN_DICTIONARY, RLE, BIT_PACKED  ← for region (good)
# encodings: PLAIN                              ← for lat/lon (expected)
# encodings: DELTA_BINARY_PACKED                ← for transaction_id (good)
```

---

*Lab 8 — NYU Center for Data Science | Parquet*

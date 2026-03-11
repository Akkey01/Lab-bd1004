# Lab 7: Column-Oriented Storage & Parquet

**NYU Center for Data Science**  
⏱ 50-minute lab | You will need your NYU HPC access and the files from Lab 6

---

## Before You Start

You should already have these files on HDFS from Lab 6. Confirm they are there:

```bash
hdfs dfs -ls /user/$USER/
```

You should see:
```
boats.txt
sailors.json
reserves.json
```

If any are missing, re-upload them:

```bash
hdfs dfs -put boats.txt sailors.json reserves.json /user/$USER/
```

---

## Part 1 — The Problem with JSON and CSV (5 min)

In Lab 6 you ran queries like this:

```python
sailors = spark.read.json(f"hdfs:/user/{userID}/sailors.json")
sailors.select("sid", "rating").filter(col("rating") > 7).show()
```

This works — but Spark is doing more work than it needs to.

When Spark reads `sailors.json`, it reads **every column of every row** off disk, even though your query only needs `sid` and `rating`. With 4 columns that is not a big deal. With 100 columns and 1 billion rows, you are reading ~98× more data than necessary.

**The question:** what if the file format could let Spark read only `sid` and `rating`, and skip the rest entirely?

That is what **Parquet** does.

---

## Part 2 — Row Storage vs Column Storage (10 min)

Right now your `sailors.json` is stored like this on disk — **row by row**:

```
{"sid": 22, "sname": "Dustin",  "rating": 7,  "age": 45.0}
{"sid": 29, "sname": "Brutus",  "rating": 1,  "age": 33.0}
{"sid": 31, "sname": "Lubber",  "rating": 8,  "age": 55.5}
...
```

To get just `rating`, Spark must parse every line and throw away `sid`, `sname`, and `age`.

**Parquet stores the same data column by column:**

```
sid:    [22, 29, 31, 32, 58, 64, 71, 74, 85, 95]
sname:  [Dustin, Brutus, Lubber, Andy, Rusty, ...]
rating: [7, 1, 8, 8, 10, 7, 10, 9, 3, 3]
age:    [45.0, 33.0, 55.5, 25.5, 35.0, ...]
```

Now to get just `rating`, Spark reads **only the rating column** — `sid`, `sname`, and `age` are never touched.

### ✅ Try It — See the difference with `.explain()`

Create `lab7_parquet.py` on HPC:

```bash
nano lab7_parquet.py
```

Paste this in:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

spark = SparkSession.builder.appName("lab7").getOrCreate()
userID = os.environ["USER"]

# Read your Lab 6 files
sailors  = spark.read.json(f"hdfs:/user/{userID}/sailors.json")
reserves = spark.read.json(f"hdfs:/user/{userID}/reserves.json")
boats    = spark.read.csv(f"hdfs:/user/{userID}/boats.txt",
                          schema="bid INT, bname STRING, color STRING")

# See what Spark actually reads from the JSON file
print("=== JSON query plan ===")
sailors.select("sid", "rating").filter(col("rating") > 7).explain()
```

Save and run:

```bash
# Ctrl+O → Enter → Ctrl+X  to save in nano
spark-submit --deploy-mode client lab7_parquet.py
```

Look at the output. You will see Spark reads **all columns** from the JSON file. Keep this open — you will compare it to Parquet in Part 4.

---

## Part 3 — Convert Your Files to Parquet (10 min)

Add the following to your `lab7_parquet.py` (below what you already have):

```python
# ── Convert JSON/CSV → Parquet ──────────────────────────────────────────────
sailors.write.mode("overwrite").parquet(f"hdfs:/user/{userID}/sailors_parquet")
reserves.write.mode("overwrite").parquet(f"hdfs:/user/{userID}/reserves_parquet")
boats.write.mode("overwrite").parquet(f"hdfs:/user/{userID}/boats_parquet")

print("✅ Parquet files written!")

# ── Read back from Parquet ───────────────────────────────────────────────────
# Notice: no inferSchema needed — schema is stored inside the Parquet file
sailors_p  = spark.read.parquet(f"hdfs:/user/{userID}/sailors_parquet")
reserves_p = spark.read.parquet(f"hdfs:/user/{userID}/reserves_parquet")
boats_p    = spark.read.parquet(f"hdfs:/user/{userID}/boats_parquet")

# Verify the data looks the same
print("=== sailors_p schema ===")
sailors_p.printSchema()
sailors_p.show()
```

Run it:

```bash
spark-submit --deploy-mode client lab7_parquet.py
```

Check the files were created on HDFS:

```bash
hdfs dfs -ls /user/$USER/sailors_parquet/
```

You will see a folder, not a single file:

```
_SUCCESS
part-00000-xxxx.snappy.parquet
part-00001-xxxx.snappy.parquet
```

Spark splits data into **partitions** and writes one file per partition. The `.snappy` in the filename tells you Spark applied **Snappy compression** automatically.

Now compare file sizes — JSON vs Parquet:

```bash
hdfs dfs -du -h /user/$USER/sailors.json
hdfs dfs -du -h /user/$USER/sailors_parquet/
```

> **What you will actually see:**
> ```
> sailors.json       →   582 bytes
> sailors_parquet/   →   1.5 KB
> ```
> Parquet is **bigger** here — and that is completely expected. Do not be surprised.

**Why?** Parquet stores extra metadata in the file footer: the schema, column statistics, min/max values per row group, encoding info. On 10 rows this overhead dominates. The file is mostly metadata, not data.

The compression advantages of Parquet only kick in at scale:

| Dataset size | JSON | Parquet |
|---|---|---|
| 10 rows (sailors) | 582 B | 1.5 KB — **Parquet is bigger** |
| 1M rows | large, uncompressed | 3–10× smaller |
| 1B rows, 100 columns | reads all columns always | reads only columns needed, skips row groups |

The footer overhead is fixed — it does not grow with the number of rows. At millions of rows the actual data dwarfs the footer and Parquet wins decisively.

---

## Part 4 — Inspect the Query Plans (10 min)

The full script you already submitted includes the `.explain()` calls. Look at the output from your run and find these sections:

### What you will see

Both plans will look similar on the sailors dataset — 10 rows and 4 columns is too small to show a real difference. **That is expected and honest.** Here is what to look for anyway:

In the **Parquet plan**, find these lines:

```
Format: Parquet
ReadSchema: struct<age:double,rating:bigint,sid:bigint,sname:string>
```

In the **JSON plan**, find:

```
Format: JSON
ReadSchema: struct<age:double,rating:bigint,sid:bigint,sname:string>
```

They look the same here — and that is the point. **Spark is smart enough to prune columns from JSON too when you explicitly `.select()` specific columns.** The difference becomes dramatic only at scale:

| Dataset size | JSON | Parquet |
|---|---|---|
| 10 rows, 4 columns (sailors) | Fast — no visible difference | Fast |
| 1B rows, 100 columns | Reads all 100 columns always | Reads only the columns your query needs |
| Large data, `WHERE category = 'X'` | Scans every row | Skips entire row groups via footer statistics |

### What the real advantages are on this dataset

The gains you **can** actually see right now are:

**1. No `inferSchema` needed**
```python
# JSON — Spark scans the whole file to guess types
sailors = spark.read.json(...) # inferSchema happens automatically, costs time

# Parquet — schema is stored inside the file, read instantly
sailors_p = spark.read.parquet(...)
sailors_p.printSchema()  # types are already known, no scan needed
```

**2. Smaller file size** — you already measured this with `hdfs dfs -du -h`

**3. The workflow is the same** — your Lab 6 queries run unchanged on Parquet. At scale, the speedup is automatic with zero code changes.

---

## Part 5 — Lab 6 Queries on Parquet (10 min)

The full script already ran all three Lab 6 queries on Parquet. Look at the output from your run and find these sections:

```
=== High-rating sailors ===
=== Reservations per high-rating sailor ===
=== Boats with below-average renter age ===
```

> **Question:** Do the results match what you got in Lab 6?  
> They should — Parquet is just a different way to store the same data. The query results are identical. What changes is how much Spark reads from disk.

<details>
<summary>▶ Answer</summary>

Yes, results are identical. The only difference is that Spark now reads far less data to produce them:
- For the first query Spark reads only `sid`, `sname`, `rating` — not `age`
- For the join queries Spark reads only the columns referenced in the query
- Filters are pushed into the file reader before data enters Spark at all

</details>

---

## Part 6 — Why Columns Compress Better (5 min)

Parquet is smaller because storing one column at a time enables compression tricks that don't work on mixed rows.

| Scheme | When it applies | Example |
|---|---|---|
| **Dictionary** | Few distinct string values | `sname`: Dustin, Brutus... → integer IDs |
| **RLE** | Long runs of the same value | `rating`: 7,7,7,8,8 → (7,3),(8,2) |
| **Delta** | Values close to each other | timestamps: 1000,1001,1002 → 1000,+1,+1 |
| **Bit-packing** | Small integers | ratings 1–10 fit in 4 bits, not 64 |

These can be combined. Your `sailors_parquet` likely uses dictionary + Snappy on `sname`, and bit-packing on `rating` and `sid`.

Check what Parquet knows about each column:

```bash
# List the parquet file to get its name
hdfs dfs -ls /user/$USER/sailors_parquet/

# Copy it locally and inspect with Python
hdfs dfs -get /user/$USER/sailors_parquet/part-00000-*.snappy.parquet sailors_sample.parquet
python3 -c "
import pyarrow.parquet as pq
f = pq.read_metadata('sailors_sample.parquet')
print(f.row_group(0).column(0))
"
```

---

## Full Script

Here is the complete `lab7_parquet.py` for reference:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

spark = SparkSession.builder.appName("lab7").getOrCreate()
userID = os.environ["USER"]

# Read Lab 6 files
sailors  = spark.read.json(f"hdfs:/user/{userID}/sailors.json")
reserves = spark.read.json(f"hdfs:/user/{userID}/reserves.json")
boats    = spark.read.csv(f"hdfs:/user/{userID}/boats.txt",
                          schema="bid INT, bname STRING, color STRING")

# Convert to Parquet
sailors.write.mode("overwrite").parquet(f"hdfs:/user/{userID}/sailors_parquet")
reserves.write.mode("overwrite").parquet(f"hdfs:/user/{userID}/reserves_parquet")
boats.write.mode("overwrite").parquet(f"hdfs:/user/{userID}/boats_parquet")
print("✅ Parquet files written!")

# Read back
sailors_p  = spark.read.parquet(f"hdfs:/user/{userID}/sailors_parquet")
reserves_p = spark.read.parquet(f"hdfs:/user/{userID}/reserves_parquet")
boats_p    = spark.read.parquet(f"hdfs:/user/{userID}/boats_parquet")

# Inspect schema
sailors_p.printSchema()
sailors_p.show()

# Compare query plans
print("=== JSON plan ===")
sailors.select("sid", "rating").filter(col("rating") > 7).explain()
print("=== Parquet plan ===")
sailors_p.select("sid", "rating").filter(col("rating") > 7).explain()

# Register as temp views
sailors_p.createOrReplaceTempView("sailors")
reserves_p.createOrReplaceTempView("reserves")
boats_p.createOrReplaceTempView("boats")

# Lab 6 queries — now running on Parquet
spark.sql("SELECT sid, sname, rating FROM sailors WHERE rating > 7").show()

spark.sql("""
    SELECT s.sid, s.sname, COUNT(r.bid) AS num_reserves
    FROM   sailors s JOIN reserves r ON s.sid = r.sid
    WHERE  s.rating > 7
    GROUP  BY s.sid, s.sname
""").show()

spark.sql("""
    SELECT bname, color, AVG(sailors.age) AS avg_renter_age
    FROM   reserves
    JOIN   sailors ON sailors.sid = reserves.sid
    JOIN   boats   ON boats.bid   = reserves.bid
    GROUP  BY bname, color
    HAVING AVG(sailors.age) < (
        SELECT AVG(age) FROM sailors
        JOIN reserves ON reserves.sid = sailors.sid
    )
""").show()
```

Submit:

```bash
spark-submit --deploy-mode client lab7_parquet.py
```

Monitor your job:

```
https://dataproc.hpc.nyu.edu/sparkhistory/
```

---

## Key Takeaways

| What you did | Why it matters |
|---|---|
| Converted JSON → Parquet | One-line change: `.write.parquet(...)` |
| Read Parquet back | No `inferSchema` needed — schema is stored in the file |
| Compared `.explain()` output | Saw `ReadSchema` and `PushedFilters` — proof Spark reads less |
| Compared file sizes on HDFS | Parquet is smaller due to column-level compression |
| Re-ran Lab 6 queries | Results identical — only storage format changed |

> **The main idea:** same data, same queries, same results — but Spark reads a fraction of the data. At scale this is the difference between a query taking 2 minutes and 2 hours.

---

*Lab 7 — NYU Center for Data Science | Column-Oriented Storage & Parquet*  
*Builds on: [Lab 6 — Apache Spark](./lab6_spark.md)*

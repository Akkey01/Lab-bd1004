# Lab 5: Apache Spark — Theory, Concepts & Practical Session

> **How to use this file:**  
> - **Sections 1–7** = theory and concepts from the lecture slides. Read before or during the session.  
> - **Sections 8–10** = step-by-step practical you run live on NYU Dataproc.  
> - **Section 11** = quick reference card to keep open while coding.

---

## Table of Contents

**Theory**
1. [Why Spark?](#1-why-spark)
2. [Core Data Structure: The RDD](#2-core-data-structure-the-rdd)
3. [Transformations vs. Actions](#3-transformations-vs-actions)
4. [The Spark Ecosystem](#4-the-spark-ecosystem)
5. [DataFrames & SparkSQL](#5-dataframes--sparksql)
6. [Narrow vs. Wide Dependencies](#6-narrow-vs-wide-dependencies)

**Practical**

7. [Environment Setup on Dataproc](#7-environment-setup-on-dataproc)
8. [Run the Starter Code](#8-run-the-starter-code)
9. [Exercises](#9-exercises)

**Reference**

10. [Quick Reference & Common Errors](#10-quick-reference--common-errors)

---

# THEORY

---

## 1. Why Spark?

Apache Spark is a unified computing engine for large-scale distributed data processing. It superseded Hadoop MapReduce for most workloads because of five core properties:

| Property | What it means |
|---|---|
| **Speed** | In-memory computation — up to 100× faster than MapReduce for iterative jobs |
| **Ease of Use** | High-level APIs in Python, Scala, Java, R, and SQL |
| **Generality** | Batch processing, streaming, ML, and graph computation — all in one engine |
| **Flexibility of computing** | Adapts to many different workload types without switching tools |
| **Scales up and down** | Runs identically on your laptop (local mode) or a 10,000-node cluster |

### Why is Spark faster than MapReduce?

MapReduce forces every intermediate result to be written to disk before the next stage can begin. This means a 10-stage pipeline causes 10 full disk writes and 10 full disk reads. Spark keeps intermediate results **in memory** and pipelines stages together, so the same 10-stage pipeline may touch disk only once. For iterative algorithms like gradient descent or k-means, which pass over the same data dozens of times, this difference is enormous.

---

## 2. Core Data Structure: The RDD

An **RDD (Resilient Distributed Dataset)** is the foundational abstraction in Spark. Every higher-level construct (DataFrames, Datasets, SparkSQL) is built on top of RDDs.

### The three properties

- **Resilient** — Fault-tolerant via *lineage*. Every RDD knows exactly how it was created from its parent data. If a node fails and a partition is lost, Spark walks back the lineage graph and recomputes only the lost partition — no need to restart the whole job.
- **Distributed** — The data is automatically split into partitions spread across all nodes in the cluster. Operations run in parallel on each partition.
- **Dataset** — A collection of records. Records can be any Python/Scala/Java objects.

### What can RDDs do that MapReduce cannot?

1. **Multi-step in-memory pipelines** — No forced disk write between stages. Narrow transformations (see Section 6) are fused and pipelined within a single stage.
2. **Caching / persistence** — An RDD can be explicitly cached in memory with `.cache()`. Iterative algorithms that reuse the same data benefit enormously from this.
3. **Arbitrary DAG execution** — MapReduce only supports map → shuffle → reduce. Spark supports any directed acyclic graph of operations — joins before filters, multiple aggregations, branching pipelines, etc.
4. **Interactive queries** — The Spark shell returns results directly to the driver without writing files to disk first.

### RDD Lineage Graph

Each RDD records its parent and the transformation that produced it. This chain is called the **lineage graph** (or DAG — directed acyclic graph).

```
boats.txt ──read──▶ RDD[raw rows] ──map──▶ RDD[tuples] ──filter──▶ RDD[filtered]
```

On node failure: Spark re-reads `boats.txt` and replays the map and filter — only for the lost partition. Everything else remains intact.

### Why RDDs matter especially for Machine Learning

ML training algorithms are **iterative** — they pass over the same dataset many times (one pass per epoch or iteration). With MapReduce, each pass requires a full disk read and a full disk write. With Spark, after the dataset is cached on the first pass it lives in RAM, and subsequent passes are ~100× faster. This single property made Spark the dominant platform for distributed ML.

---

## 3. Transformations vs. Actions

This is the most important design principle in Spark. Understanding it explains why Spark is fast and how to write efficient jobs.

### Transformations — lazy, produce a new RDD/DataFrame

When you call a transformation, **nothing happens yet**. Spark records the operation in the lineage graph as a plan but defers execution.

| Transformation | What it does |
|---|---|
| `select(...)` | Keep only specified columns |
| `filter(...)` | Keep rows matching a condition |
| `groupBy(...)` | Group rows by one or more keys |
| `orderBy(...)` | Sort rows |
| `distinct()` | Remove duplicate rows |
| `limit(n)` | Take first n rows (still lazy!) |
| `join(other, on)` | Join two DataFrames on a key |
| `agg(...)` | Compute aggregates within groups |

### Actions — eager, trigger execution, return a non-RDD value

When you call an action, Spark takes the full lineage graph built up so far, optimizes it, and executes it. The result is a concrete value (a number, a list, a printed table, a saved file) — not another RDD.

| Action | What it does |
|---|---|
| `show(n)` | Print first n rows to console |
| `count()` | Return the number of rows as an integer |
| `collect()` | Pull all rows to the driver as a Python list |
| `take(n)` | Pull first n rows to the driver |
| `save(...)` / `write` | Write results to HDFS or other storage |

### Lazy evaluation in practice

```python
df       = spark.read.csv("data.csv")         # lazy — no data loaded yet
filtered = df.filter(df.age > 30)             # lazy — just adds to the plan
grouped  = filtered.groupBy("city").count()   # lazy — still just a plan

grouped.show()   # ACTION — Spark now executes everything in one optimized pass
```

### Why laziness is powerful

Because Spark defers execution, its **Catalyst optimizer** gets to see the entire plan before running a single line. It can:
- Push `filter` operations before `join` operations to reduce the amount of data being shuffled
- Fuse multiple `select` and `filter` steps into a single pass over the data
- Choose the most efficient physical join strategy (hash join vs. sort-merge join vs. broadcast join)

If Spark executed eagerly like Pandas, none of these optimizations would be possible.

---

## 4. The Spark Ecosystem

Spark is not a single tool — it is a layered ecosystem of components all built on the same core engine:

```
┌──────────────────────────────────────────────────────────┐
│  Spark SQL       │  Streaming   │  MLlib     │  GraphX   │
│  + DataFrames    │              │  (ML)      │  (Graphs) │
├──────────────────────────────────────────────────────────┤
│                     Spark Core API                       │
├─────────┬─────────┬──────────┬──────────┬────────────────┤
│    R    │   SQL   │  Python  │  Scala   │  Java          │
└─────────┴─────────┴──────────┴──────────┴────────────────┘
```

- **Spark Core** — The foundation. Handles scheduling, memory management, fault recovery, and the RDD API.
- **Spark SQL + DataFrames** — Structured data processing. Lets you query data with SQL or a DataFrame API. Uses the Catalyst optimizer to produce efficient execution plans. This is what we use in this lab.
- **Streaming** — Processes live data as it arrives, using micro-batches or a continuous processing model.
- **MLlib** — Distributed machine learning library. Includes algorithms for classification, regression, clustering, recommendation, and more — all designed to run across a cluster.
- **GraphX** — Graph-parallel computation. Used for algorithms like PageRank and connected components on large graphs.

All four higher-level components compile down to RDD operations in Spark Core, so they benefit from the same in-memory execution, caching, and fault tolerance.

---

## 5. DataFrames & SparkSQL

A **DataFrame** is a distributed table with named, typed columns. Think of it as a Pandas DataFrame except the data is split across all the nodes in your cluster and operations run in parallel. DataFrames are built on top of RDDs but expose a much cleaner API for structured data.

### The SparkSession — your entry point

Every Spark program starts by creating a `SparkSession`. This is the object you use to read data, run SQL, and configure your job.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()
```

### Reading data into DataFrames

```python
# CSV — must specify schema because CSV has no type information
boats = spark.read.csv("boats.txt", schema="bid INT, bname STRING, color STRING")

# JSON — schema is inferred automatically from the file
sailors  = spark.read.json("sailors.json")
reserves = spark.read.json("reserves.json")
```

### Inspecting a DataFrame

```python
boats.printSchema()   # prints column names and types
boats.show()          # prints first 20 rows
boats.count()         # returns number of rows (this is an action)
```

### Two equivalent interfaces: SparkSQL and the DataFrame API

For every query you can use either **SparkSQL** (SQL strings) or the **DataFrame transformation API** (chained Python method calls). Both produce identical execution plans — choose whichever is more readable for the task.

To use SparkSQL, you first register the DataFrame as a named temporary view:

```python
sailors.createOrReplaceTempView("sailors")
# Now "sailors" can be referenced in SQL strings
spark.sql("SELECT * FROM sailors").show()
```

#### Example A — Filter and project

```python
# SparkSQL
spark.sql("""
    SELECT sid, sname, rating
    FROM   sailors
    WHERE  rating > 7
""").show()

# DataFrame API — identical result
from pyspark.sql.functions import col

sailors.filter(col("rating") > 7) \
       .select("sid", "sname", "rating") \
       .show()
```

#### Example B — Join and aggregate

```python
# SparkSQL
spark.sql("""
    SELECT   s.sid, s.sname, COUNT(r.bid) AS num_reserves
    FROM     sailors s
    JOIN     reserves r ON s.sid = r.sid
    WHERE    s.rating > 7
    GROUP BY s.sid, s.sname
""").show()

# DataFrame API
from pyspark.sql.functions import count

sailors.join(reserves, "sid") \
       .filter(col("rating") > 7) \
       .groupBy("sid", "sname") \
       .agg(count("bid").alias("num_reserves")) \
       .show()
```

#### Example C — Subquery (HAVING below average)

This is the most complex pattern: filtering groups by a condition that compares against a value computed from the whole dataset.

```python
from pyspark.sql.functions import avg

# SparkSQL — use a subquery inside HAVING
spark.sql("""
    SELECT   bname, color, AVG(age) AS average_renter_age
    FROM     reserves
    JOIN     sailors ON sailors.sid = reserves.sid
    JOIN     boats   ON boats.bid   = reserves.bid
    GROUP BY bname, color
    HAVING   AVG(sailors.age) < (
        SELECT AVG(age)
        FROM   sailors
        JOIN   reserves ON reserves.sid = sailors.sid
    )
""").show()

# DataFrame API — compute the scalar average separately
joined       = reserves.join(sailors, "sid").cache()
avg_per_boat = joined.groupBy("bid").agg(avg("age").alias("avg_age"))
avg_overall  = joined.select(avg("age")).collect()[0][0]  # extract scalar

below_avg = avg_per_boat.filter(avg_per_boat.avg_age < avg_overall)

below_avg.join(boats, "bid") \
         .select(boats.bname, boats.color, below_avg.avg_age.alias("average_renter_age")) \
         .show()
```

> **Why `.cache()` here?** The DataFrame `joined` is referenced twice — once to compute `avg_per_boat` and once to compute `avg_overall`. Without `.cache()`, Spark re-executes the full join both times. With `.cache()`, the join runs once and the result is stored in memory for the second use.

---

## 6. Narrow vs. Wide Dependencies

Every transformation in Spark creates a dependency between the input RDD and the output RDD. These dependencies fall into two categories, and the distinction is critical for performance.

### Narrow dependencies

Each output partition depends on **exactly one** input partition. No data needs to move between nodes. These transformations can be pipelined together in a single stage with no network communication.

Examples: `filter`, `select`, `map`, `flatMap`, `limit`

```
Partition 1 ──filter──▶ Partition 1'
Partition 2 ──filter──▶ Partition 2'
Partition 3 ──filter──▶ Partition 3'
```

### Wide dependencies (shuffle)

Each output partition may depend on **multiple** input partitions — potentially from different nodes. Spark must collect all records with the same key from across the cluster and send them to the same node. This network movement is called a **shuffle** and is expensive.

Examples: `groupBy`, `join`, `orderBy`, `distinct`

```
Partition 1 ──╮
Partition 2 ──┼──groupBy──▶ Partition A (all "city=NYC" records)
Partition 3 ──╯
```

### Why this matters

- Wide dependencies create **stage boundaries** in Spark's execution plan. All tasks in a stage must complete before the next stage can begin.
- Shuffles write data to disk and send it over the network — the two slowest operations in a distributed system.
- You can minimize shuffle cost by filtering data before grouping or joining, reducing the amount of data that needs to move.

| Operation | Dependency type |
|---|---|
| `filter(...)` | Narrow |
| `select(...)` | Narrow |
| `map(...)` | Narrow |
| `groupBy(...).agg(...)` | **Wide** |
| `join(...)` | **Wide** (usually) |
| `orderBy(...)` | **Wide** |
| `distinct()` | **Wide** |

---



# PRACTICAL SESSION

---

## 7. Environment Setup on Dataproc

### What's in this repo

```
lab5-spark/
├── README.md  (this file)
├── data/
│   ├── boats.txt
│   ├── sailors.json
│   └── reserves.json
├── lab_5_examples.py        ← starter / reference code
├── lab_5_spark_example.py   ← full reference solution
└── lab5_solutions.py        ← YOU fill this in today
```

### Step 1 — SSH into Dataproc

```bash
ssh YOUR_NETID@dataproc.hpc.nyu.edu
```

### Step 2 — Clone this repo onto Dataproc

```bash
git clone https://github.com/Akkey01/lab-6-bd1004.git
cd lab-6-bd1004
```

### Step 3 — Confirm the data files are present

```bash
ls data/
```

Expected output:
```
boats.txt  reserves.json  sailors.json
```

If the `data/` folder is missing or empty, stop and ask your TA.

### Step 4 — Upload data files to HDFS

Spark on Dataproc reads from **HDFS**, not from your local login-node filesystem. You must upload the files before running any Spark job.

```bash
# Create your HDFS user directory (only needed once)
hdfs dfs -mkdir -p /user/$USER

# Upload the three data files
hdfs dfs -put data/boats.txt     /user/$USER/
hdfs dfs -put data/sailors.json  /user/$USER/
hdfs dfs -put data/reserves.json /user/$USER/

# Verify the upload
hdfs dfs -ls /user/$USER/
```

Expected output:
```
Found 3 items
-rw-r--r--   ...  /user/YOUR_NETID/boats.txt
-rw-r--r--   ...  /user/YOUR_NETID/reserves.json
-rw-r--r--   ...  /user/YOUR_NETID/sailors.json
```

> **If you skip this step**, every Spark job will fail with `FileNotFoundException`. This is the most common mistake in this lab.

---

## 8. Run the Starter Code

### Step 5 — Read through `lab_5_examples.py`

```bash
cat lab_5_examples.py
```

Before running it, identify in the code:
- Where is the `SparkSession` created?
- Which lines are transformations (lazy)?
- Which lines are actions (trigger execution)?
- Where are the temp views registered?

### Step 6 — Submit the job

```bash
# CORRECT — always include --deploy-mode client
spark-submit --deploy-mode client lab_5_examples.py
```

> ⚠️ **The most common mistake in this lab:**
> ```bash
> # WRONG — "client" is treated as the script filename, not a flag
> spark-submit client lab_5_examples.py
> ```
> This throws a cryptic `SparkException: Failed to get main class in JAR` error. Always include `--deploy-mode`.

### Step 7 — Verify the output

You should see two result tables printed — one from SparkSQL and one from the DataFrame API. They should be identical. If you see errors, check that Step 4 (HDFS upload) completed successfully.

---

## 9. Exercises

### Setup — create `lab5_solutions.py`

```bash
nano lab5_solutions.py
```

Paste this boilerplate:

```python
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg

def main(spark, userID):
    # Load data from HDFS
    boats    = spark.read.csv(f'hdfs:/user/{userID}/boats.txt',
                              schema='bid INT, bname STRING, color STRING')
    sailors  = spark.read.json(f'hdfs:/user/{userID}/sailors.json')
    reserves = spark.read.json(f'hdfs:/user/{userID}/reserves.json')

    # Register temp views for SQL
    boats.createOrReplaceTempView('boats')
    sailors.createOrReplaceTempView('sailors')
    reserves.createOrReplaceTempView('reserves')

    print("\n── Exercise 1 ───────────────────────────────────────────")
    # YOUR CODE HERE

    print("\n── Exercise 2 ───────────────────────────────────────────")
    # YOUR CODE HERE

    print("\n── Exercise 3 ───────────────────────────────────────────")
    # YOUR CODE HERE


if __name__ == "__main__":
    spark  = SparkSession.builder.appName('lab5_solutions').getOrCreate()
    userID = os.environ['USER']
    main(spark, userID)
```

Save: `Ctrl+X` → `Y` → `Enter`

---

### Exercise 1 — Filter and project

**Task:** Find all sailors with a rating greater than 7. Display their `sid`, `sname`, and `rating`.

Implement using **both** SparkSQL and the DataFrame API. They should produce identical output.

```python
# SparkSQL version
ex1_sql = spark.sql("""
    SELECT ___
    FROM   sailors
    WHERE  ___ > ___
""")
ex1_sql.show()

# DataFrame API version
ex1_df = sailors.filter(___) \
                .select(___)
ex1_df.show()
```

**Expected output:**
```
+---+-------+------+
|sid|  sname|rating|
+---+-------+------+
| 31| Lubber|     8|
| 32|   Andy|     8|
| 58|  Rusty|    10|
| 71|  Zorba|    10|
| 74|Horatio|     9|
+---+-------+------+
```

<details>
<summary>Solution — only open after attempting</summary>

```python
# SparkSQL
ex1_sql = spark.sql("""
    SELECT sid, sname, rating
    FROM   sailors
    WHERE  rating > 7
""")
ex1_sql.show()

# DataFrame API
ex1_df = sailors.filter(col("rating") > 7) \
                .select("sid", "sname", "rating")
ex1_df.show()
```
</details>

---

### Exercise 2 — Join + aggregation

**Task:** For each sailor with rating > 7, count how many boat reservations they have made. Show `sid`, `sname`, and `num_reserves`.

Note that sailors with a high rating but zero reservations should **not** appear — think about why.

```python
# SparkSQL version
ex2_sql = spark.sql("""
    SELECT   s.sid, s.sname, COUNT(___) AS num_reserves
    FROM     sailors s
    JOIN     reserves r ON ___
    WHERE    ___
    GROUP BY ___
""")
ex2_sql.show()

# DataFrame API version
ex2_df = sailors.join(reserves, ___) \
                .filter(___) \
                .groupBy(___) \
                .agg(count(___).alias("num_reserves"))
ex2_df.show()
```

**Expected output:**
```
+---+-------+------------+
|sid|  sname|num_reserves|
+---+-------+------------+
| 31| Lubber|           3|
| 74|Horatio|           1|
+---+-------+------------+
```

> **Why do Andy, Rusty, and Zorba not appear?** They have rating > 7 but have never made a reservation. An `INNER JOIN` on `reserves` drops them because there is no matching row.

<details>
<summary>Solution — only open after attempting</summary>

```python
# SparkSQL
ex2_sql = spark.sql("""
    SELECT   s.sid, s.sname, COUNT(r.bid) AS num_reserves
    FROM     sailors s
    JOIN     reserves r ON s.sid = r.sid
    WHERE    s.rating > 7
    GROUP BY s.sid, s.sname
""")
ex2_sql.show()

# DataFrame API
ex2_df = sailors.join(reserves, "sid") \
                .filter(col("rating") > 7) \
                .groupBy("sid", "sname") \
                .agg(count("bid").alias("num_reserves"))
ex2_df.show()
```
</details>

---

### Exercise 3 — Subquery / correlated average

**Task:** Find the name and color of every boat whose average renter age is **below** the overall average age of all renters. Show `bname`, `color`, and `average_renter_age`.

This is the hardest exercise. Break it into steps before writing code:

1. Join `reserves` + `sailors` to get one row per reservation with the renter's age
2. Compute the **average age per boat** — group by `bid`, aggregate with `avg("age")`
3. Compute the **overall average age** — a single number across all reservations
4. Filter step 2 to keep only boats where `avg_age < overall_avg`
5. Join with `boats` to get `bname` and `color`

```python
# SparkSQL version — use a subquery inside HAVING
ex3_sql = spark.sql("""
    SELECT   bname, color, AVG(age) AS average_renter_age
    FROM     reserves
    JOIN     sailors ON ___
    JOIN     boats   ON ___
    GROUP BY ___
    HAVING   AVG(sailors.age) < (
        SELECT ___
        FROM   sailors JOIN reserves ON ___
    )
""")
ex3_sql.show()

# DataFrame API version
joined       = reserves.join(sailors, ___).cache()   # .cache() because we use this twice
avg_per_boat = joined.groupBy("bid").agg(avg("age").alias("avg_age"))
avg_overall  = joined.select(avg("age")).collect()[0][0]   # extracts the scalar number

below_avg    = avg_per_boat.filter(___)

ex3_df = below_avg.join(boats, ___) \
                  .select(___, ___, below_avg.avg_age.alias("average_renter_age"))
ex3_df.show()
```

**Expected output:**
```
+---------+-----+------------------+
|    bname|color|average_renter_age|
+---------+-----+------------------+
|Interlake| blue|              40.0|
+---------+-----+------------------+
```

> The overall average renter age across all reservations is **45.15**. Only the blue Interlake (rented only by Dustin, age 45 — wait, rented by sid 22 and 64 whose ages average to 40.0) falls below this. All other boats have average renter ages above 45.15.

<details>
<summary>Solution — only open after attempting</summary>

```python
# SparkSQL
ex3_sql = spark.sql("""
    SELECT   bname, color, AVG(age) AS average_renter_age
    FROM     reserves
    JOIN     sailors ON sailors.sid = reserves.sid
    JOIN     boats   ON boats.bid   = reserves.bid
    GROUP BY bname, color
    HAVING   AVG(sailors.age) < (
        SELECT AVG(age)
        FROM   sailors
        JOIN   reserves ON reserves.sid = sailors.sid
    )
""")
ex3_sql.show()

# DataFrame API
joined       = reserves.join(sailors, reserves.sid == sailors.sid).cache()
avg_per_boat = joined.groupBy("bid").agg(avg("age").alias("avg_age"))
avg_overall  = joined.select(avg("age")).collect()[0][0]

below_avg    = avg_per_boat.filter(avg_per_boat.avg_age < avg_overall)

ex3_df = below_avg.join(boats, boats.bid == below_avg.bid) \
                  .select(boats.bname,
                          boats.color,
                          below_avg.avg_age.alias("average_renter_age"))
ex3_df.show()
```
</details>

---

### Step — Run all solutions

```bash
spark-submit --deploy-mode client lab5_solutions.py
```

All three exercises should print without errors.

### Step — Inspect the query plan (bonus)

Add this line after Exercise 2 and re-run:

```python
ex2_df.explain()
```

Look for `Exchange` in the output — that is a shuffle (wide dependency). Look for `Filter` appearing above `Join` — that is Catalyst pushing the filter earlier to reduce the amount of data being shuffled.

### Step — View the DAG in Spark History Server

Open in your browser: `https://dataproc.hpc.nyu.edu/sparkhistory/`

1. Search for your NYU NetID in the search box
2. Click your most recent job
3. Click a Stage → DAG Visualization
4. Find nodes labelled **Exchange** — those are shuffles (wide dependencies)
5. Find stages where multiple operations are fused — those are pipelined narrow dependencies

### Step — Push to GitHub

```bash
git add lab5_solutions.py
git commit -m "completed lab5 exercises 1-3"
git push origin main
```

### Submission checklist

- [ ] `lab5_solutions.py` runs on Dataproc with no errors
- [ ] Exercise 1 output matches expected
- [ ] Exercise 2 output matches expected
- [ ] Exercise 3 output matches expected
- [ ] Pushed to GitHub

---

# REFERENCE

---

## 10. Quick Reference & Common Errors

### Submit a Spark job
```bash
spark-submit --deploy-mode client YOUR_SCRIPT.py
```

### HDFS commands
```bash
hdfs dfs -put local_file /user/$USER/     # upload
hdfs dfs -ls  /user/$USER/                # list
hdfs dfs -get /user/$USER/file ./         # download
hdfs dfs -rm  /user/$USER/file            # delete
```

### Key code patterns

```python
# Start a session
spark = SparkSession.builder.appName("name").getOrCreate()

# Read data
df = spark.read.json(f'hdfs:/user/{userID}/file.json')
df = spark.read.csv('file.txt', schema='col1 INT, col2 STRING')

# Register for SQL then query
df.createOrReplaceTempView("my_table")
spark.sql("SELECT ... FROM my_table WHERE ...").show()

# DataFrame API chain
from pyspark.sql.functions import col, count, avg
df.filter(col("rating") > 7) \
  .groupBy("sid") \
  .agg(count("bid").alias("num_reserves")) \
  .show()

# Extract a single scalar from a DataFrame
scalar = df.select(avg("col")).collect()[0][0]

# Cache a DataFrame you'll reference more than once
df = df.cache()

# Inspect the execution plan
df.explain()
```

### Common errors

| Error message | Cause | Fix |
|---|---|---|
| `SparkException: Failed to get main class in JAR` | Missing `--deploy-mode` flag | `spark-submit --deploy-mode client script.py` |
| `FileNotFoundException: hdfs:/user/...` | Data not in HDFS | `hdfs dfs -put data/* /user/$USER/` |
| `AnalysisException: Table or view not found` | Temp view not registered | Call `df.createOrReplaceTempView("name")` before SQL |
| Driver OOM / job hangs | `.collect()` on a large DataFrame | Use `.show(20)` to preview; only `.collect()` for scalars |
| Slow job on repeated operations | No caching on reused DataFrame | Add `.cache()` to any DataFrame used multiple times |

---

*Lab 5 — NYU Center for Data Science | Apache Spark*

# Lab 5: Apache Spark — Complete Guide & Practical

---

## Table of Contents
1. [Why Spark?](#1-why-spark)
2. [Core Data Structure: The RDD](#2-core-data-structure-the-rdd)
3. [Transformations vs. Actions](#3-transformations-vs-actions)
4. [The Spark Ecosystem](#4-the-spark-ecosystem)
5. [DataFrames & SparkSQL](#5-dataframes--sparksql)
6. [Running Spark on Dataproc](#6-running-spark-on-dataproc)
7. [Practical Exercises](#8-practical-exercises)
8. [Common Mistakes & Tips](#9-common-mistakes--tips)

---

## 1. Why Spark?

Apache Spark is a unified computing engine for large-scale distributed data processing. It superseded Hadoop MapReduce for most workloads due to:

| Property | What it means |
|---|---|
| **Speed** | In-memory computation; up to 100× faster than MapReduce for iterative jobs |
| **Ease of Use** | High-level APIs in Python, Scala, Java, R, SQL |
| **Generality** | Batch, streaming, ML, graph — all in one engine |
| **Flexibility** | Runs locally on a laptop or on a 10,000-node cluster |

**Key insight:** MapReduce writes intermediate results to disk between every stage. Spark keeps them in memory and pipelines stages together, which is critical for iterative algorithms like gradient descent.

---

## 2. Core Data Structure: The RDD

An **RDD (Resilient Distributed Dataset)** is the foundational abstraction in Spark.

### Properties

- **Resilient** — Fault-tolerant via *lineage*: if a partition is lost, Spark re-derives it from the parent RDDs
- **Distributed** — Partitioned across multiple nodes in the cluster
- **Dataset** — A collection of records (any Python/Scala/Java objects)

### What can RDDs do that MapReduce can't?

1. **Multi-step in-memory pipelines** — No forced disk write between stages
2. **Caching / persistence** — Cache an RDD so iterative algorithms reuse it without recomputation
3. **Arbitrary DAG execution** — Not just map → reduce; any directed acyclic graph of operations
4. **Interactive queries** — Results come back to the driver without writing files

### RDD Lineage Graph

Each RDD knows how it was derived from its parents. This is the *lineage graph* (DAG). On node failure, Spark walks back the lineage and recomputes only the lost partitions — no full restart needed.

```
boats.txt  ──read──▶  RDD[raw lines]  ──map──▶  RDD[tuples]  ──filter──▶  RDD[filtered]
```

### Why RDDs matter for Machine Learning

ML algorithms (SGD, k-means, PageRank) are **iterative** — they pass over the same dataset many times. With MapReduce each pass costs a full disk read/write. With Spark, after the first pass the dataset lives in RAM and subsequent passes are ~100× faster.

---

## 3. Transformations vs. Actions

This is **the** key design principle of Spark. It enables lazy evaluation and query optimization.

### Transformations — lazy, return a new RDD/DataFrame

Computation is **deferred**. Spark builds a plan but does not execute it.

| Transformation | Description |
|---|---|
| `select(...)` | Project specific columns |
| `filter(...)` | Keep rows matching a condition |
| `groupBy(...)` | Group rows by key(s) |
| `orderBy(...)` | Sort rows |
| `distinct()` | Remove duplicate rows |
| `limit(n)` | Take first n rows (still lazy!) |
| `join(other, on)` | Join two DataFrames |
| `agg(...)` | Aggregate within groups |

### Actions — eager, trigger execution, return a non-RDD value

Calling an action causes Spark to execute the entire lineage graph up to that point.

| Action | Description |
|---|---|
| `show(n)` | Print first n rows to console |
| `count()` | Return the number of rows |
| `collect()` | Pull all rows to the driver as a Python list |
| `take(n)` | Pull first n rows to the driver |
| `save(...)` / `write` | Write results to storage |

### Lazy evaluation example

```python
# Nothing executes yet — Spark just records the plan
df = spark.read.csv("data.csv")               # lazy
filtered = df.filter(df.age > 30)             # lazy
grouped  = filtered.groupBy("city").count()   # lazy

# NOW Spark executes the full plan in one optimized pass
grouped.show()                                # ACTION → execution
```

**Why is laziness good?** Spark's *Catalyst optimizer* can reorder, fuse, and prune operations before running anything. It may push the `filter` before the `join`, saving enormous amounts of work.

---

## 4. The Spark Ecosystem

```
┌─────────────────────────────────────────────────────────┐
│  Spark SQL      │  Streaming  │  MLlib    │  GraphX     │
│  + DataFrames   │             │  (ML)     │  (Graphs)   │
├─────────────────────────────────────────────────────────┤
│                    Spark Core API                       │
├────────┬────────┬──────────┬──────────┬─────────────────┤
│   R    │  SQL   │  Python  │  Scala   │  Java           │
└────────┴────────┴──────────┴──────────┴─────────────────┘
```

- **Spark Core** — Scheduling, memory management, fault tolerance, RDD API
- **Spark SQL / DataFrames** — Structured data, SQL queries, Catalyst optimizer
- **Streaming** — Micro-batch or continuous processing of live data
- **MLlib** — Distributed ML: classification, regression, clustering, collaborative filtering
- **GraphX** — Graph-parallel computation (PageRank, connected components)

In this lab we focus on **Spark SQL + DataFrames**.

---

## 5. DataFrames & SparkSQL

A **DataFrame** is a distributed table with named, typed columns — think of it as a distributed Pandas DataFrame. It is built on top of RDDs but exposes a much higher-level API.

### Creating a SparkSession

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()
```

### Reading data

```python
boats    = spark.read.csv("boats.txt", schema="bid INT, bname STRING, color STRING")
sailors  = spark.read.json("sailors.json")
reserves = spark.read.json("reserves.json")
```

### Two equivalent interfaces

For every query you can use either **SparkSQL** (write SQL strings) or the **DataFrame transformation API** (chain Python methods). They produce identical execution plans.

#### Example: High-rating sailors

```python
# SparkSQL
sailors.createOrReplaceTempView("sailors")
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

#### Example: Join + aggregation

```python
# SparkSQL
spark.sql("""
    SELECT   s.sid, COUNT(r.bid) AS num_reserves
    FROM     sailors s
    JOIN     reserves r ON s.sid = r.sid
    WHERE    s.rating > 7
    GROUP BY s.sid
""").show()

# DataFrame API
from pyspark.sql.functions import count

sailors.join(reserves, "sid") \
       .filter(col("rating") > 7) \
       .groupBy("sid") \
       .agg(count("bid").alias("num_reserves")) \
       .select("sid", "num_reserves") \
       .show()
```

#### Example: Subquery / having below average

```python
from pyspark.sql.functions import avg

# SparkSQL with subquery
spark.sql("""
    SELECT bname, color, AVG(age) AS average_renter_age
    FROM   reserves
    JOIN   sailors ON sailors.sid = reserves.sid
    JOIN   boats   ON boats.bid   = reserves.bid
    GROUP BY bname, color
    HAVING AVG(sailors.age) < (
        SELECT AVG(age) FROM sailors JOIN reserves ON reserves.sid = sailors.sid
    )
""").show()

# DataFrame API
joined       = reserves.join(sailors, "sid").cache()   # cache for reuse
avg_per_boat = joined.groupBy("bid").agg(avg("age").alias("avg_age"))
avg_overall  = joined.select(avg("age")).collect()[0][0]  # scalar

below_avg = avg_per_boat.filter(avg_per_boat.avg_age < avg_overall)

below_avg.join(boats, "bid") \
         .select(boats.bname, boats.color, below_avg.avg_age.alias("average_renter_age")) \
         .show()
```

> **Note on `.cache()`**: When a DataFrame is used more than once (here `joined` is used to compute both `avg_per_boat` and `avg_overall`), calling `.cache()` stores it in memory after first computation and avoids re-reading and re-joining the data.

---

## 6. Running Spark on Dataproc

### Steps

1. Transfer your data files to HDFS:
   ```bash
   hdfs dfs -put boats.txt sailors.json reserves.json /user/$USER/
   ```

2. Submit your job — **always include `--deploy-mode client`**:
   ```bash
   spark-submit --deploy-mode client lab_5_examples.py
   ```

3. Monitor via Spark History Server:
   ```
   https://dataproc.hpc.nyu.edu/sparkhistory/
   ```
   Search by your userID to find your job, then inspect the DAG, stages, and task timelines.

### Reading from HDFS in code

```python
import os
userID = os.environ["USER"]

boats    = spark.read.csv(f"hdfs:/user/{userID}/boats.txt")
sailors  = spark.read.json(f"hdfs:/user/{userID}/sailors.json")
reserves = spark.read.json(f"hdfs:/user/{userID}/reserves.json")
```

### ⚠️ Most Common Mistake

```bash
# WRONG — "client" is treated as the script name
spark-submit client lab_5_examples.py

# CORRECT
spark-submit --deploy-mode client lab_5_examples.py
```

The error you'll see when doing it wrong is a cryptic Java `SparkException: Failed to get main class in JAR`.


## 7. Practical Exercises

Work through these using the sailors/boats/reserves dataset. For each, implement using **both** SparkSQL and the DataFrame API.

---

### Exercise 1 — Schema inspection

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lab5_practice").getOrCreate()

boats    = spark.read.csv("boats.txt",    schema="bid INT, bname STRING, color STRING")
sailors  = spark.read.json("sailors.json")
reserves = spark.read.json("reserves.json")

boats.printSchema()
sailors.printSchema()
reserves.printSchema()

boats.show()
sailors.show()
reserves.show()
```

**Questions:** What are the column names and types in each DataFrame? How many rows are in each? (Use `.count()`)

---

### Exercise 2 — Filter and project

**Task:** Find all sailors whose rating is greater than 7. Display `sid`, `sname`, and `rating`.

```python
# SparkSQL — fill in the blanks
sailors.createOrReplaceTempView("sailors")
spark.sql("SELECT ___, ___, ___ FROM sailors WHERE ___ > ___").show()

# DataFrame API
from pyspark.sql.functions import col
sailors.filter(___).select(___).show()
```

<details>
<summary>▶ Solution</summary>

```python
# SparkSQL
spark.sql("SELECT sid, sname, rating FROM sailors WHERE rating > 7").show()

# DataFrame API
sailors.filter(col("rating") > 7).select("sid", "sname", "rating").show()
```
</details>

---

### Exercise 3 — Join + aggregation

**Task:** For each sailor with rating > 7, count how many boat reservations they have made. Show `sid`, `sname`, and `num_reserves`.

<details>
<summary>▶ Solution</summary>

```python
from pyspark.sql.functions import count, col

# SparkSQL
reserves.createOrReplaceTempView("reserves")
spark.sql("""
    SELECT s.sid, s.sname, COUNT(r.bid) AS num_reserves
    FROM   sailors s
    JOIN   reserves r ON s.sid = r.sid
    WHERE  s.rating > 7
    GROUP  BY s.sid, s.sname
""").show()

# DataFrame API
sailors.join(reserves, "sid") \
       .filter(col("rating") > 7) \
       .groupBy("sid", "sname") \
       .agg(count("bid").alias("num_reserves")) \
       .show()
```
</details>

---

### Exercise 4 — Subquery / correlated average

**Task:** Find the names and colors of boats whose average renter age is **below** the overall average age of all renters.

<details>
<summary>▶ Solution</summary>

```python
from pyspark.sql.functions import avg

boats.createOrReplaceTempView("boats")

# SparkSQL
spark.sql("""
    SELECT bname, color, AVG(age) AS average_renter_age
    FROM   reserves
    JOIN   sailors ON sailors.sid = reserves.sid
    JOIN   boats   ON boats.bid   = reserves.bid
    GROUP  BY bname, color
    HAVING AVG(sailors.age) < (
        SELECT AVG(age) FROM sailors JOIN reserves ON reserves.sid = sailors.sid
    )
""").show()

# DataFrame API
joined       = reserves.join(sailors, "sid").cache()
avg_per_boat = joined.groupBy("bid").agg(avg("age").alias("avg_age"))
avg_overall  = joined.select(avg("age")).collect()[0][0]

avg_per_boat.filter(avg_per_boat.avg_age < avg_overall) \
            .join(boats, "bid") \
            .select(boats.bname, boats.color, avg_per_boat.avg_age.alias("average_renter_age")) \
            .show()
```
</details>

---

### Exercise 5 — Narrow vs. wide dependencies

Label each operation as **narrow** or **wide**:

| Operation | Narrow or Wide? | Why? |
|---|---|---|
| `filter(col("age") > 30)` | ? | |
| `groupBy("city").count()` | ? | |
| `select("name", "age")` | ? | |
| `join(other_df, "sid")` | ? | |
| `map(lambda x: x * 2)` | ? | |

<details>
<summary>▶ Answers</summary>

| Operation | Answer | Explanation |
|---|---|---|
| `filter(...)` | **Narrow** | Each output partition depends on exactly one input partition — no shuffle |
| `groupBy(...).count()` | **Wide** | Records with the same key may be on different nodes — requires shuffle |
| `select(...)` | **Narrow** | Just drops columns; no data movement between partitions |
| `join(...)` | **Wide** (usually) | Matching keys must co-locate — requires shuffle. Exception: broadcast join |
| `map(...)` | **Narrow** | Element-wise transformation within each partition |
</details>

---

### Exercise 6 — Caching benchmark (thought experiment)

Suppose you run k-means clustering on a dataset with 10 iterations. Each iteration reads the full dataset and computes new centroids.

- Disk read time per iteration: **10 seconds**
- RAM read time per iteration: **0.1 seconds**

**Without caching:** `10 × 10s = 100s`

**With caching:** `10s (first load) + 9 × 0.1s = 10.9s`

**Speedup ≈ 9.2×**

This scales dramatically with more iterations — which is exactly why Spark transformed ML on large datasets.

---

## 8. Common Mistakes & Tips

### Mistake 1: Missing `--deploy-mode` flag
Always: `spark-submit --deploy-mode client your_script.py`

### Mistake 2: Calling `.collect()` on a huge DataFrame
`collect()` pulls **all** data to the driver. On millions of rows this will cause an OOM error. Use `.show()` to preview, or `.write` to save results.

### Mistake 3: Not caching reused DataFrames
If a DataFrame appears in multiple downstream operations, call `.cache()` on it once. Otherwise Spark re-executes the full lineage each time.

### Mistake 4: Forgetting to register temp views
```python
df.createOrReplaceTempView("my_table")   # required before spark.sql(...)
spark.sql("SELECT * FROM my_table").show()
```

### Mistake 5: Shuffling unnecessarily large data
Push `filter` upstream of `groupBy` or `join` to minimize the data that gets shuffled across the network.

### Tip: Inspect query plans with `.explain()`
```python
df.filter(col("rating") > 7).groupBy("sid").count().explain()
```

### Tip: Develop locally first
```bash
pip install pyspark
```
Run against small local files to iterate fast, then submit to the cluster.

---

*Lab 5 — NYU Center for Data Science | Apache Spark*

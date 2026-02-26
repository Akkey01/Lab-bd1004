# Lab 5: Running a Hadoop MapReduce Job on NYU HPC

**NYU Big Data — Spring 2025**

---

## Overview

In this lab you will write a Python MapReduce job, test it locally, run it on a real Hadoop cluster, and retrieve the results. This is the exact workflow you'll use for **Homework 3**, so treat this as your practice run.

**By the end of this lab you will be able to:**

- Navigate a Linux terminal and inspect files with CLI commands
- Understand how MapReduce splits work across a cluster
- Write a mapper and reducer in Python using `mrjob`
- Run a job locally for debugging, then deploy it on Hadoop
- Retrieve and interpret results from HDFS

---

## Background

### Why can't we just use one machine?

A single laptop has 4–16 CPU cores and 8–32 GB of RAM. That's fine for a CSV with a few thousand rows. Real companies deal with **billions** of events per day and **petabytes** of data. At that scale a single machine simply crashes or takes days. The solution is a **cluster** — dozens to thousands of servers networked together, pooling their CPUs, memory, and storage so your job runs across all of them simultaneously.

### What is Hadoop?

Hadoop is an open-source framework with two core pieces:

| Component | What it does | Key idea |
|-----------|-------------|----------|
| **HDFS** (Hadoop Distributed File System) | Splits large files into 128 MB blocks and replicates each block across multiple machines | If one machine fails, your data is safe on others — like a distributed hard drive |
| **MapReduce** | Breaks computation into parallel tasks and sends each task to the machine that already holds the data | "Compute where the data lives" — avoids expensive network transfer |

### What is MapReduce?

Think of it as a **distributed `GROUP BY`**. It has three phases:

| Phase | Who runs it | What happens | Example |
|-------|------------|--------------|---------|
| **Map** | Your code | Reads each row and emits a `(key, value)` pair | `"Alice,2024,Milk,Grocery,3.50"` → `("Grocery", 1)` |
| **Shuffle & Sort** | Hadoop (automatic) | Groups all values that share the same key | `"Grocery"` → `[1, 1, 1, 1, …]` |
| **Reduce** | Your code | Receives one key + all its values, outputs the aggregated result | `"Grocery", [1,1,1,…]` → `("Grocery", 40)` |

The SQL equivalent of what we're building today:

```sql
SELECT category, COUNT(*)
FROM shopping_data
GROUP BY category;
```

MapReduce does the same thing — but across hundreds of machines in parallel.

---

## Our Dataset

**File:** `data/shopping_data_200.csv` — 200 rows of shopping transactions (plus 1 header row).

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `user_id` | TEXT | Customer identifier | U001 |
| `date` | DATE | Transaction date | 2024-01-15 |
| `item` | TEXT | Product purchased | Milk |
| `category` | TEXT | **Our GROUP BY key** (column index 3) | Grocery |
| `price` | FLOAT | Purchase amount (USD) | 3.50 |

**Sample row:**
```
U012,2024-01-15,Milk,Grocery,3.50
```

**Goal:** Count the number of purchases per category.

---

## Step 1 — Create Your GitHub Repo

1. Go to [github.com](https://github.com) → **New repository**
2. Name it exactly: **`lab5-hpc-mapreduce`** (this exact name is used for grading)
3. Create two folders: `data/` and `src/`
4. Upload `shopping_data_200.csv` into the `data/` folder and commit

Your repo should look like this:

```
lab5-hpc-mapreduce/
├── data/
│   └── shopping_data_200.csv
└── src/
    └── (empty for now)
```

> **Tip:** You can create folders in the GitHub web UI by typing `data/shopping_data_200.csv` as the filename when uploading — GitHub will create the folder automatically.

---

## Step 2 — Connect to the NYU Dataproc Cluster

Open the **Dataproc Web Terminal** link provided by your TA. Then run:

```bash
# Create a working directory and move into it
mkdir lab5
cd lab5

# Clone your GitHub repo onto the cluster
git clone https://github.com/<YOUR_USERNAME>/lab5-hpc-mapreduce.git

# Move into the repo
cd lab5-hpc-mapreduce
```

**Command reference:**

| Command | What it does |
|---------|-------------|
| `mkdir lab5` | **M**a**k**e a new **dir**ectory called `lab5` |
| `cd lab5` | **C**hange **d**irectory — move into the folder |
| `git clone <url>` | Download the entire GitHub repo to the cluster |

> **Important:** All remaining commands assume you are inside `lab5-hpc-mapreduce/`.

---

## Step 3 — Linux Warm-Up Commands

Before writing any code, get comfortable with the terminal and verify your files.

### 3a — Inspecting files

```bash
# List files and folders in the current directory
ls
# → data/  src/

# List contents of the data folder
ls data
# → shopping_data_200.csv

# Print your current location on the filesystem
pwd
# → /home/<user>/lab5/lab5-hpc-mapreduce

# Show the first 10 lines of the CSV (quick sanity check)
head data/shopping_data_200.csv

# Count total lines (201 = 200 data rows + 1 header)
wc -l data/shopping_data_200.csv

# Display column names with their index numbers
head -n 1 data/shopping_data_200.csv | tr ',' '\n' | nl
# →  1  user_id
#    2  date
#    3  item
#    4  category
#    5  price
```

**Quick reference for the commands above:**

| Command | Meaning |
|---------|---------|
| `ls` | **L**i**s**t files in a directory |
| `pwd` | **P**rint **w**orking **d**irectory |
| `head` | Show the first 10 lines of a file |
| `head -n 1` | Show only the first line |
| `wc -l` | **W**ord **c**ount — count **l**ines |
| `tr ',' '\n'` | **Tr**anslate (replace) commas with newlines |
| `nl` | **N**umber **l**ines |
| `\|` (pipe) | Send the output of one command as input to the next |

### 3b — Searching with `grep`

`grep` (**G**lobal **R**egular **E**xpression **P**rint) filters lines that match a pattern — like Ctrl+F for your terminal.

```bash
# Show the first 10 lines that contain "Grocery"
grep Grocery data/shopping_data_200.csv | head
# → U001,2024-01-01,Milk,Grocery,3.50
#   U007,2024-01-08,Bread,Grocery,2.20  ...

# Count how many Grocery transactions exist
grep -c Grocery data/shopping_data_200.csv

# Case-insensitive search
grep -i grocery data/shopping_data_200.csv | head
```

> **Try it yourself:** How many `Electronics` purchases are in the dataset? (Hint: `grep -c Electronics data/shopping_data_200.csv`)

---

## Step 4 — Write the MapReduce Job

Create the file using the `nano` text editor:

```bash
nano src/mr_sales_per_category.py
```

Paste the following code:

```python
from mrjob.job import MRJob
import csv

class MRSalesPerCategory(MRJob):

    def mapper(self, _, line):
        # Skip the header row
        if "user_id" in line:
            return

        # Parse the CSV line
        row = next(csv.reader([line]))
        category = row[3]          # 4th column = category

        # Emit (category, 1) for every transaction
        yield category, 1

    def reducer(self, key, values):
        # Sum all the 1s for each category
        yield key, sum(values)

if __name__ == "__main__":
    MRSalesPerCategory.run()
```

**Save and exit:** `Ctrl+X` → `Y` → `Enter`

### Code walkthrough

| Line(s) | What it does |
|---------|-------------|
| `from mrjob.job import MRJob` | Import the MRJob library — a Python wrapper that handles all the Hadoop plumbing for you |
| `import csv` | Python's built-in CSV parser — handles commas inside quoted fields correctly |
| `if "user_id" in line: return` | Detects the header row and skips it so we don't count column names as data |
| `row = next(csv.reader([line]))` | Parses one CSV line into a list of fields |
| `category = row[3]` | Grabs the 4th column (0-indexed) — this is our GROUP BY key |
| `yield category, 1` | Emits a key-value pair. Every transaction adds 1 to its category's count |
| `reducer(self, key, values)` | Called once per unique key. `values` is an iterator of all the 1s emitted for that key |
| `yield key, sum(values)` | Outputs the final count for each category |
| `MRSalesPerCategory.run()` | Entry point — tells MRJob to start the MapReduce pipeline |

### Commit and push to GitHub

```bash
git add src/mr_sales_per_category.py
git commit -m "Add MapReduce job for sales per category"
git push
```

---

## Step 5 — Run Locally (Debug Mode)

**Always test locally before submitting to the cluster.** Cluster errors are much harder to diagnose.

```bash
python3 src/mr_sales_per_category.py data/shopping_data_200.csv
```

What happens behind the scenes:

1. MRJob calls `mapper()` on each line of your CSV — simulating the Map phase
2. MRJob groups and sorts output by key in memory — simulating the Shuffle phase
3. MRJob calls `reducer()` for each unique key — simulating the Reduce phase
4. Results print directly to your terminal

**Expected output** (order may vary):

```
"Clothing"       42
"Electronics"    38
"Grocery"        40
"Personal Care"  39
"Stationery"     41
```

> **Troubleshooting:** If you see a Python error, read the traceback carefully. Common issues: wrong column index, missing `import csv`, or indentation errors.

---

## Step 6 — Run on the Hadoop Cluster

Now let's run the same job distributed across the cluster.

### Finding the Hadoop Streaming JAR

First, you need to locate the streaming JAR on your cluster. This is the bridge between your Python code and Hadoop's Java engine — Hadoop is written in Java, but your mapper/reducer are Python scripts. The streaming JAR tells Hadoop: "pipe each input line to this Python script's stdin, and collect its stdout as output."

The lab guide says it's at `/usr/lib/hadoop-mapreduce/hadoop-streaming*.jar`, but the actual location varies by cluster. **Search for it:**

```bash
find / -name "hadoop-streaming*.jar" 2>/dev/null
```

You might see multiple results like:

```
/usr/lib/hadoop/hadoop-streaming-3.3.6.jar
/usr/lib/hadoop/hadoop-streaming.jar
/usr/lib/hadoop-mapreduce/hadoop-streaming-2.10.2.jar
```

**Don't worry — they are all the same thing.** The different filenames are just different versions or symlinks (shortcuts). The one without a version number (e.g. `hadoop-streaming.jar`) is usually a symlink pointing to the versioned one. **Any of them will work — just pick one.**

Save it to a variable so you don't have to retype it:

```bash
STREAMING_JAR=$(find / -name "hadoop-streaming*.jar" 2>/dev/null | head -1)
echo $STREAMING_JAR
```

### Running the job

```bash
# 1. Create a unique output directory name using a timestamp
OUTDIR="lab5_out_$(date +%s)"

# 2. Submit the job to Hadoop
python3 src/mr_sales_per_category.py data/shopping_data_200.csv \
    -r hadoop \
    --hadoop-streaming-jar $STREAMING_JAR \
    --output-dir $OUTDIR \
    --python-bin python3
```

### Flag reference

| Flag | Purpose |
|------|---------|
| `-r hadoop` | **The key switch** — tells MRJob to use the real Hadoop cluster instead of local mode |
| `--hadoop-streaming-jar` | Path to the JAR that bridges Python code with Hadoop's Java-based streaming engine |
| `--output-dir $OUTDIR` | Where results are stored on HDFS. Must be a **new** directory every run |
| `--python-bin python3` | Tells all cluster worker nodes to use Python 3 (not Python 2) |

> **Why the timestamp?** `date +%s` returns seconds since Jan 1, 1970. This ensures every run gets a unique folder name, because **Hadoop cannot overwrite an existing output directory**.

The job will take a minute or two. You'll see progress logs showing map and reduce percentages.

---

## Step 7 — Retrieve Results from HDFS

HDFS is **not** your local filesystem. You need special `hadoop fs` commands to interact with it.

```bash
# List the output files on HDFS
hadoop fs -ls $OUTDIR

# Download and merge all output shards into one local file
hadoop fs -getmerge $OUTDIR result.out

# View the results
head result.out

# (Optional) Clean up the HDFS output directory
hadoop fs -rm -r $OUTDIR
```

### Command reference

| Command | What it does |
|---------|-------------|
| `hadoop fs -ls` | List files stored on HDFS |
| `hadoop fs -getmerge` | Download all `part-00000`, `part-00001`, … files and merge them into one local file |
| `hadoop fs -rm -r` | Recursively delete a directory from HDFS |

> **Why getmerge?** Hadoop splits output across multiple files (one per reducer). `getmerge` combines them so you have a single clean file.

### Expected output in `result.out`

```
"Grocery"        40
"Electronics"    38
"Stationery"     41
"Personal Care"  39
"Clothing"       42
```

The format is: `"key"<TAB>value`. MRJob wraps string keys in quotes — this is normal.

> **Note:** Output is **not guaranteed to be sorted**. If you need sorted output: `sort result.out`

---

## Quick Reference — Complete Command Sequence

```bash
# --- Setup ---
mkdir lab5 && cd lab5
git clone https://github.com/<YOUR_USERNAME>/lab5-hpc-mapreduce.git
cd lab5-hpc-mapreduce

# --- Explore ---
ls
head data/shopping_data_200.csv
wc -l data/shopping_data_200.csv

# --- Write code ---
nano src/mr_sales_per_category.py
# (paste code, Ctrl+X → Y → Enter)

# --- Test locally ---
python3 src/mr_sales_per_category.py data/shopping_data_200.csv

# --- Run on Hadoop ---
OUTDIR="lab5_out_$(date +%s)"
python3 src/mr_sales_per_category.py data/shopping_data_200.csv \
    -r hadoop \
    --hadoop-streaming-jar /usr/lib/hadoop-mapreduce/hadoop-streaming*.jar \
    --output-dir $OUTDIR \
    --python-bin python3

# --- Get results ---
hadoop fs -getmerge $OUTDIR result.out
head result.out

# --- Commit everything ---
git add src/mr_sales_per_category.py
git commit -m "Add MapReduce job for sales per category"
git push
```

---

## Common Mistakes & Troubleshooting

### 1. Hadoop Streaming JAR Not Found

**Error:** `No such file or directory: /usr/lib/hadoop-mapreduce/hadoop-streaming*.jar`

The JAR location varies by cluster. See **Step 6 — Finding the Hadoop Streaming JAR** above for the full fix. Quick version:

```bash
STREAMING_JAR=$(find / -name "hadoop-streaming*.jar" 2>/dev/null | head -1)
echo $STREAMING_JAR
```

---

### 2. `--output-dir: expected one argument` (Empty $OUTDIR)

**Error:** `mr_sales_per_category.py: error: argument -o/--output-dir: expected one argument`

This means `$OUTDIR` is empty. This happens when:
- You forgot to run the `OUTDIR=...` line
- Your SSH session disconnected and you reconnected (variables are lost)
- You opened a new terminal tab

**Fix — set it again right before running the job:**

```bash
OUTDIR="lab5_out_$(date +%s)"
```

Always run this line **immediately before** the `python3` command. If in doubt, check it:

```bash
echo $OUTDIR
# Should print something like: lab5_out_1772063840
# If it prints a blank line, the variable is empty — set it again
```

---

### 3. `FileAlreadyExistsException` (Output Directory Exists)

**Error:** `org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory ... already exists`

Hadoop refuses to overwrite existing output directories. Two fixes:

**Option A — Delete the old directory:**
```bash
hadoop fs -rm -r $OUTDIR
```

**Option B — Just create a new unique name:**
```bash
OUTDIR="lab5_out_$(date +%s)"
```

The timestamp ensures a unique name every time.

**To see what output directories you already have on HDFS:**
```bash
hadoop fs -ls
```

---

### 4. `No such file or directory` when running `hadoop fs -ls $OUTDIR`

This is **not an error** if you haven't run the Hadoop job yet. The output directory gets created *by* the Hadoop job. Run `hadoop fs -ls $OUTDIR` **after** the job finishes, not before.

---

### 5. Other Common Issues

| Problem | Cause | Fix |
|---------|-------|-----|
| `ModuleNotFoundError: mrjob` | mrjob not installed | Run `pip3 install mrjob` (should be pre-installed on Dataproc) |
| Output includes the header row | Header detection not working | Make sure `if "user_id" in line: return` is in your mapper |
| `IndexError: list index out of range` | A row has fewer columns than expected | Add a try/except around your CSV parsing, or check `len(row) > 3` |
| `Permission denied` on git push | Not authenticated | Set up a GitHub personal access token or SSH key on the cluster |
| All counts are 1 | Reducer is missing or not summing | Make sure you have `yield key, sum(values)` in your reducer |
| Job runs but no output appears | Forgot `-r hadoop` flag | Without it, MRJob runs locally and prints to terminal instead of HDFS |

---

## Bonus Challenges (Optional)

If you finish early, try modifying your MapReduce job:

1. **Total spending per category** — Instead of counting transactions, sum the `price` column. (Hint: `yield category, float(row[4])` in the mapper)
2. **Purchases per user** — Change the key from `category` to `user_id` (`row[0]`)
3. **Average price per category** — This requires emitting both count and sum from the mapper. In the reducer, compute `total_price / count`.

---

## Key Takeaways

- **MapReduce = Map + Shuffle + Reduce.** You write the Map and Reduce; Hadoop handles the Shuffle.
- **Always test locally first.** `python3 script.py input.csv` runs everything in memory on your machine.
- **`-r hadoop` is the switch** that sends your job to the real cluster.
- **HDFS ≠ local filesystem.** Use `hadoop fs` commands to interact with cluster storage.
- **Output directories must be unique.** Hadoop refuses to overwrite — use timestamps or delete before re-running.

---

*Good luck — you're ready for HW3!*

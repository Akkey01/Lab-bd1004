# Lab 4: HDFS and MapReduce - Complete Guide

## 📋 Table of Contents

1. [Prerequisites](#prerequisites)
2. [Part 1: GitHub Setup](#part-1-github-setup)
3. [Part 2: Clone Repository](#part-2-clone-repository)
4. [Part 3: Understanding HDFS](#part-3-understanding-hdfs)
5. [Part 4: Create Input File](#part-4-create-input-file)
6. [Part 5: Create MapReduce Program](#part-5-create-mapreduce-program)
7. [Part 6: Test Locally](#part-6-test-locally)
8. [Part 7: Upload to HDFS](#part-7-upload-to-hdfs)
9. [Part 8: Run on Hadoop](#part-8-run-on-hadoop)
10. [Part 9: Download Results](#part-9-download-results)
11. [Part 10: Commit to GitHub](#part-10-commit-to-github)
12. [Troubleshooting](#troubleshooting)
13. [Quick Reference](#quick-reference)

---

## Prerequisites

Before starting, you need:
- Access to NYU Dataproc cluster
- GitHub account
- Basic knowledge of terminal commands

**Time Required:** 60-75 minutes

---

## Part 1: GitHub Setup

### Step 1.1: Connect to Dataproc

1. Open your browser and go to: https://dataproc.hpc.nyu.edu/
2. Login with your NYU credentials (NetID and password)
3. Click **"SSH in Browser"**

You should see a terminal prompt:
```bash
username@nyu-dataproc-m:~$
```

### Step 1.2: Check if SSH Key Exists

```bash
ls ~/.ssh/
```

**If you see `id_ed25519` and `id_ed25519.pub`**, skip to [Part 2](#part-2-clone-repository).

**If not**, continue below.

### Step 1.3: Generate SSH Key

```bash
ssh-keygen -t ed25519 -C "your_netid@nyu.edu"
```

When prompted:
- **Enter file in which to save the key:** Press `Enter` (accept default)
- **Enter passphrase:** Press `Enter` (no passphrase)
- **Enter same passphrase again:** Press `Enter`

### Step 1.4: Display Your Public Key

```bash
cat ~/.ssh/id_ed25519.pub
```

**Copy the entire output.** It should start with `ssh-ed25519` and end with your email.

### Step 1.5: Add SSH Key to GitHub

1. Open a new browser tab: https://github.com/settings/keys
2. Click **"New SSH key"**
3. **Title:** `NYU Dataproc`
4. **Key:** Paste your public key
5. Click **"Add SSH key"**
6. Confirm with your GitHub password if prompted

### Step 1.6: Test SSH Connection

```bash
ssh -T git@github.com
```

**First time?** Type `yes` and press Enter.

**Success message:**
```
Hi username! You've successfully authenticated, but GitHub does not provide shell access.
```

✅ **SSH setup complete!**

---

## Part 2: Clone Repository

### Step 2.1: Create Repository on GitHub

1. Go to: https://github.com
2. Click **"+"** (top-right) → **"New repository"**
3. Fill in:
   - **Repository name:** `nyu-hdfs-lab4`
   - **Description:** `HDFS and MapReduce Lab`
   - **Visibility:** Private
   - **Do NOT** check "Initialize with README"
4. Click **"Create repository"**

### Step 2.2: Clone to Dataproc

```bash
# Navigate to home directory
cd ~

# Clone repository (replace YOUR_USERNAME with your GitHub username)
git clone git@github.com:YOUR_USERNAME/nyu-hdfs-lab4.git

# Navigate into repository
cd nyu-hdfs-lab4

# Create directory structure
mkdir -p word_count

# Navigate to word_count directory
cd word_count

# Verify location
pwd
```

**Expected output:** `/home/your_username/nyu-hdfs-lab4/word_count`

---

## Part 3: Understanding HDFS

### 🔑 Critical Concept: Two Separate File Systems

Your cluster has **TWO completely separate file systems**:

#### 1. Linux Filesystem
- **Location:** `/home/your_username/`
- **Commands:** `ls`, `cd`, `nano`, `cat`, `cp`, `mv`
- **Purpose:** Create and edit code, test locally
- **Access:** Direct file editing

#### 2. HDFS (Hadoop Distributed File System)
- **Location:** `hdfs:///user/your_username/`
- **Commands:** `hadoop fs -ls`, `hadoop fs -put`, `hadoop fs -get`
- **Purpose:** Store big data, run MapReduce jobs
- **Access:** Cannot edit files directly, only upload/download

### Workflow Diagram

```
┌─────────────────────────────────────────────────────┐
│  Linux Filesystem (/home/username/)                 │
│  - Create/edit code                                 │
│  - Test locally                                     │
│  📄 mr_wordcount.py                                 │
│  📄 book.txt                                        │
└─────────────────────────────────────────────────────┘
                    ↓
              hadoop fs -put
                    ↓
┌─────────────────────────────────────────────────────┐
│  HDFS (hdfs:///user/username/)                      │
│  - Store data                                       │
│  - Run MapReduce                                    │
│  📄 book.txt (uploaded)                             │
│  📁 wordcount_output/                               │
└─────────────────────────────────────────────────────┘
                    ↓
            hadoop fs -getmerge
                    ↓
┌─────────────────────────────────────────────────────┐
│  Linux Filesystem                                   │
│  📄 result.txt (downloaded)                         │
└─────────────────────────────────────────────────────┘
```

---

## Part 4: Create Input File

### Step 4.1: Create book.txt

```bash
# Make sure you're in the correct directory
cd ~/nyu-hdfs-lab4/word_count

# Create the file
nano book.txt
```

### Step 4.2: Add Content

Copy and paste this text into nano:

```
The quick brown fox jumps over the lazy dog
The lazy dog sleeps under the tree
A quick fox runs through the forest
The forest is quiet and peaceful
The quick brown cat climbs the tree
The dog and the cat play together
The peaceful forest has many trees
```

### Step 4.3: Save and Exit

- **Save:** Press `Ctrl + O`, then `Enter`
- **Exit:** Press `Ctrl + X`

### Step 4.4: Verify File

```bash
# Check file exists
ls -la

# View contents
cat book.txt
```

✅ **Input file created!**

---

## Part 5: Create MapReduce Program

### Step 5.1: Create Python File

```bash
nano mr_wordcount.py
```

### Step 5.2: Add Complete Code

Copy this entire code into nano:

```python
#!/usr/bin/env python3
"""
Word Count MapReduce Program
Lab 4: HDFS and MapReduce

This program counts the frequency of each word in a text file.

How it works:
1. Mapper: Reads each line, extracts words, emits (word, 1)
2. Shuffle: Hadoop groups all values by key (automatic)
3. Reducer: Sums all counts for each word
"""

from mrjob.job import MRJob
import re

# Regular expression to match words
# Matches letters, numbers, and apostrophes
WORD_RE = re.compile(r"[\w']+")

class MRWordCount(MRJob):
    
    def mapper(self, _, line):
        """
        MAPPER PHASE
        
        Input: Each line of text from the input file
        Output: (word, 1) for every word found
        
        Example:
        Input:  "The quick brown fox"
        Output: ("the", 1), ("quick", 1), ("brown", 1), ("fox", 1)
        """
        # Extract all words from the line using regex
        words = WORD_RE.findall(line)
        
        # For each word found
        for word in words:
            # Convert to lowercase (so "The" and "the" are same)
            word_lower = word.lower()
            
            # Emit (word, 1) - means "I found this word once"
            yield (word_lower, 1)
    
    def reducer(self, word, counts):
        """
        REDUCER PHASE
        
        Input: A word and ALL its counts from all mappers
        Output: (word, total_count)
        
        Example:
        Input:  word = "the", counts = [1, 1, 1, 1, 1, 1, 1]
        Output: ("the", 7)
        """
        # Sum all the 1s to get total occurrences
        total = sum(counts)
        
        # Emit the word with its total count
        yield (word, total)

# Run the MapReduce job
if __name__ == '__main__':
    MRWordCount.run()
```

### Step 5.3: Save and Exit

- **Save:** Press `Ctrl + O`, then `Enter`
- **Exit:** Press `Ctrl + X`

### Step 5.4: Verify Files

```bash
ls -la
```

You should see:
- `book.txt`
- `mr_wordcount.py`

✅ **MapReduce program created!**

### 📖 Understanding the Code

#### The Mapper Function

**What it does:**
- Receives one line of text at a time
- Extracts all words using regular expression
- Converts each word to lowercase
- Emits `(word, 1)` for each word found

**Example:**
```
Input:  "The quick brown fox"
Output: ("the", 1), ("quick", 1), ("brown", 1), ("fox", 1)
```

#### The Shuffle Phase (Automatic)

**What Hadoop does:**
- Groups all values by their key
- No code needed - framework handles this

**Example:**
```
After shuffle:
  "the"    → [1, 1, 1, 1, 1, 1, 1]
  "quick"  → [1, 1, 1]
  "brown"  → [1, 1]
```

#### The Reducer Function

**What it does:**
- Receives a word and ALL its counts
- Sums all the counts
- Emits `(word, total_count)`

**Example:**
```
Input:  "the", [1, 1, 1, 1, 1, 1, 1]
Action: sum([1, 1, 1, 1, 1, 1, 1]) = 7
Output: ("the", 7)
```

---

## Part 6: Test Locally

### ⚠️ Important: Always Test Locally First!

Testing locally is **100x faster** than Hadoop. Always verify your code works before running on the cluster.

### Step 6.1: Run Locally

```bash
# Make sure you're in the correct directory
cd ~/nyu-hdfs-lab4/word_count

# Run the MapReduce program locally
python3 mr_wordcount.py book.txt
```

### Step 6.2: Expected Output

You should see something like:

```
No configs found; falling back on auto-configuration
No configs specified for inline runner
Creating temp directory /tmp/mr_wordcount...
Running step 1 of 1...
Streaming final output from /tmp/...
"a"         1
"and"       2
"brown"     2
"cat"       2
"climbs"    1
"dog"       3
"forest"    2
"fox"       2
"has"       1
"is"        1
"jumps"     1
"lazy"      2
"many"      1
"over"      1
"peaceful"  2
"play"      1
"quick"     3
"quiet"     1
"runs"      1
"sleeps"    1
"the"       7
"through"   1
"together"  1
"tree"      2
"trees"     1
"under"     1
Removing temp directory...
```

✅ **If you see word counts, your program works!**

### Step 6.3: Save Local Results

```bash
# Save output to a file
python3 mr_wordcount.py book.txt > output_local.txt

# View top 10 most frequent words
cat output_local.txt | sort -t$'\t' -k2 -nr | head -10
```

**Output:**
```
"the"       7
"quick"     3
"dog"       3
"brown"     2
"cat"       2
"forest"    2
"fox"       2
"lazy"      2
"peaceful"  2
"tree"      2
```

✅ **Local testing complete!**

---

## Part 7: Upload to HDFS

### Step 7.1: Check Current HDFS Contents

```bash
hadoop fs -ls
```

This shows what files are currently in your HDFS home directory.

### Step 7.2: Upload Input File

```bash
# Upload book.txt to HDFS
hadoop fs -put book.txt

# Verify upload
hadoop fs -ls
```

**Expected output:**
```
-rw-r--r--   3 username supergroup    245 2026-02-18 16:30 book.txt
```

✅ **File uploaded to HDFS!**

### 📝 Note on File Already Exists

If you see this error:
```
put: `book.txt': File exists
```

**Solution:** Use one of these options:

```bash
# Option 1: Remove old file first
hadoop fs -rm book.txt
hadoop fs -put book.txt

# Option 2: Force overwrite
hadoop fs -put -f book.txt
```

---

## Part 8: Run on Hadoop

### Step 8.1: Clean Old Output (Important!)

```bash
# Remove old output directory if it exists
hadoop fs -rm -r wordcount_output
```

**Note:** It's OK if this shows "No such file or directory" - means no old output exists.

### Step 8.2: Run MapReduce on Hadoop

```bash
python3 mr_wordcount.py -r hadoop \
  hdfs:///user/$USER/book.txt \
  -o hdfs:///user/$USER/wordcount_output
```

**Command breakdown:**
- `python3 mr_wordcount.py` - Your MapReduce script
- `-r hadoop` - Run on Hadoop (not locally)
- `hdfs:///user/$USER/book.txt` - Input file location in HDFS
- `-o hdfs:///user/$USER/wordcount_output` - Output directory in HDFS

### Step 8.3: What You'll See

The job will take **1-3 minutes**. You'll see:

```
No configs found; falling back on auto-configuration
No configs specified for hadoop runner
Looking for hadoop binary in /usr/lib/hadoop/bin...
Found hadoop binary: /usr/lib/hadoop/bin/hadoop
Using Hadoop version 3.3.6
Creating temp directory /tmp/mr_wordcount...
Uploading working dir files to hdfs:///user/username/tmp/...
Running step 1 of 1...
  packageJobJar: []
  Running job: job_1234567890123_0001
  map 0% reduce 0%
  map 50% reduce 0%
  map 100% reduce 0%
  map 100% reduce 100%
Job succeeded
Streaming final output from hdfs:///user/username/wordcount_output...
```

✅ **When you see "Job succeeded", your MapReduce job completed!**

### Step 8.4: Monitor Job (Optional)

While the job is running, you can monitor it:

1. Open browser: https://dataproc.hpc.nyu.edu/jobhistory/
2. Find your job at the top of the list
3. Click the Job ID to see details:
   - Map progress
   - Reduce progress
   - Start and finish times
   - Task details

### Step 8.5: Verify Output Created

```bash
# List output directory contents
hadoop fs -ls wordcount_output
```

**Expected output:**
```
Found 3 items
-rw-r--r--   part-00000
-rw-r--r--   part-00001
-rw-r--r--   _SUCCESS
```

The `_SUCCESS` file indicates the job completed successfully!

---

## Part 9: Download Results

### Step 9.1: Merge Output Parts

MapReduce creates multiple output files (part-00000, part-00001, etc.). We need to merge them into one file.

```bash
# Merge all parts into a single file
hadoop fs -getmerge wordcount_output wordcount_hadoop.txt

# Verify file was created
ls -la
```

You should now see `wordcount_hadoop.txt` in your directory.

### Step 9.2: View Results

```bash
# View top 10 most frequent words
cat wordcount_hadoop.txt | sort -t$'\t' -k2 -nr | head -10
```

**Expected output:**
```
"the"       7
"quick"     3
"dog"       3
"brown"     2
"cat"       2
"forest"    2
"fox"       2
"lazy"      2
"peaceful"  2
"tree"      2
```

### Step 9.3: Compare Local vs Hadoop Results

```bash
# Compare the two results
echo "=== LOCAL RESULTS ==="
cat output_local.txt | sort -t$'\t' -k2 -nr | head -10

echo ""
echo "=== HADOOP RESULTS ==="
cat wordcount_hadoop.txt | sort -t$'\t' -k2 -nr | head -10
```

**They should be identical!** The difference is:
- **Local:** Runs on one machine, fast for small files
- **Hadoop:** Distributes work across cluster, scalable for huge files

✅ **Results downloaded and verified!**

---

## Part 10: Commit to GitHub

### Step 10.1: Configure Git (First Time Only)

```bash
git config --global user.name "Your Name"
git config --global user.email "your_netid@nyu.edu"
```

### Step 10.2: Check Status

```bash
cd ~/nyu-hdfs-lab4
git status
```

You'll see all your new files in red (untracked).

### Step 10.3: Add All Files

```bash
git add .
```

### Step 10.4: Commit Changes

```bash
git commit -m "Complete Lab 4: HDFS and MapReduce word count"
```

### Step 10.5: Push to GitHub

```bash
git push origin main
```

**If that fails**, try:
```bash
git push origin master
```

### Step 10.6: Verify on GitHub

1. Go to: `https://github.com/YOUR_USERNAME/nyu-hdfs-lab4`
2. Refresh the page
3. You should see all your files:
   - `word_count/book.txt`
   - `word_count/mr_wordcount.py`
   - `word_count/output_local.txt`
   - `word_count/wordcount_hadoop.txt`

✅ **Lab complete and backed up on GitHub!**

---

## Troubleshooting

### Issue 1: Output Directory Already Exists

**Error:**
```
Error Launching job : Output directory hdfs://nyu-dataproc-m/user/username/wordcount_output already exists
```

**Solution:**
```bash
hadoop fs -rm -r wordcount_output
# Then re-run your MapReduce job
```

---

### Issue 2: Input File Not Found

**Error:**
```
Exception: Input path hdfs:///user/username/book.txt does not exist!
```

**Solution:**
```bash
# Check what's in HDFS
hadoop fs -ls

# Upload the file if missing
hadoop fs -put book.txt

# Verify
hadoop fs -ls
```

---

### Issue 3: File Already Exists in HDFS

**Error:**
```
put: `book.txt': File exists
```

**Solution (choose one):**
```bash
# Option 1: Remove and re-upload
hadoop fs -rm book.txt
hadoop fs -put book.txt

# Option 2: Force overwrite
hadoop fs -put -f book.txt
```

---

### Issue 4: SSH Authentication Failed

**Error:**
```
remote: Invalid username or token. Password authentication is not supported
```

**Problem:** You're using HTTPS URL instead of SSH.

**Solution:** Use SSH URL format:
```bash
# Wrong (HTTPS)
git clone https://github.com/username/repo.git

# Correct (SSH)
git clone git@github.com:username/repo.git
```

---

### Issue 5: Job Takes Too Long

**If your job is stuck:**

1. Check job status: https://dataproc.hpc.nyu.edu/jobhistory/
2. Find your job (most recent at top)
3. Check the state:
   - **RUNNING:** Be patient, wait for completion
   - **FAILED:** Click Job ID to see error details
   - **Stuck at map 0%:** Input file might be too large or code has issues

For small files like ours, the job should complete in 1-3 minutes.

---

### Issue 6: Cannot Find Downloaded File

**Problem:** File not in expected location.

**Solution:**
```bash
# Check current directory
pwd

# Should be in: /home/username/nyu-hdfs-lab4/word_count

# If not, navigate there
cd ~/nyu-hdfs-lab4/word_count

# List files
ls -la

# Run getmerge again
hadoop fs -getmerge wordcount_output wordcount_hadoop.txt
```

---

## Quick Reference

### HDFS Commands

| Task | Command |
|------|---------|
| List files | `hadoop fs -ls` |
| Upload file | `hadoop fs -put file.txt` |
| Download file | `hadoop fs -get file.txt` |
| Merge output | `hadoop fs -getmerge output/ result.txt` |
| View file | `hadoop fs -cat file.txt` |
| Remove file | `hadoop fs -rm file.txt` |
| Remove directory | `hadoop fs -rm -r dirname` |
| Check space | `hadoop fs -du -h` |

### MapReduce Commands

| Task | Command |
|------|---------|
| Test locally | `python3 mr_script.py input.txt` |
| Run on Hadoop | `python3 mr_script.py -r hadoop hdfs:///user/$USER/input.txt -o hdfs:///user/$USER/output` |
| Monitor jobs | https://dataproc.hpc.nyu.edu/jobhistory/ |

### Git Commands

| Task | Command |
|------|---------|
| Check status | `git status` |
| Add files | `git add .` |
| Commit | `git commit -m "message"` |
| Push | `git push origin main` |
| Pull | `git pull` |

### Common Workflow

```bash
# 1. Navigate to directory
cd ~/nyu-hdfs-lab4/word_count

# 2. Test locally (always first!)
python3 mr_wordcount.py book.txt

# 3. Upload to HDFS
hadoop fs -put -f book.txt

# 4. Clean old output
hadoop fs -rm -r wordcount_output

# 5. Run on Hadoop
python3 mr_wordcount.py -r hadoop hdfs:///user/$USER/book.txt -o hdfs:///user/$USER/wordcount_output

# 6. Download results
hadoop fs -getmerge wordcount_output result.txt

# 7. View results
cat result.txt | sort -t$'\t' -k2 -nr | head -10

# 8. Commit to GitHub
git add .
git commit -m "Update results"
git push
```

---

## Summary

### What You Accomplished

✅ Set up GitHub SSH keys  
✅ Created and cloned a GitHub repository  
✅ Understood HDFS vs Linux filesystem  
✅ Created input data file  
✅ Wrote a complete MapReduce program  
✅ Tested locally (fast debugging)  
✅ Uploaded data to HDFS  
✅ Ran MapReduce on Hadoop cluster  
✅ Downloaded and verified results  
✅ Committed everything to GitHub  

### Key Takeaways

1. **Two file systems:** Linux filesystem for code, HDFS for data
2. **Always test locally first:** It's much faster than Hadoop
3. **Clean output before re-running:** `hadoop fs -rm -r output`
4. **Use SSH for GitHub:** Not HTTPS
5. **Monitor jobs:** https://dataproc.hpc.nyu.edu/jobhistory/
6. **Commit frequently:** Save your work to GitHub regularly

---

## 📚 Additional Resources

- **mrjob Documentation:** https://mrjob.readthedocs.io/
- **Hadoop Documentation:** https://hadoop.apache.org/docs/
- **NYU HPC Guide:** https://sites.google.com/nyu.edu/nyu-hpc
- **Job Monitoring:** https://dataproc.hpc.nyu.edu/jobhistory/

---

**Congratulations on completing Lab 4!** 🎉

You now have the skills to work with HDFS and run MapReduce jobs on a Hadoop cluster.

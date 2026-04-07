# Lab 9 Report: GPU Computing

**Name:**
**NetID:**
**Date:**

---

## Q1: SAXPY Speedup

CPU time (N=10M): ___ ms  
GPU kernel-only time (N=10M): ___ ms  
GPU end-to-end time (N=10M): ___ ms  

Kernel-only speedup: ___x  
End-to-end speedup: ___x  

**Explanation:**

---

## Q2: Crossover Point

Approximate problem size where GPU end-to-end first beats CPU: ___

**Explanation of why this crossover exists:**

---

## Q3: Shared Memory and syncthreads

**Why is `cuda.syncthreads()` called inside the reduction loop rather than once before it, and what breaks if you remove it?**

---

## Q4: Matrix Multiply vs SAXPY

**Why is the matrix multiply GPU speedup much larger than SAXPY?**

---

## Q5: When Not to Use a GPU

**Describe a realistic data science task where GPU would be slower end-to-end than CPU, and explain why:**

---

## Q6: Framework Choice

| Task | Framework choice | Reason |
|---|---|---|
| Training a transformer model | | |
| Aggregating a 100 GB CSV on a DataFrame | | |
| Custom non-standard activation function | | |

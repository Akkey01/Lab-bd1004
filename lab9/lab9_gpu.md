# Lab 9: GPU Computing
**DS-GA 1004: Big Data — NYU Center for Data Science**

---

## Before You Start

This lab runs on **Torch Cloud Bursting** — NYU's GPU cluster. You will write and run Python code in a Jupyter notebook on a real GPU. Every section below is a step you follow in order. By the end you will have written GPU kernels from scratch and seen firsthand when GPUs help and when they don't.

**What you need:**
- NYU VPN (if off campus)
- Your NYU NetID and password
- About 2 hours

**No dataset is needed.** All exercises use randomly generated arrays.

---

## Step 1: Connect to the NYU VPN

If you are on campus (NYU Wi-Fi), skip this step.

If you are off campus:
1. Open the Cisco AnyConnect VPN client on your laptop.
2. Connect to `vpn.nyu.edu` using your NYU credentials.
3. Once connected, continue to Step 2.

---

## Step 2: Launch a GPU Session on Torch Cloud Bursting

1. Open your browser and go to: `https://ood-burst-001.hpc.nyu.edu/`
2. Log in with your NYU NetID and password.
3. Once on the dashboard, click **Jupyter** in the top menu.
4. A form will appear. Fill it in **exactly** as follows:

| Field | Value |
|---|---|
| Number of GPUs | `1` |
| Slurm Account | `cims_ga_1003-2026sp` |
| Slurm Partition | `n1s8-t4-1` |
| Optional Slurm options | `--requeue` |
| Root directory | `home` |
| Number of hours | `2` |

5. Click **Launch**.
6. You will see a queue screen. Wait until the button turns green and says **Connect to Jupyter**. This usually takes 1–3 minutes.
7. Click **Connect to Jupyter**.

> **Spot instance warning:** Torch Cloud Bursting runs on Google Cloud spot instances. Your session can be interrupted at any time. Save your notebook frequently with Ctrl+S, and save a copy to `/scratch/$USER` before ending your session.

---

## Step 3: Set Up Your Environment

This step installs everything you need for the entire lab. You only need to do this once — if your session gets interrupted, your environment will still be there when you relaunch.

1. Click **File → New → Terminal** to open a terminal tab.
2. Copy and paste the following commands into the terminal **one at a time**, pressing Enter after each and waiting for it to finish before running the next:

**Create the conda environment:**
```bash
conda create -n gpu_lab python=3.10 -y
```

Wait until you see `done`. Then activate it:

```bash
source activate gpu_lab
```

> **Note:** Use `source activate`, not `conda activate`. The default shell on Cloud Bursting does not support `conda activate`.

Your prompt should now start with `(gpu_lab)`. 

**Install all packages** (this takes 2–3 minutes):
```bash
pip install numba numpy matplotlib ipykernel nvidia-cuda-nvcc-cu12 nvidia-cuda-runtime-cu12 torch --index-url https://download.pytorch.org/whl/cu118
```

Wait until you see `Successfully installed ...` at the end.

**Register the kernel for Jupyter:**
```bash
python -m ipykernel install --user --name gpu_lab --display-name "gpu_lab" --env CUDA_HOME "$CONDA_PREFIX/lib/python3.10/site-packages/nvidia/cuda_nvcc"
```

You should see:
```
Installed kernelspec gpu_lab in /home/<your-netid>/.local/share/jupyter/kernels/gpu_lab
```

**Keep your session alive** (prevents idle timeout):
```bash
while true; do sleep 60; done &
```

You will see something like `[1] 5736` — that means it is running in the background. You can ignore it.

3. Go back to the Jupyter tab and **refresh the page** (press F5). This is required for Jupyter to see the new kernel.
4. Click **File → New → Notebook**.
5. When asked to select a kernel, choose **gpu_lab** from the dropdown.

You are now ready to start the lab. All remaining work happens in the notebook — you do not need to go back to the terminal.

---

## Step 4: Verify Your GPU

Copy the code below into the first cell of your notebook and press **Shift+Enter** to run it.

```python
from numba import cuda

gpu = cuda.get_current_device()
print("GPU detected:", gpu.name)
print("Compute capability:", gpu.compute_capability)
print("Max threads per block:", gpu.MAX_THREADS_PER_BLOCK)
print("Number of multiprocessors:", gpu.MULTIPROCESSOR_COUNT)
```

**You should see something like:**
```
GPU detected: b'Tesla T4'
Compute capability: (7, 5)
Max threads per block: 1024
Number of multiprocessors: 40
```

If you see an error, your session may not have a GPU. Go back to the OOD dashboard, end the session, and relaunch with the same settings from Step 2.

---

## Background: How a GPU Works

Before writing your first kernel, read this — it will make the code make sense.

### CPU vs GPU

A CPU has a few powerful cores (typically 4–32) designed to do one thing at a time, very fast. A GPU has thousands of small, simple cores designed to do many things at the same time.

Think of it this way: a CPU is an expert chef who can cook any dish, quickly. A GPU is a factory with thousands of workers all cooking the same dish simultaneously.

GPUs are fast for tasks where you need to do the **same operation on millions of values at once** — like multiplying every element of a large array, or computing a layer in a neural network.

### Threads, Blocks, and Grids

When you run code on a GPU, your function (called a **kernel**) runs once per **thread**. Threads are organized into **blocks**, and blocks are organized into a **grid**.

```
Grid  →  many Blocks  →  each Block has many Threads
```

Each thread knows its own position using three built-in values:
- `cuda.threadIdx.x` — the thread's position within its block (0, 1, 2, ...)
- `cuda.blockIdx.x` — which block this thread belongs to (0, 1, 2, ...)
- `cuda.blockDim.x` — how many threads are in each block

To find the thread's **global position** in the entire array:

```
i = blockIdx.x * blockDim.x + threadIdx.x
```

This is equivalent to the `i` in a regular Python for loop. Each thread processes one element of your data.

### Memory: The Biggest Bottleneck

Your data starts in CPU memory (RAM). To use the GPU, you must **copy it to the GPU first** over a connection called the PCIe bus. When computation is done, you copy results back.

This transfer is slow. For small arrays, the transfer time is longer than just computing on the CPU. GPUs only win when the array is large enough that the parallelism outweighs the transfer cost. You will measure this yourself in Part 3.

---

## Part 1: SAXPY — Your First GPU Kernel

SAXPY means: `y[i] = a * x[i] + y[i]` for every element `i`. It is the standard first GPU example because every element is completely independent — perfect for parallelism.

### 1a. CPU version (baseline)

Copy this into a new cell and run it:

```python
import numpy as np
import time

N = 10_000_000        # 10 million elements
a = 2.5
x = np.random.rand(N).astype(np.float32)
y = np.random.rand(N).astype(np.float32)

start = time.perf_counter()
y_result_cpu = a * x + y
cpu_time = time.perf_counter() - start

print(f"CPU time: {cpu_time * 1000:.2f} ms")
```

**You should see something like:**
```
CPU time: 20.00 ms
```

Your exact number will differ. Write it down — you will compare it to the GPU time shortly.

### 1b. GPU kernel definition

Now write the same operation as a GPU kernel. Copy this into a new cell and run it:

```python
from numba import cuda

# The @cuda.jit decorator tells Numba to compile this function for the GPU
@cuda.jit
def saxpy_kernel(a, x, y, out):
    # Each thread computes one element
    # This line gives each thread a unique index i
    i = cuda.blockIdx.x * cuda.blockDim.x + cuda.threadIdx.x

    # Guard: make sure we don't go past the end of the array
    if i < x.shape[0]:
        out[i] = a * x[i] + y[i]
```

This defines the kernel but does not run it yet. Run the cell — there will be no output, which is expected.

### 1c. Transfer data to the GPU and run the kernel

Copy this into a new cell and run it:

```python
# Copy arrays from CPU memory to GPU memory
d_x = cuda.to_device(x)
d_y = cuda.to_device(y)

# Allocate an empty output array directly on the GPU
d_out = cuda.device_array(N, dtype=np.float32)

# Configure how many threads and blocks to use
threads_per_block = 256
blocks_per_grid = (N + threads_per_block - 1) // threads_per_block

print(f"Launching {blocks_per_grid} blocks of {threads_per_block} threads each")
print(f"Total threads: {blocks_per_grid * threads_per_block:,}")
```

**You should see:**
```
Launching 39063 blocks of 256 threads each
Total threads: 10,000,128
```

The total threads is slightly more than N — that is fine, the `if i < x.shape[0]` guard in the kernel handles the extra threads safely.

### 1d. Time the GPU kernel

Copy this into a new cell and run it:

```python
# First run: Numba compiles the kernel on the first call (warm-up)
# We do not time this run because the compilation time is a one-time cost
saxpy_kernel[blocks_per_grid, threads_per_block](a, d_x, d_y, d_out)
cuda.synchronize()   # wait for GPU to finish before continuing

# Second run: this is the one we time
start = time.perf_counter()
saxpy_kernel[blocks_per_grid, threads_per_block](a, d_x, d_y, d_out)
cuda.synchronize()   # always call this before stopping the timer
gpu_kernel_time = time.perf_counter() - start

print(f"GPU kernel time: {gpu_kernel_time * 1000:.2f} ms")
print(f"Speedup over CPU: {cpu_time / gpu_kernel_time:.1f}x")
```

**You should see something like:**
```
GPU kernel time: 1.17 ms
Speedup over CPU: 17.0x
```

> **Why `cuda.synchronize()`?** GPU kernels run asynchronously — your Python code moves on immediately after launching the kernel, while the GPU is still working. Without `synchronize()` you would be timing only the launch instruction (microseconds), not the actual computation. Always call it before stopping a timer.

### 1e. Full round-trip time (including data transfer)

The kernel time above assumes data is already on the GPU. In practice you also pay for the transfer. Copy this into a new cell and run it:

```python
start_full = time.perf_counter()

# Transfer to GPU
d_x2 = cuda.to_device(x)
d_y2 = cuda.to_device(y)
d_out2 = cuda.device_array(N, dtype=np.float32)

# Run kernel
saxpy_kernel[blocks_per_grid, threads_per_block](a, d_x2, d_y2, d_out2)
cuda.synchronize()

# Transfer result back to CPU
result = d_out2.copy_to_host()

full_time = time.perf_counter() - start_full

print(f"GPU full round-trip time (with data transfer): {full_time * 1000:.2f} ms")
print(f"Round-trip speedup over CPU: {cpu_time / full_time:.1f}x")
```

**You should see something like:**
```
GPU full round-trip time (with data transfer): 36.70 ms
Round-trip speedup over CPU: 0.5x
```

Notice: the GPU is now **slower** than the CPU once you include the data transfer. The kernel itself is fast, but copying 10 million floats across the PCIe bus costs more than the computation gains. This is the central tension of GPU programming, and you will explore it properly in Part 3.

---

## Part 2: Dot Product with Shared Memory

SAXPY was easy because every thread worked independently. The dot product is harder — each thread computes a partial product, then all partial products need to be added together. This requires threads to **communicate**.

The solution is **shared memory**: a small, fast memory region shared by all threads in the same block. Here is how it works:

1. Each thread loads one product into shared memory.
2. All threads wait (`cuda.syncthreads()`) until everyone has loaded their value.
3. Threads cooperate to add up all values in the block (reduction).
4. Thread 0 of each block writes the block's total to global memory.
5. The host adds up all the block totals.

Copy this into a new cell and run it:

```python
from numba import cuda, float32
import math

@cuda.jit
def dot_product_kernel(a, b, partial_sums):
    # Allocate shared memory: one float per thread in this block
    # This memory is shared between all threads in the same block
    shared = cuda.shared.array(shape=256, dtype=float32)

    tid = cuda.threadIdx.x                                       # thread index within block
    i   = cuda.blockIdx.x * cuda.blockDim.x + cuda.threadIdx.x  # global index

    # Step 1: each thread loads its product into shared memory
    if i < a.shape[0]:
        shared[tid] = a[i] * b[i]
    else:
        shared[tid] = 0.0

    # Step 2: wait for ALL threads in this block to finish loading
    cuda.syncthreads()

    # Step 3: parallel reduction — halve the active threads each round
    stride = cuda.blockDim.x // 2
    while stride > 0:
        if tid < stride:
            shared[tid] += shared[tid + stride]
        cuda.syncthreads()   # wait after each round before continuing
        stride //= 2

    # Step 4: thread 0 writes this block's result to global memory
    if tid == 0:
        partial_sums[cuda.blockIdx.x] = shared[0]
```

Now run the kernel and verify the result. Copy this into a new cell and run it:

```python
N = 1_000_000
a_np = np.random.rand(N).astype(np.float32)
b_np = np.random.rand(N).astype(np.float32)

threads_per_block = 256
blocks_per_grid = math.ceil(N / threads_per_block)

d_a = cuda.to_device(a_np)
d_b = cuda.to_device(b_np)
d_partial = cuda.device_array(blocks_per_grid, dtype=np.float32)

# Run kernel
dot_product_kernel[blocks_per_grid, threads_per_block](d_a, d_b, d_partial)
cuda.synchronize()

# Step 5: sum the partial results on the CPU
gpu_dot = float(d_partial.copy_to_host().sum())

# Compare to NumPy
cpu_dot = float(np.dot(a_np, b_np))

print(f"GPU dot product: {gpu_dot:.4f}")
print(f"CPU dot product: {cpu_dot:.4f}")
print(f"Difference:      {abs(gpu_dot - cpu_dot):.6f}")
```

**You should see something like:**
```
GPU dot product: 249925.5625
CPU dot product: 249925.4219
Difference:      0.140625
```

The small difference is normal — floating point operations in a different order give slightly different results. The values should be very close but not identical.

> **Why `cuda.syncthreads()` inside the loop?** Without it, a fast thread could read a value that a slow thread hasn't written yet, producing a wrong answer. The sync ensures every thread has completed its write before anyone starts reading.

---

## Part 3: The Communication Bottleneck

You saw in Part 1 that the GPU was slower than the CPU for SAXPY once data transfer was included. But that was for N=10 million. What happens at different sizes?

This cell runs SAXPY at many array sizes and plots the results. It will take about a minute to complete.

Copy this into a new cell and run it:

```python
import warnings
import numba.core.errors
warnings.filterwarnings('ignore', category=numba.core.errors.NumbaPerformanceWarning)

import matplotlib.pyplot as plt
import numpy as np
import time
from numba import cuda

@cuda.jit
def saxpy_kernel_plot(a, x, y, out):
    i = cuda.blockIdx.x * cuda.blockDim.x + cuda.threadIdx.x
    if i < x.shape[0]:
        out[i] = a * x[i] + y[i]

a = 2.5
sizes = [1_000, 10_000, 100_000, 1_000_000, 5_000_000, 20_000_000, 100_000_000]
cpu_times    = []
gpu_kernel   = []
gpu_e2e      = []

for n in sizes:
    x_t = np.random.rand(n).astype(np.float32)
    y_t = np.random.rand(n).astype(np.float32)
    tpb = 256
    bpg = (n + tpb - 1) // tpb

    # --- CPU ---
    t0 = time.perf_counter()
    _ = a * x_t + y_t
    cpu_times.append(time.perf_counter() - t0)

    # --- GPU kernel only (data already on device) ---
    d_xt  = cuda.to_device(x_t)
    d_yt  = cuda.to_device(y_t)
    d_out = cuda.device_array(n, dtype=np.float32)
    saxpy_kernel_plot[bpg, tpb](a, d_xt, d_yt, d_out)  # warm up
    cuda.synchronize()
    t0 = time.perf_counter()
    saxpy_kernel_plot[bpg, tpb](a, d_xt, d_yt, d_out)
    cuda.synchronize()
    gpu_kernel.append(time.perf_counter() - t0)

    # --- GPU end-to-end (includes transfer) ---
    t0 = time.perf_counter()
    dx = cuda.to_device(x_t)
    dy = cuda.to_device(y_t)
    dout = cuda.device_array(n, dtype=np.float32)
    saxpy_kernel_plot[bpg, tpb](a, dx, dy, dout)
    cuda.synchronize()
    _ = dout.copy_to_host()
    gpu_e2e.append(time.perf_counter() - t0)

    print(f"N={n:>12,}  CPU={cpu_times[-1]*1000:7.2f}ms  "
          f"GPU kernel={gpu_kernel[-1]*1000:7.2f}ms  "
          f"GPU e2e={gpu_e2e[-1]*1000:7.2f}ms")

plt.figure(figsize=(9, 5))
plt.loglog(sizes, cpu_times,  'o-',  label='CPU (NumPy)')
plt.loglog(sizes, gpu_kernel, 's--', label='GPU kernel only')
plt.loglog(sizes, gpu_e2e,    '^:',  label='GPU end-to-end (with transfer)')
plt.xlabel('Array size (number of elements)')
plt.ylabel('Time (seconds)')
plt.title('SAXPY: CPU vs GPU across problem sizes')
plt.legend()
plt.grid(True, which='both', alpha=0.3)
plt.tight_layout()
plt.savefig('saxpy_scaling.png', dpi=120)
plt.show()
print("Plot saved as saxpy_scaling.png")
```

**You should see a table like:**
```
N=       1,000  CPU=   0.02ms  GPU kernel=   0.15ms  GPU e2e=   1.01ms
N=      10,000  CPU=   0.03ms  GPU kernel=   0.10ms  GPU e2e=   0.95ms
N=     100,000  CPU=   0.15ms  GPU kernel=   0.10ms  GPU e2e=   1.87ms
N=   1,000,000  CPU=   1.21ms  GPU kernel=   0.20ms  GPU e2e=   6.24ms
N=   5,000,000  CPU=   9.83ms  GPU kernel=   0.57ms  GPU e2e=  19.37ms
N=  20,000,000  CPU=  38.89ms  GPU kernel=   1.97ms  GPU e2e=  69.98ms
N= 100,000,000  CPU= 194.86ms  GPU kernel=   9.48ms  GPU e2e= 327.77ms
```

And a plot with three lines. Look at it and notice:
- The **GPU kernel** line is always below the CPU line — the kernel itself is always faster.
- The **GPU end-to-end** line has a flat floor at small sizes — that floor is the fixed cost of data transfer.
- There is a **crossover point** (around 20 million elements) where end-to-end GPU finally beats CPU.

---

## Part 4: Matrix Multiply with PyTorch

SAXPY is memory-bound — each element needs one multiply and one add. For truly dramatic speedups, you need compute-bound work: operations where each output element requires many floating point operations.

Matrix multiplication is the canonical example. Computing one element of C = A @ B requires K multiply-and-add operations. For large matrices, the GPU has so much work to do that the transfer cost is negligible.

This is directly relevant to ML — every linear layer in a neural network is a matrix multiply.

Copy this into a new cell and run it:

```python
import torch

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print("PyTorch is using:", device)

if device.type == 'cpu':
    print("WARNING: PyTorch did not find a GPU. Check your environment.")
```

**You should see:**
```
PyTorch is using: cuda
```

Now run the benchmark. Copy this into a new cell and run it:

```python
M, K, N = 4096, 4096, 4096

A = torch.randn(M, K)
B = torch.randn(K, N)

# CPU timing
t0 = time.perf_counter()
C_cpu = torch.mm(A, B)
cpu_matmul = time.perf_counter() - t0

# Move to GPU
A_gpu = A.to(device)
B_gpu = B.to(device)

# Warm up
_ = torch.mm(A_gpu, B_gpu)
torch.cuda.synchronize()

# GPU timing
t0 = time.perf_counter()
C_gpu = torch.mm(A_gpu, B_gpu)
torch.cuda.synchronize()
gpu_matmul = time.perf_counter() - t0

print(f"\nMatrix size: {M} x {K} @ {K} x {N}")
print(f"CPU matmul:  {cpu_matmul * 1000:.1f} ms")
print(f"GPU matmul:  {gpu_matmul * 1000:.1f} ms")
print(f"Speedup:     {cpu_matmul / gpu_matmul:.1f}x")
```

**You should see something like:**
```
Matrix size: 4096 x 4096 @ 4096 x 4096
CPU matmul:  563.4 ms
GPU matmul:  45.9 ms
Speedup:     12.3x
```

Compare this to the SAXPY speedup. Matrix multiply is compute-heavy — the GPU has thousands of cores all multiplying simultaneously, and the computation-to-transfer ratio is much more favorable.

---

## Part 5: Check GPU Utilization with nvidia-smi

You can verify the GPU is being used by checking its utilization.

1. Open a new terminal tab (File → New → Terminal).
2. Run:

```bash
nvidia-smi
```

**You should see something like:**
```
+-----------------------------------------------------------------------------------------+
| GPU  Name                 Persistence-M | Bus-Id          Disp.A | Volatile Uncorr. ECC |
|=========================================+========================+======================|
|   0  Tesla T4                       On  |   00000000:00:04.0 Off |                    0 |
| N/A   51C    P0             27W /   70W |     457MiB /  15360MiB |      0%      Default |
+-----------------------------------------+------------------------+----------------------+
```

The key fields:
- **GPU Name:** Tesla T4 — this is your GPU.
- **Memory-Usage:** How much of the 15 GB GPU memory your notebook is using.
- **GPU-Util:** Will show 80–100% while a kernel is actively running.

---

## Save Your Work

Before ending your session, save your notebook to scratch storage:

1. Open a terminal tab.
2. Run:

```bash
cp ~/your_notebook_name.ipynb /scratch/$USER/
```

Replace `your_notebook_name.ipynb` with the actual name of your notebook file.

---

## Report Questions

Answer these in `REPORT.md` and push your notebook and report to your GitHub repo.

**Q1.** What GPU did you get? How many multiprocessors does it have?

**Q2.** From Part 1: what was your CPU time and GPU kernel-only time for SAXPY at N=10 million? What was the speedup?

**Q3.** From Part 1: once you included the data transfer (end-to-end), was the GPU faster or slower than CPU? Why?

**Q4.** From Part 3: at approximately what array size did the GPU end-to-end first beat the CPU? What does this tell you about when you should use a GPU?

**Q5.** From Part 4: what speedup did you observe for matrix multiply? Why is this larger than the SAXPY speedup?

**Q6.** In Part 2, `cuda.syncthreads()` is called inside the reduction loop, not just once before it. What would go wrong if you removed it?

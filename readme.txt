# Bitcoin Mining Distributed Simulation Project

## Overview
This project simulates distributed Bitcoin mining strategies using three algorithms to optimize nonce range allocation among a network of miners. The goal is to evaluate efficiency in terms of block discovery time, total hashes performed, idle time, and communication overhead, without modifying blockchain consensus.

### Algorithms Implemented
1. **Static Partition (Algo 1)**: Baseline - Fixed nonce ranges assigned equally at startup. Minimal communication.
2. **Dynamic Work Stealing (Algo 2)**: Decentralized - Idle miners request sub-ranges from peers via work stealing.
3. **Pool-based Scheduling (Algo 3)**: Centralized - A coordinator assigns small work units to miners on request, simulating Stratum protocol.

The simulation uses:
- **C++17** for miner processes, threading (pthreads), and inter-process communication (ZeroMQ).
- **Python 3** for SHA-256 double-hashing and leading zero checks (via `hashlib`).

Metrics are logged to CSV files for analysis (e.g., plotting with Python/Matplotlib).

**Course Context**: CS5320 Distributed Computing, Autumn 2025 - Option 1 (Implementation and Comparison).

**Authors**:
- Bolla Lokesh Reddy (CS23BTECH11011)
- Muvvala Sairam Suhas (CS23BTECH11038)


## Prerequisites
- **OS**: Ubuntu 22.04 (or compatible Linux with POSIX threads).
- **Compiler**: g++ (GCC 9+ for C++17 support).
- **Libraries**:
  - ZeroMQ (libzmq3-dev) for message passing.
  - pthread (included in glibc).
- **Python**: 3.8+ with `hashlib` (standard library).
- **Build Tools**: Make (optional, but used here).
- **Hardware**: Multi-core CPU (e.g., 8-core) for simulating multiple miners.

No external pip installs required for Python (uses built-ins).

## Installation and Setup
1. **Install Dependencies** (on Ubuntu/Debian):
   ```
   sudo apt update
   sudo apt install libzmq3-dev build-essential python3
   ```

2. **Clone/Download Project Files**:
   - Extract the project zip (`AllDocs_DCPrjt-CS23BTECH11011.zip`).
   - Navigate to the source directory: `cd Src_Prjt-CS23BTECH11011/`.

3. **Build the C++ Executable**:
   ```
   make clean  # Optional: Clean previous builds
   make        # Builds mining_sim executable
   ```
   - Makefile targets:
     - `all`: Builds `mining_sim`.
     - `clean`: Removes object files and executable.
   - Manual build (if no Make):
     ```
     g++ -std=c++17 -pthread -lzmq -O2 -o mining_sim *.cpp
     ```

4. **Verify Python Hasher**:
   - Test: `python3 hasher.py sample_header.bin 0 100 18` (should output hashes or -1).

## Project Structure
```
Src_Prjt-CS23BTECH11011/
├── Makefile                  # Build script
├── mining_sim                # Compiled executable (after build)
├── miner.cpp                 # Core miner logic (threads, hashing calls)
├── coordinator.cpp           # Pool coordinator for Algo 3
├── zmq_utils.h               # ZeroMQ helpers
├── config.json               # Sample config (see below)
├── hasher.py                 # Python hashing script
├── analyze.py                # Python script for plotting CSV results
├── sample_config.json        # Sample input file
├── README.txt                # This file
├── results/                  # Output directory (created on run)
│   ├── logs/                 # CSV logs per run
│   └── plots/                # Generated plots (via analyze.py)
└── report/                   # Rpt_Prjt-CS23BTECH11011.pdf (separate submission)
```

## Configuration
The simulation is configured via a JSON file (e.g., `config.json`). Key parameters:

```json
{
  "num_miners": 4,
  "algo": 1,          // 1: Static, 2: Work Stealing, 3: Pool
  "nonce_space": 4294967296,  // 2^32
  "difficulty": 18,   // Leading zeros target (adjust for ~60s expected time)
  "mean_hash_rate": 1000,     // Hashes/sec per miner
  "speed_variance": 0.25,     // Std dev as fraction of mean (heterogeneity)
  "network_delay_ms": 25,     // Avg delay per message (uniform 0-50ms)
  "work_unit_size": 1048576,  // 2^20 nonces for Algo 3
  "idle_threshold_s": 1.0,    // Time before stealing in Algo 2
  "num_trials": 100,          // Runs for averaging
  "output_dir": "results/",
  "block_header": "sample_header.bin"  // Binary file with block header (4KB placeholder)
}
```

- **Sample Input File**: Use `sample_config.json` (included) for quick tests. It sets `algo=1`, `num_miners=4`, low difficulty for fast runs.
- **Block Header**: Create `sample_header.bin` as a 80-byte binary (Bitcoin block header placeholder). For testing:
  ```
  echo -n "00000000000000000000000000000000000000000000000000000000000000000000000000" | xxd -r -p > sample_header.bin
  ```
  (All zeros for simplicity; nonce appended as 4 bytes.)

## Usage
### Running Simulations
1. **Single Run**:
   ```
   ./mining_sim -c config.json
   ```
   - Flags:
     - `-c <file>`: Config JSON (default: `config.json`).
     - `-a <1|2|3>`: Override algo (default: from config).
     - `-t <trials>`: Override num_trials.
     - `-v`: Verbose logging.
   - Launches `num_miners` processes + coordinator (if Algo 3).
   - Output: CSV logs in `results/logs/algo<1-3>_trial<N>.csv` with columns: timestamp, miner_id, hashes, idle_s, messages, etc.

2. **Batch Runs** (All Algos):
   ```
   for algo in {1..3}; do
     sed "s/\"algo\": .*/\"algo\": $algo,/" config.json > temp.json
     ./mining_sim -c temp.json -t 10
   done
   ```
   - Run 10 trials per algo.

3. **Expected Runtime**:
   - Single trial: 1-5 minutes (depending on difficulty/cores).
   - Full 100 trials x 3 algos: ~2-3 hours on 8-core machine.

### Analyzing Results
1. **Generate Plots**:
   ```
   python3 analyze.py results/logs/
   ```
   - Outputs: `results/plots/` with Matplotlib plots (block_time_vs_delay.png, etc.).
   - Customize: Edit `analyze.py` for new metrics.

2. **Sample Output CSV Snippet** (algo1_trial1.csv):
   ```
   trial_id,algo,block_time_s,total_hashes,idle_time_pct,messages,nonce_found
   1,1,62.3,249000000,18.2,1,12345678
   ```

### Troubleshooting
- **ZeroMQ Errors**: Ensure `libzmq` installed; check ports (uses 5555-5560).
- **Python Subprocess Fails**: Verify `hasher.py` executable (`chmod +x hasher.py`).
- **Slow Hashes**: Lower `difficulty` (e.g., 12) for testing; real Bitcoin is ~40+.
- **Heterogeneous Speeds**: Simulated via sleeps; for real variance, run on multi-machine setup (extend ZMQ endpoints).
- **Memory**: Nonce space tracked efficiently (bitmaps for pool); scales to 2^32.

## Sample Input File
Copy `sample_config.json`:
```json
{
  "num_miners": 4,
  "algo": 1,
  "nonce_space": 4294967296,
  "difficulty": 12,
  "mean_hash_rate": 500,
  "speed_variance": 0.2,
  "network_delay_ms": 0,
  "work_unit_size": 1048576,
  "idle_threshold_s": 1.0,
  "num_trials": 5,
  "output_dir": "results/",
  "block_header": "sample_header.bin"
}
```
Run: `./mining_sim -c sample_config.json`

## Limitations and Extensions
- **Simplifications**: No real network (local ZMQ); fixed header (no mid-block updates).
- **Extensions**: Add fault tolerance (miner crashes), scale to 100+ miners, integrate GPU hashing.
- **License**: Academic use only.

For issues, contact authors or refer to report `Rpt_Prjt-CS23BTECH11011.pdf`.

---  

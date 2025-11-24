=============================
 Distributed Mining Project
=============================

Folder Structure
----------------
Distributed/KL
│
├── Makefile
├── readme.txt
└── src
    ├── cpu
    │   ├── static_partition.cpp
    │   ├── dynamic_stealing.cpp
    │   ├── static_partition      (binary after compile)
    │   ├── dynamic_stealing      (binary after compile)
    │   └── logs/                 (output logs)
    │
    ├── mpi
    │   ├── mpi_mining.cpp
    │   ├── mpi_mining            (binary after compile)
    │   ├── mining_utils.hpp
    │   ├── file_io.hpp
    │   ├── raft_state.hpp
    │   ├── work_manager.hpp
    │   ├── hosts.txt
    │   └── checkpoints/          (auto-generated)
    │
    └── transactions.txt


========================================
 Compilation Commands
========================================

Static Partition Miner
----------------------
cd src/cpu
g++ static_partition.cpp -o static_partition -std=c++17 -pthread -lssl -lcrypto -O2

Dynamic Work-Stealing Miner
---------------------------
cd src/cpu
g++ dynamic_stealing.cpp -o dynamic_stealing -std=c++17 -pthread -lssl -lcrypto -O2

MPI Distributed Miner
---------------------
cd src/mpi
mpic++ mpi_mining.cpp -o mpi_mining -std=c++17 -lssl -lcrypto -O2 -pthread


========================================
 Running Commands
========================================

Static Partition Miner
----------------------
./static_partition <threads> <difficulty> <total_nonces>

Example:
./static_partition 4 5 50000000

Output Printed:
- Time (ms)
- Winning nonce
- Hash
- Logs stored in src/cpu/logs/


Dynamic Work-Stealing Miner
---------------------------
./dynamic_stealing <threads> <difficulty> <total_nonces> <chunk_size>

Example:
./dynamic_stealing 8 5 5000000 20000

Output Printed:
- Time (ms)
- Winning nonce
- Hash
- Logs stored in src/cpu/logs/
- dynamic_summary.csv generated


MPI Distributed Miner
---------------------
mpirun -np <nodes> -hostfile hosts.txt ./mpi_mining <difficulty>

Example:
mpirun -np 5 -hostfile hosts.txt ./mpi_mining 5

Output Printed:
- Time (seconds)
- Winning nonce
- Hash
- Rank that found solution
- Hashes by each worker

Output Files:
- logs/rank_<id>.log
- logs/final_result.txt
- checkpoints/global_work_state_term_*.state


========================================
 Output Files Summary
========================================

CPU Static Miner:
-----------------
src/cpu/logs/static_summary.log
src/cpu/logs/final_result.txt
src/cpu/logs/miner_<id>.log

CPU Dynamic Miner:
------------------
src/cpu/logs/global.log
src/cpu/logs/dynamic_summary.csv
src/cpu/logs/miner_<id>.log
src/cpu/logs/final_result.txt

MPI Miner:
----------
src/mpi/logs/rank_<id>.log
src/mpi/logs/final_result.txt
src/mpi/checkpoints/global_work_state_term_*.state


========================================
 End of README
========================================

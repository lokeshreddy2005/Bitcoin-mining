#!/bin/bash

# sync_and_compile.sh - Sync source code to VMs and compile

# List of VMs
VMS=(
    "10.96.0.130"
    "10.96.0.69"
    "10.96.1.46"
)

REMOTE_BASE="/home/ubuntu/src"
REMOTE_MPI="$REMOTE_BASE/mpi"

echo "=========================================="
echo "Syncing source code to cluster VMs"
echo "=========================================="

# Source files to sync
FILES=(
    "mpi_mining.cpp"
    "mining_utils.hpp"
    "raft_state.hpp"
    "file_io.hpp"
    "work_manager.hpp"
    "hosts.txt"
)

# Deploy to each VM
for vm in "${VMS[@]}"; do
    echo ""
    echo "Syncing to ubuntu@$vm..."
    
    # Backup and remove old mpi directory contents (keep logs/checkpoints)
    ssh ubuntu@$vm "cd $REMOTE_MPI && rm -f *.cpp *.hpp *.txt mpi_mining 2>/dev/null"
    
    # Copy all source files
    for file in "${FILES[@]}"; do
        if [ -f "$file" ]; then
            scp "$file" ubuntu@$vm:$REMOTE_MPI/
            if [ $? -eq 0 ]; then
                echo "  ✓ Copied $file"
            else
                echo "  ✗ Failed to copy $file"
            fi
        else
            echo "  ⚠ Warning: $file not found locally"
        fi
    done
    
    # Compile on remote VM
    echo ""
    echo "  Compiling on $vm..."
    ssh ubuntu@$vm "cd $REMOTE_MPI && mpic++ mpi_mining.cpp -o mpi_mining -std=c++17 -lssl -lcrypto -O2 -pthread 2>&1"
    
    if [ $? -eq 0 ]; then
        echo "  ✓ Compilation successful on $vm"
    else
        echo "  ✗ Compilation failed on $vm"
    fi
done

echo ""
echo "=========================================="
echo "Sync and compile complete!"
echo "=========================================="
echo ""
echo "Directory structure on VMs:"
echo "  ~/src/mpi/          (your updated code)"
echo "  ~/src/transactions.txt  (unchanged)"
echo ""
echo "Now run from any VM:"
echo "  ssh ubuntu@10.96.0.69"
echo "  cd ~/src/mpi"
echo "  mpirun -np 4 -hostfile hosts.txt /home/ubuntu/src/mpi/mpi_mining 2"

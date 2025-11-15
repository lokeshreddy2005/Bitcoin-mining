#!/bin/bash
set -e


# Build and run one trial
make


echo "Running static partition trial..."
./build/static_partition


echo "Logs written to data/logs/"
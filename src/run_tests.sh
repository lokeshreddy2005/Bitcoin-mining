#!/bin/bash

echo "Running multiple tests..."

for i in {1..10}; do
    echo "Test $i"
    ./static_partition > run_$i.out
done

echo "Done."

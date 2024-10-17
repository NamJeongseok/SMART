#! /bin/bash

computeNR=$1
memoryNR=$2

if [ ! -d "./SMART/build" ]; then
	mkdir ./SMART/build
fi

# Compile
cd ./SMART/build
cmake .. && make -j

if [ ! -d "../test/result" ]; then
  mkdir "../test/result"
fi

# Set HugePage
../script/hugepage_memory.sh

# Start benchmark
./benchmark_server ${computeNR} ${memoryNR}

# Clear HugePage
../script/clear_hugepage.sh
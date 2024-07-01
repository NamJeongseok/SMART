#! /bin/bash

computeNR=1
memoryNR=1

if [ ! -d "../build" ]; then
	mkdir ../build
fi

# Compile
cd ../build
cmake .. && make -j

if [ ! -d "../test/result" ]; then
  mkdir "../test/result"
fi

# Set HugePage
../script/hugepage_memory.sh
../script/restartMemc.sh

# Start benchmark
./benchmark_server ${computeNR} ${memoryNR}

# Clear HugePage
../script/clear_hugepage.sh
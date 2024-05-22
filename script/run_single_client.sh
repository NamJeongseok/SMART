#! /bin/bash

DEBUG=0
memoryNR=1
ycsb=1
workloadPath=_test

if [ ! -f ${workloadPath} ]; then
  echo "[ERROR] No workload named ${workloadPath}"
  exit 1;  
fi

if [ ! -d "../build" ]; then
	mkdir ../build
fi

# Compile
cd ../build
cmake -DDEBUG=${DEBUG} .. && make -j

if [ ! -d "../test/result" ]; then
  mkdir "../test/result"
fi

numKeys=$(cat ${workloadPath} | wc -l)

# Set HugePage
../script/hugepage.sh

# Start benchmark
./benchmark_single_client ${ycsb} ${memoryNR} ${workloadPath} ${numKeys}

# Clear HugePage
../script/clear_hugepage.sh

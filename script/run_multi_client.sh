#! /bin/bash

computeNR=1
memoryNR=2
threadNum=64
workloadPath=/mnt/data/review-82M-v2.csv

if [ ! -f ${workloadPath} ]; then
  echo "[ERROR] No workload named ${workloadPath}"
  exit 1;  
fi

if [ ! -d "../build" ]; then
	mkdir ../build
fi

# Compile
cd ../build
cmake .. && make -j

if [ ! -d "../test/result" ]; then
  mkdir "../test/result"
fi

numKeys=$(cat ${workloadPath} | wc -l)

# Set HugePage
../script/hugepage.sh

# Start benchmark
./benchmark_multi_client ${computeNR} ${memoryNR} ${threadNum} ${workloadPath} ${numKeys}

# Clear HugePage
../script/clear_hugepage.sh

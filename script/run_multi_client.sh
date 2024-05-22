#! /bin/bash

isYCSB=1
computeNR=1
memoryNR=1
threadNum=4
# YCSB workload name (e.g., a, b, c or d) if YCSB benchmark, 
# else {path}/{filename} for key-only workloads. 
workloadPath=a

if [ ! -f ${workloadPath} ] & [ ! isYCSB ]; then
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

if [ isYCSB ]; then
  numKeys=0
else
  numKeys=$(cat ${workloadPath} | wc -l)
fi

# Set HugePage
../script/hugepage.sh

# Start benchmark
./benchmark_multi_client ${computeNR} ${memoryNR} ${threadNum} ${workloadPath} ${numKeys} ${isYCSB}

# Clear HugePage
../script/clear_hugepage.sh

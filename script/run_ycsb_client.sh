#!/bin/bash

computeNR=1
memoryNR=2
threadNum=64
# Path to the directory where YCSB workload exists
workloadDir=/mnt/data/ycsb_workloads
# YCSB workload name (e.g., a, b, c or d)
workloadName=a

loadWorkloadPath=${workloadDir}/load_randint_workload${workloadName}
txnWorkloadPath=${workloadDir}/txn_randint_workload${workloadName}

if [ ! -f ${loadWorkloadPath} ]; then
  echo "[ERROR] YCSB load workload (${loadWorkloadPath}) missing"
  exit 1;  
fi

if [ ! -f ${txnWorkloadPath} ]; then
  echo "[ERROR] YCSB txn workload (${txnWorkloadPath}) missing"
  exit 1;  
fi

if [ ! -d "../build" ]; then
  mkdir "../build"
fi

# Compile
cd ../build
cmake .. && make -j

if [ ! -d "../test/result" ]; then
  mkdir "../test/result"
fi

loadNumKeys=$(cat ${loadWorkloadPath} | wc -l)
txnNumKeys=$(cat ${txnWorkloadPath} | wc -l)

# Set HugePage
../script/hugepage.sh

# Start benchmark
./benchmark_ycsb_client ${computeNR} ${memoryNR} ${threadNum} ${workloadDir} ${workloadName} ${loadNumKeys} ${txnNumKeys}

# Clear HugePage
../script/clear_hugepage.sh
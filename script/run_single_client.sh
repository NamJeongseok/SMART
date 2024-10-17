#! /bin/bash

DEBUG=$1
computeNR=$2
memoryNR=$3
threadNum=$4
cacheSize=$5 # MB
workloadDir=$6 # Path to the directory where workload exists
workloadName=$7 # Workload name


workloadPath=${workloadDir}/${workloadName}

if [ ! -f ${workloadPath} ]; then
  echo "[ERROR] No workload named ${workloadPath}"
  exit 1;  
fi

if [ ! -d "./SMART/build" ]; then
	mkdir ./SMART/build
fi

# Compile
cd ./SMART/build
cmake -DDEBUG=${DEBUG} .. && make -j

if [ ! -d "../test/result" ]; then
  mkdir "../test/result"
fi

numKeys=$(cat ${workloadPath} | wc -l)

# Set HugePage
../script/hugepage_compute.sh

# Start benchmark
./benchmark_single_client ${memoryNR} ${workloadPath} ${numKeys} ${cacheSize}

# Clear HugePage
../script/clear_hugepage.sh

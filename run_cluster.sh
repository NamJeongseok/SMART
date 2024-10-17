#!/bin/bash

######################################################################################
# SSH Information
USER_ID=root
USER_PASSWD=1234
REPO_PATH=/home/root

# Compile options
DEBUG=0

# Execution parameters
PORT=1234
CLIENT_IPS=("12.34.56.78" "87.65.43.21")
SERVER_IPS=("12.34.56.78" "87.65.43.21")

THREAD=(64)
CACHE_SIZE=(64 1024) # MB
SCRIPT_NAME=run_multi_client.sh
WORKLOAD_DIR=/mnt/ssd/data
WORKLOAD_NAME=("transposed_review_228M_8bit.csv" "transposed_map_m_8bit.csv")
######################################################################################

computeNR=${#SERVER_IPS[@]}
memoryNR=${#CLIENT_IPS[@]}
script_path=${REPO_PATH}/SMART/script

echo "[CAUTION] SSH public key registration is essential for successful connection"

for thread in "${THREAD[@]}"
do
  for cache_size in "${CACHE_SIZE[@]}"
  do
    for workload_name in "${WORKLOAD_NAME[@]}"
    do
      echo "=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*"
      echo "[NOTICE] Running with following parameters"
      echo "         CLIENT_IPS=(${CLIENT_IPS[*]})"
      echo "         SERVER_IPS=(${SERVER_IPS[*]})"
      echo "         DEBUG=${DEBUG}"
      echo "         THREAD=${thread}, CACHE_SIZE=${cache_size}"
      echo "         SCRIPT_NAME=${SCRIPT_NAME}, WORKLOAD=${WORKLOAD_DIR}/${workload_name}"

      echo "[NOTICE] Kill old instances"
      for ip in "${SERVER_IPS[@]}"
      do
        kill_command="sudo pkill -9 -f run_server\.sh"
        echo "${USER_PASSWD}" | ssh -tt -p ${PORT} ${USER_ID}@${ip} "(${kill_command})" > /dev/null 2>&1
      done
      for ip in "${CLIENT_IPS[@]}"
      do
        kill_command="sudo pkill -9 -f run_.*_client\.sh"
        echo "${USER_PASSWD}" | ssh -tt -p ${PORT} ${USER_ID}@${ip} "(${kill_command})" > /dev/null 2>&1
      done

      for ((i = 0; i < ${#SERVER_IPS[@]}; ++i));
      do
        if [ ${i} = 0 ]; then     
          echo "[NOTICE] Start memcached server at IP address ${SERVER_IPS[${i}]}"
          run_command="sudo ${script_path}/restartMemc.sh"
          echo "${USER_PASSWD}" | ssh -tt -p ${PORT} ${USER_ID}@${SERVER_IPS[${i}]} "(${run_command})" > /dev/null 2>&1
        fi     

        echo "[NOTICE] Start memory server at IP address ${SERVER_IPS[${i}]}"
        run_command="sudo ${script_path}/run_memory.sh ${computeNR} ${memoryNR}"
        echo "${USER_PASSWD}" | ssh -tt -p ${PORT} ${USER_ID}@${SERVER_IPS[${i}]} "(${run_command})" > /dev/null 2>&1 &
      done

      sleep 3

      for ((i = 0; i < ${#CLIENT_IPS[@]}; ++i));
      do
        echo "[NOTICE] Start client server at IP address ${CLIENT_IPS[${i}]}"
        run_command="sudo ${script_path}/${SCRIPT_NAME} ${DEBUG} ${computeNR} ${memoryNR} ${thread} ${cache_size} ${WORKLOAD_DIR} ${workload_name}"

        if [ ${i} = `expr ${#CLIENT_IPS[@]} - 1` ]; then
          echo "${USER_PASSWD}" | ssh -tt -p ${PORT} ${USER_ID}@${CLIENT_IPS[${i}]} "(${run_command})"
        else
          echo "${USER_PASSWD}" | ssh -tt -p ${PORT} ${USER_ID}@${CLIENT_IPS[${i}]} "(${run_command})" > /dev/null 2>&1 &
        fi
      done

      sleep 10
      echo -e "=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*\n"
    done
  done
done
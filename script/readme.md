# Client-Server Separated Benchmark
## Before run
1. Modify `memcached.conf` file to contain correct `ip` and `port` to point your designated `memcached` server.
```sh
<one>.<memcached>.<server>.<ip>
<port>
```

2. Modify `script/hugepage.sh` to set appropriate num. of hugepages per each node.

2. Modify environment variables for your experiment environment.
at `include/Common.h`:
```c++
22  #define MEMORY_NODE_NUM <your_mem_node_num>
23  #define CPU_PHYSICAL_CORE_NUM <server_physical_core_num>
24  #define MAX_CORO_NUM <coro_num, default=1>

...

50 #define MAX_APP_THREAD   <thread_num + 1> // Number of client thread num + 1

...

93 constexpr uint64_t dsmSize = <MN_size>; // Memory Node's shared memory region.
                                           // Note: You don't need this at client
                                           //       side, so remain as small as possible
                                           //       at your client node.

...

102 constexpr int kIndexCacheSize = 600    // Increase here, if you want to run
                                           // big dataset.
```


at `script/run_multi_client.sh`:
```sh
computeNR=<your_compute_node_num>
memoryNR=<your_memory_node_num>
threadNum=<thread_num_per_client>
workloadPath=<your_workload_path>
```

## Run experiment
Restart `memcached` at your memcached server. (Only one server is needed.)
```sh
cd script/
sudo ./restartMemc.sh
```
After that, run script appropriate for each node's role.

At client nodes,
```bash
sudo ./run_multi_client.sh # run multi client benchmark
# or, to run single client benchmark
sudo ./run_single_client.sh
```

At memory nodes,
```bash
sudo ./run_server.sh
```

## Known issues
1. `can't bind core!` error can be occured when you want to run threads more than your physical core, but you can ignore it.
2. `dsm->barrier("...");` function might not work propery at some old servers -- comment out might useful.
3. The variables `maxCompute`, `maxServer` that is used for condition check at `while` loop at `keeper.cpp:113` and `keeper.cpp:132`, replace it into constant `1` might helpful when you stuck here.
e.g., `curServer < 1`.

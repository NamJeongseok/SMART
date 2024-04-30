#include "Timer.h"
#include "Tree.h"
#include "log_writer.h"

#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <thread>
#include <vector>
#include <string>
#include <fstream>

//////////////////// workload parameters /////////////////////
std::vector<uint64_t> keys;
//////////////////// workload parameters /////////////////////

DSM *dsm;
Tree *tree;

extern uint64_t rdma_write_num;
extern uint64_t rdma_write_size;
extern uint64_t rdma_read_num;
extern uint64_t rdma_read_size;
extern uint64_t rdma_atomic_num;
extern uint64_t rdma_atomic_size;
extern uint64_t cache_miss[MAX_APP_THREAD][8];
extern uint64_t cache_hit[MAX_APP_THREAD][8];

bool skip_BOM(ifstream& in) {
  char test[4] = {0};

  in.read(test, 3);
  if (strcmp(test, "\xEF\xBB\xBF") == 0) {
    return true;
  } 
  in.seekg(0);

  return false;
}

int main(int argc, char *argv[]) {
  if (argc != 4) {
    fprintf(stderr, "[ERROR] Three arguments are required but received %d\n", argc - 1);
    exit(1);
  }

  DSMConfig config;
  config.isCompute = true;
  config.memoryNR = 1;
  config.computeNR = atoi(argv[1]);

  std::string workloadPath = argv[2];
  uint64_t numKeys = atol(argv[3]);

  /* Start reading keys */
  std::ifstream ifs;
  ifs.open(workloadPath);

  if (skip_BOM(ifs)) {
    printf("[NOTICE] Removed BOM in target workload\n");
  }

  fprintf(stdout, "[NOTICE] Start reading %lu keys\n", numKeys);
  keys.reserve(numKeys);
  for (uint64_t i = 0; i < numKeys; ++i) {
    ifs >> keys[i];
  }

  LogWriter* lw = new LogWriter("COMPUTE");
  // lw->LOG_client_info("Single client", 1, workloadPath, numKeys);
#ifndef PRIVATE_DEBUG
  fprintf(stdout, "[NOTICE] Start single client benchmark\n");
#else
  fprintf(stdout, "[NOTICE] Start single client benchmark (DEBUG mode)\n");
#endif
  dsm = DSM::getInstance(config);
  dsm->registerThread();
  tree = new Tree(dsm);

  dsm->barrier("benchmark");

  printf("[NOTICE] Start insert\n");
  struct timespec insert_start, insert_end;
  clock_gettime(CLOCK_REALTIME, &insert_start);
  for (uint64_t i = 0; i < numKeys; ++i) {
    tree->insert(int2key(keys[i]), reinterpret_cast<Value>(keys[i]));
  } 
  clock_gettime(CLOCK_REALTIME, &insert_end);

  uint64_t insert_time = (insert_end.tv_sec - insert_start.tv_sec) * 1000000000 + (insert_end.tv_nsec - insert_start.tv_nsec);
  lw->LOG("Average insert latency(nsec/op): %.3e", (double)insert_time/(double)numKeys);

  fprintf(stdout, "[NOTICE] Start search\n");
  Value v;
  uint64_t found_keys = 0;
  struct timespec search_start, search_end;
  clock_gettime(CLOCK_REALTIME, &search_start);
  for (uint64_t i = 0; i < numKeys; ++i) {
    auto ret = tree->search(int2key(keys[i]), v);
    if (ret && v == reinterpret_cast<Value>(keys[i])) {
      found_keys++;
    }
  }
  clock_gettime(CLOCK_REALTIME, &search_end);

  uint64_t search_time = (search_end.tv_sec - search_start.tv_sec) * 1000000000 + (search_end.tv_nsec - search_start.tv_nsec);
  lw->LOG("Average search latency(nsec/op): %.3e (%lu/%lu found)", (double)search_time/(double)numKeys, found_keys, numKeys);

  dsm->set_key("metric", "LATENCY");
  dsm->set_key("insert_keys", numKeys);
  dsm->set_key("inserted_keys", numKeys);
  dsm->set_key("insert_time", insert_time);
  dsm->set_key("search_keys", numKeys);  
  dsm->set_key("searched_keys", found_keys);   
  dsm->set_key("search_time", search_time);  

#ifdef PRIVATE_DEBUG
  fprintf(stdout, "==================== Execution Information ====================\n");   
  fprintf(stdout, "Total RDMA write request number : %lu\n", rdma_write_num);
  fprintf(stdout, "Total RDMA write request size(B): %lu\n", rdma_write_size);
  fprintf(stdout, "Total RDMA read request number : %lu\n", rdma_read_num);
  fprintf(stdout, "Total RDMA read request size(B): %lu\n", rdma_read_size);
  fprintf(stdout, "Total RDMA atomic request number : %lu\n", rdma_atomic_num);
  fprintf(stdout, "Total RDMA atomic request size(B): %lu\n", rdma_atomic_size);
  fprintf(stdout, "===============================================================\n"); 
#endif

  delete lw;

  return 0;
}
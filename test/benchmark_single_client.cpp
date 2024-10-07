#include "Timer.h"
#include "Tree.h"
#include "log_writer.h"
#include "key_generator.h"

#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <thread>
#include <vector>
#include <string>
#include <fstream>

//////////////////// workload parameters /////////////////////
std::vector<Key> keys;
//////////////////// workload parameters /////////////////////

DSM *dsm;
Tree *tree;

int main(int argc, char *argv[]) {
  if (argc != 4) {
    fprintf(stderr, "[ERROR] Three arguments are required but received %d\n", argc - 1);
    exit(1);
  }

  DSMConfig config;
  config.isCompute = true;
  config.computeNR = 1;
  config.memoryNR = atoi(argv[1]);

  std::string workloadPath = argv[2];
  uint64_t numKeys = atol(argv[3]);

  std::ifstream ifs;
  ifs.open(workloadPath);

  if (!ifs.is_open()) {
    fprintf(stdout, "[ERROR] Failed opening file %s (error: %s)\n", workloadPath.c_str(), strerror(errno));
    exit(1);
  }
  
  if (skip_BOM(ifs)) {
    fprintf(stdout, "[NOTICE] Removed BOM in target workload\n");
  }

  fprintf(stdout, "[NOTICE] Start reading %lu keys\n", numKeys);

  keys.reserve(numKeys);
  uint64_t k;
  for (uint64_t i = 0; i < numKeys; ++i) {
    ifs >> k;
    keys.push_back(int2key(k));
  }

  LogWriter* lw = new LogWriter("COMPUTE");
  lw->print_client_info(1, define::kIndexCacheSize, workloadPath.c_str(), 0, numKeys, numKeys);
  lw->LOG_client_info(1, define::kIndexCacheSize, workloadPath.c_str(), 0, numKeys, numKeys);

  fprintf(stdout, "[NOTICE] Start single client benchmark\n");
  dsm = DSM::getInstance(config);

  fprintf(stdout, "[NOTICE] Start initializing index structure\n");
  dsm->registerThread();
  tree = new Tree(dsm);

  dsm->barrier("benchmark");

  fprintf(stdout, "[NOTICE] Start insert\n");
  struct timespec insert_start, insert_end;
  clock_gettime(CLOCK_REALTIME, &insert_start);
  for (uint64_t i = 0; i < numKeys; ++i) {
    tree->insert(keys[i], (Value)key2int(keys[i]));
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
    auto ret = tree->search(keys[i], v);
    if (ret && v == (Value)key2int(keys[i])) {
      found_keys++;
    }
  }
  clock_gettime(CLOCK_REALTIME, &search_end);

  uint64_t search_time = (search_end.tv_sec - search_start.tv_sec) * 1000000000 + (search_end.tv_nsec - search_start.tv_nsec);
  lw->LOG("Average search latency(nsec/op): %.3e (%lu/%lu found)", (double)search_time/(double)numKeys, found_keys, numKeys);

  lw->LOG_client_cache_info(tree->get_cache_statistics());

  dsm->set_key("metric", "REAL_LATENCY");
  dsm->set_key("thread_num", 1UL);
  dsm->set_key("cache_size", (uint64_t)define::kIndexCacheSize);
  dsm->set_key("bulk_keys", 0UL);
  dsm->set_key("load_workload_path", workloadPath);
  dsm->set_key("txn_workload_path", workloadPath);
  dsm->set_key("load_keys", numKeys);
  dsm->set_key("load_done_keys", numKeys);
  dsm->set_key("load_time", insert_time);
  dsm->set_key("txn_keys", numKeys);
  dsm->set_key("txn_done_keys", found_keys);
  dsm->set_key("txn_time", search_time);

  delete lw;
  delete tree;

  return 0;
}
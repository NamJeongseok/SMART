#include "DSM.h"
#include "log_writer.h"

#include <string>
#include <stdlib.h>

static void log_stats(DSM* dsm, bool verbose=false) {
  LogWriter* lw = new LogWriter("SERVER");
  lw->LOG_server_info(dsm->getClusterSize(), dsm->getComputeNR());

  uint64_t cum_insert_keys = 0;
  uint64_t cum_search_keys = 0;
  uint64_t cum_inserted_keys = 0;
  uint64_t cum_searched_keys = 0;
  double cum_insert_latency = 0;
  double cum_search_latency = 0;
  double cum_insert_throughput = 0;
  double cum_search_throughput = 0;
  for (uint16_t i = 0; i < dsm->getComputeNR(); ++i) {
    std::string metric = std::string(dsm->get_key("metric", i));
    uint64_t insert_keys = *(uint64_t*)dsm->get_key("insert_keys", i);
    uint64_t inserted_keys = *(uint64_t*)dsm->get_key("inserted_keys", i);
    uint64_t insert_time = *(uint64_t*)dsm->get_key("insert_time", i);
    uint64_t search_keys = *(uint64_t*)dsm->get_key("search_keys", i);
    uint64_t searched_keys = *(uint64_t*)dsm->get_key("searched_keys", i);
    uint64_t search_time = *(uint64_t*)dsm->get_key("search_time", i);

    if (verbose) {
      if (!metric.compare("LATENCY")) {
        lw->LOG("[Client %d latency] Insert: %.3e (nsec/op)\tSearch: %.3e (nsec/op)", 
                i, (double)insert_time/(double)insert_keys, (double)search_time/(double)search_keys);
      } else {
        lw->LOG("[Client %d throughput] Insert: %.3e (op/nsec)\tSearch: %.3e (op/nsec)", 
                i, (double)insert_keys/(double)insert_time, (double)search_keys/(double)search_time);
      }
      
      lw->LOG("[Client %d operation status] Insert: %lu/%lu\tSearch: %lu/%lu", 
              i, inserted_keys, insert_keys, searched_keys, search_keys);
    }

    cum_insert_keys += insert_keys;
    cum_search_keys += search_keys;
    cum_inserted_keys += inserted_keys;
    cum_searched_keys += searched_keys;
    cum_insert_latency += ((double)insert_time/(double)insert_keys);
    cum_search_latency += ((double)search_time/(double)search_keys);
    cum_insert_throughput += ((double)insert_keys/(double)insert_time);
    cum_search_throughput += ((double)search_keys/(double)search_time);
  }

  if (!std::string(dsm->get_key("metric", 0)).compare("LATENCY")) {
    lw->LOG("[Average latency] Insert: %.3e (nsec/op)\tSearch: %.3e (nsec/op)", 
            cum_insert_latency/(double)dsm->getComputeNR(), cum_search_latency/(double)dsm->getComputeNR()); 
  } else {
    lw->LOG("[Cumulative throughput] Insert: %.3e (op/nsec)\tSearch: %.3e (op/nsec)", 
            cum_insert_throughput, cum_search_throughput);
  }

  lw->LOG("[Operation status] Insert: %lu/%lu\tSearch: %lu/%lu", 
          cum_inserted_keys, cum_insert_keys, cum_searched_keys, cum_search_keys);

  delete lw;
}

int main(int argc, char *argv[]) {
  if (argc != 3) {
    fprintf(stderr, "[ERROR] Two arguments are required but received %d\n", argc - 1);
    exit(1);
  }

  DSMConfig config;
  config.isCompute = false;
  config.computeNR = atoi(argv[1]);
  config.memoryNR = atoi(argv[2]);

  DSM* dsm = DSM::getInstance(config);

  fprintf(stdout, "==================== Starting Server %u ====================\n", dsm->getMyNodeID());
  fprintf(stdout, "[NOTICE] computeNR: %d / memoryNR: %d\n", config.computeNR, config.memoryNR);

  printf("[NOTICE] Enter anything to end server\n");
  getchar();

  if (dsm->getMyNodeID() == 0) {
    log_stats(dsm);
  }

  fprintf(stdout, "====================  Ending Server %u  ====================\n", dsm->getMyNodeID());

  return 0;
}

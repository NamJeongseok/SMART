#include "DSM.h"
#include "log_writer.h"

#include <string>
#include <stdlib.h>

static void log_stats(DSM* dsm, bool verbose=false) {
  LogWriter* lw = new LogWriter("SERVER");

  std::string metric = std::string(dsm->get_key("metric", 0));

  uint64_t total_thread_num = 0;
  for (int i = 0; i < dsm->getComputeNR(); ++i) {
    total_thread_num += *(uint64_t*)dsm->get_key("thread_num", i);
  }
  
  if (!metric.compare("REAL_LATENCY") || !metric.compare("REAL_THROUGHPUT")) {
    lw->LOG_server_info(dsm->getClusterSize(), dsm->getComputeNR(), total_thread_num, *(uint64_t*)dsm->get_key("cache_size", 0),
                        dsm->get_key("load_workload_path", 0), *(uint64_t*)dsm->get_key("bulk_keys", 0));
  } else if (!metric.compare("YCSB_THROUGHPUT")) {
    lw->LOG_ycsb_server_info(dsm->getClusterSize(), dsm->getComputeNR(), total_thread_num, *(uint64_t*)dsm->get_key("cache_size", 0), 
                             dsm->get_key("load_workload_path", 0), dsm->get_key("txn_workload_path", 0), *(uint64_t*)dsm->get_key("bulk_keys", 0));
  } else {
    fprintf(stderr, "[ERROR] Undefined metric type detected\n");
    exit(1);
  }

  uint64_t total_load_keys = 0;
  uint64_t total_load_done_keys = 0;
  uint64_t total_txn_keys = 0;
  uint64_t total_txn_done_keys = 0;

  double cum_load_latency = 0;
  double cum_load_throughput = 0;

  double cum_txn_latency = 0;
  double cum_txn_throughput = 0;

  for (int i = 0; i < dsm->getComputeNR(); ++i) {
    uint64_t load_keys = *(uint64_t*)dsm->get_key("load_keys", i);
    uint64_t load_done_keys = *(uint64_t*)dsm->get_key("load_done_keys", i);
    uint64_t load_time = *(uint64_t*)dsm->get_key("load_time", i);
    
    uint64_t txn_keys = *(uint64_t*)dsm->get_key("txn_keys", i);
    uint64_t txn_done_keys = *(uint64_t*)dsm->get_key("txn_done_keys", i);
    uint64_t txn_time = *(uint64_t*)dsm->get_key("txn_time", i);

    if (verbose) {
      if (!metric.compare("REAL_LATENCY")) {
        lw->LOG("[Client %d latency] Insert: %.3e (nsec/op)\tSearch: %.3e (nsec/op)", 
                i, (double)load_time/(double)load_keys, (double)txn_time/(double)txn_keys);
        lw->LOG("[Client %d operation status] Insert: %lu/%lu\tSearch: %lu/%lu\n", 
                i, load_done_keys, load_keys, txn_done_keys, txn_keys);
      } else if (!metric.compare("REAL_THROUGHPUT")) {
        lw->LOG("[Client %d throughput] Insert: %.3e (op/nsec)\tSearch: %.3e (op/nsec)", 
                i, (double)load_keys/(double)load_time, (double)txn_keys/(double)txn_time);
        lw->LOG("[Client %d operation status] Insert: %lu/%lu\tSearch: %lu/%lu\n", 
                i, load_done_keys, load_keys, txn_done_keys, txn_keys);
      } else if (!metric.compare("YCSB_THROUGHPUT")) {
        lw->LOG("[Client %d throughput] Load: %.3e (op/nsec)\tTxn: %.3e (op/nsec)", 
                i, (double)load_keys/(double)load_time, (double)txn_keys/(double)txn_time);
        lw->LOG("[Client %d operation status] Load: %lu/%lu\tTxn: %lu/%lu\n", 
                i, load_done_keys, load_keys, txn_done_keys, txn_keys);
      }
    }

    total_load_keys += load_keys;
    total_load_done_keys += load_done_keys;

    total_txn_keys += txn_keys;
    total_txn_done_keys += txn_done_keys;

    cum_load_latency += ((double)load_time/(double)load_keys);
    cum_load_throughput += ((double)load_keys/(double)load_time);

    cum_txn_latency += ((double)txn_time/(double)txn_keys);
    cum_txn_throughput += ((double)txn_keys/(double)txn_time);
  }

  if (!metric.compare("REAL_LATENCY")) {
    lw->LOG("[Average latency] Insert: %.3e (nsec/op)\tSearch: %.3e (nsec/op)", 
            cum_load_latency/(double)dsm->getComputeNR(), cum_txn_latency/(double)dsm->getComputeNR());
    lw->LOG("[Total operation status] Insert: %lu/%lu\tSearch: %lu/%lu", 
            total_load_done_keys, total_load_keys, total_txn_done_keys, total_txn_keys);
  } else if (!metric.compare("REAL_THROUGHPUT")) {
    lw->LOG("[Cumulative throughput] Insert: %.3e (op/nsec)\tSearch: %.3e (op/nsec)", 
            cum_load_throughput, cum_txn_throughput);
    lw->LOG("[Total operation status] Insert: %lu/%lu\tSearch: %lu/%lu", 
            total_load_done_keys, total_load_keys, total_txn_done_keys, total_txn_keys);
  } else if (!metric.compare("YCSB_THROUGHPUT")) {
    lw->LOG("[Cumulative throughput] Load: %.3e (op/nsec)\tTxn: %.3e (op/nsec)", 
            cum_load_throughput, cum_txn_throughput);
    lw->LOG("[Total operation status] Load: %lu/%lu\tTxn: %lu/%lu", 
            total_load_done_keys, total_load_keys, total_txn_done_keys, total_txn_keys);
  }

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
  
  if (dsm->getMyNodeID() == 0) {
    dsm->init_key("end");
  }

  fprintf(stdout, "===================== Starting Server %d =====================\n", dsm->getMyNodeID());  
  fprintf(stdout, "[NOTICE] computeNR: %d / memoryNR: %d\n", config.computeNR, config.memoryNR);

  while (dsm->get_key("end") != dsm->getComputeNR()) {
    continue;
  }

  if (dsm->getMyNodeID() == 0) {
    log_stats(dsm);
  }

  fprintf(stdout, "=====================  Ending Server %u  =====================\n", dsm->getMyNodeID());

  return 0;
}
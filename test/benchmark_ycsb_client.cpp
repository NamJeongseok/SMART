#include "Timer.h"
#include "Tree.h"
#include "zipf.h"
#include "log_writer.h"
#include "key_generator.h"

#include <city.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <thread>
#include <vector>
#include <string>
#include <fstream>

//////////////////// workload parameters /////////////////////
//#define USE_CORO
//const int kCoroCnt = 3;

int threadNum;
std::vector<Request>* rr_load_requests;
std::vector<Request>* rr_txn_requests;
//////////////////// workload parameters /////////////////////

DSM *dsm;
Tree *tree;

pthread_t* tid_list;

pthread_barrier_t load_ready_barrier;
pthread_barrier_t load_done_barrier;
pthread_barrier_t txn_ready_barrier;
pthread_barrier_t txn_done_barrier;
  
uint64_t* load_time_list;
extern uint64_t* successed_requests_list;
uint64_t* txn_time_list;

struct ThreadArgs {
  int tid;
};

class RequsetGenBench : public RequstGen {
public:
  RequsetGenBench(int coro_id, DSM *dsm, int coro_cnt, const std::vector<Request>& requests)
  : coro_id(coro_id), dsm(dsm), requests(requests) {
    start = requests.begin();
    uint64_t requests_per_coroutine = requests.size()/coro_cnt;

    for (int i = 0; i < coro_id; ++i) {
      start += (i < requests.size()%coro_cnt) ? requests_per_coroutine+1 : requests_per_coroutine;
    }

    end = (coro_id < requests.size()%coro_cnt) ? start+requests_per_coroutine+1 : start+requests_per_coroutine;
  }

  Request next() override {
    Request r = *start;
    start++;

    return r;
  }

  std::vector<Request>::const_iterator current_it() {
    return start;
  }

  std::vector<Request>::const_iterator end_it() {
    return end;
  }

private:
  int coro_id;
  DSM *dsm;

  std::vector<Request> requests;
  std::vector<Request>::const_iterator start;
  std::vector<Request>::const_iterator end;
};

RequstGen* coro_func(int coro_id, DSM *dsm, int coro_cnt, const std::vector<Request>& requests) {
  return new RequsetGenBench(coro_id, dsm, coro_cnt, requests);
}

void load_func(Tree *tree, const Request& r, int tid, CoroContext *ctx = nullptr, int coro_id = 0) {
  if (r.op == OpType::INSERT) {
    tree->insert(r.key, key2int(r.key), ctx, coro_id, false, true);
  } else {
    assert(false);
  }
}

void txn_func(Tree *tree, const Request& r, int tid, CoroContext *ctx = nullptr, int coro_id = 0) {
  if (r.op == OpType::INSERT) {
    tree->insert(r.key, key2int(r.key), ctx, coro_id, false);
    successed_requests_list[tid]++;
  } else if (r.op == OpType::SEARCH) {
    Value v;
    auto ret = tree->search(r.key, v, ctx, coro_id);
    if (ret && v == key2int(r.key)) {
      successed_requests_list[tid]++;
    }
  } else if (r.op == OpType::UPDATE) {
    tree->insert(r.key, key2int(r.key), ctx, coro_id, true);
    successed_requests_list[tid]++;
  } else if (r.op == OpType::SCAN) {
    assert(r.range_size > 0);
    std::map<Key, Value> ret;
    tree->range_query(r.key, r.key + r.range_size, ret);
    successed_requests_list[tid]++;
  } else {
    assert(false);
  }
}

void* run_thread(void* _thread_args) {
  struct ThreadArgs* thread_args = (struct ThreadArgs*)_thread_args;
  int tid = thread_args->tid;
  
  bindCore(tid);
  dsm->registerThread();

  struct timespec load_start, load_end;
  struct timespec txn_start, txn_end;

  if (tid == 0) {
    // Wait for all clients to initialize
    dsm->barrier("benchmark");
  }

  pthread_barrier_wait(&load_ready_barrier);

  if (tid == 0) {
    fprintf(stdout, "[NOTICE] Start load\n");
    clock_gettime(CLOCK_REALTIME, &load_start); 
  }

#ifdef USE_CORO
  tree->custom_run_coroutine(coro_func, kCoroCnt, load_func, rr_load_requests[tid]); 
#else
  for (uint64_t i = 0; i < rr_load_requests[tid].size(); ++i) {
    load_func(tree, rr_load_requests[tid][i], tid);
  }
#endif

  // Wait for all the threads to finish insert
  pthread_barrier_wait(&load_done_barrier);

  if (tid == 0) {
    clock_gettime(CLOCK_REALTIME, &load_end);  

    // Wait for all clients to finish insert
    dsm->barrier("load");
  }

  // Wait for all the threads to be ready for search
  pthread_barrier_wait(&txn_ready_barrier);

  if (tid == 0) {
    fprintf(stdout, "[NOTICE] Start txn\n");
    clock_gettime(CLOCK_REALTIME, &txn_start);    
  }

#ifdef USE_CORO
  tree->custom_run_coroutine(coro_func, kCoroCnt, txn_func, rr_txn_requests[tid]); 
#else
  for (uint64_t i = 0; i < rr_txn_requests[tid].size(); ++i) {
    txn_func(tree, rr_txn_requests[tid][i], tid);
  }
#endif

  // Wait for all the threads to finish search
  pthread_barrier_wait(&txn_done_barrier);

  if (tid == 0) {
    clock_gettime(CLOCK_REALTIME, &txn_end);
  
    load_time_list[tid] = (load_end.tv_sec - load_start.tv_sec) * 1000000000 + (load_end.tv_nsec - load_start.tv_nsec); 
    txn_time_list[tid] = (txn_end.tv_sec - txn_start.tv_sec) * 1000000000 + (txn_end.tv_nsec - txn_start.tv_nsec);

    // Wait for all clients to finish insert
    dsm->barrier("txn");
  }

  return NULL;
}

int main(int argc, char *argv[]) {
  if (argc != 9) {
    fprintf(stderr, "[ERROR] Eight arguments are required but received %d\n", argc - 1);
    exit(1);
  }

  DSMConfig config;
  config.isCompute = true;
  config.computeNR = atoi(argv[1]);
  config.memoryNR = atoi(argv[2]);

  threadNum = atoi(argv[3]);
  std::string workloadDir = argv[4];
  std::string workloadName = argv[5];
  uint64_t loadNumKeys = atol(argv[6]);
  uint64_t txnNumKeys = atol(argv[7]);
  uint64_t cache_size = atol(argv[8]);

  // Should not exceed maximum thread number
  assert(threadNum < MAX_APP_THREAD);

  std::string ycsbLoadPath = workloadDir + "/load_randint_workload" + workloadName;
  std::string ycsbTxnPath = workloadDir + "/txn_randint_workload" + workloadName;
  if (workloadName == "d" || workloadName == "25" || workloadName == "50" || workloadName == "75") {
    ycsbTxnPath += "_new";
  } 

  uint64_t numBulkKeys = 102400;

  vector<Request> bulk_requests(numBulkKeys);
  vector<Request> load_requests(loadNumKeys - numBulkKeys);

  ifstream load_ifs;
  load_ifs.open(ycsbLoadPath);

  if (!load_ifs.is_open()) {
    fprintf(stdout, "[ERROR] Failed opening file %s (error: %s)\n", ycsbLoadPath.c_str(), strerror(errno));
    exit(1);
  }

  if (skip_BOM(load_ifs)) {
    fprintf(stdout, "[NOTICE] Removed BOM in target workload\n");
  }

  fprintf(stdout, "[NOTICE] Start reading %lu load keys\n", loadNumKeys);

  uint64_t k;
  std::string op;
  for (uint64_t i = 0; i < numBulkKeys; ++i) {
    load_ifs >> op >> k;

    assert(op == "INSERT");
    bulk_requests[i].op = OpType::INSERT;
    bulk_requests[i].range_size = 0;
    bulk_requests[i].key = int2key(k);
  }

  for (uint64_t i = 0; i < loadNumKeys - numBulkKeys; ++i) {
    load_ifs >> op >> k;

    assert(op == "INSERT");
    load_requests[i].op = OpType::INSERT;
    load_requests[i].range_size = 0;
    load_requests[i].key = int2key(k);
  }

  fprintf(stdout, "[NOTICE] Start multi client benchmark\n");
  dsm = DSM::getInstance(config);
  
  KeyGenerator<Request, Value> key_gen;
#ifdef USE_CORO
  fprintf(stdout, "[NOTICE] Start dividing load keys to %d threads (%u coroutine/thread)\n", threadNum, kCoroCnt);
  rr_load_requests = key_gen.gen_key_multi_client(load_requests, loadNumKeys - numBulkKeys, config.computeNR, threadNum, kCoroCnt, dsm->getMyNodeID());
#else
  fprintf(stdout, "[NOTICE] Start dividing load keys to %d threads (coroutine disabled)\n", threadNum);
  rr_load_requests = key_gen.gen_key_multi_client(load_requests, loadNumKeys - numBulkKeys, config.computeNR, threadNum, 1, dsm->getMyNodeID());
#endif

  load_requests.clear();
  load_requests.shrink_to_fit();

  vector<Request> txn_requests(txnNumKeys); 

  ifstream txn_ifs;
  txn_ifs.open(ycsbTxnPath);

  if (!txn_ifs.is_open()) {
    fprintf(stdout, "[ERROR] Failed opening file %s (error: %s)\n", ycsbTxnPath.c_str(), strerror(errno));
    exit(1);
  }

  if (skip_BOM(txn_ifs)) {
    fprintf(stdout, "[NOTICE] Removed BOM in target txn workload\n");
  }

  fprintf(stdout, "[NOTICE] Start reading %lu txn keys\n", txnNumKeys);

  for (uint64_t i = 0; i < txnNumKeys; ++i) {
    txn_ifs >> op >> k;
    
    if (op == "INSERT") {
      txn_requests[i].op = OpType::INSERT;
      txn_requests[i].range_size = 0;
      txn_requests[i].key = int2key(k);
    } else if (op == "READ") {
      txn_requests[i].op = OpType::SEARCH;
      txn_requests[i].range_size = 0;
      txn_requests[i].key = int2key(k);
    } else if (op == "UPDATE") {
      txn_requests[i].op = OpType::UPDATE;
      txn_requests[i].range_size = 0;
      txn_requests[i].key = int2key(k);
    } else if (op == "SCAN") {
      txn_requests[i].op = OpType::SCAN;
      txn_ifs >> txn_requests[i].range_size;
      txn_requests[i].key = int2key(k);
    } else {
      fprintf(stdout, "[ERROR] Unknown operation type '%s'\n", op.c_str());
      exit(1);
    }
  }
   
#ifdef USE_CORO
  fprintf(stdout, "[NOTICE] Start dividing txn keys to %d threads (%u coroutine/thread)\n", threadNum, kCoroCnt);
  rr_txn_requests = key_gen.gen_key_multi_client(txn_requests, txnNumKeys, config.computeNR, threadNum, kCoroCnt, dsm->getMyNodeID());
#else
  fprintf(stdout, "[NOTICE] Start dividing txn keys to %d threads (coroutine disabled)\n", threadNum);
  rr_txn_requests = key_gen.gen_key_multi_client(txn_requests, txnNumKeys, config.computeNR, threadNum, 1, dsm->getMyNodeID());
#endif

  txn_requests.clear();
  txn_requests.shrink_to_fit();

  LogWriter* lw = new LogWriter("COMPUTE");
  lw->print_ycsb_client_info(threadNum, cache_size*define::MB, ycsbLoadPath.c_str(), ycsbTxnPath.c_str(), loadNumKeys - numBulkKeys, txnNumKeys, numBulkKeys);
  lw->LOG_ycsb_client_info(threadNum, cache_size*define::MB, ycsbLoadPath.c_str(), ycsbTxnPath.c_str(), loadNumKeys - numBulkKeys, txnNumKeys, numBulkKeys);

  fprintf(stdout, "[NOTICE] Start initializing index structure\n");
  dsm->registerThread();
  tree = new Tree(dsm, cache_size);

  if (dsm->getMyNodeID() == 0) {
    fprintf(stdout, "[NOTICE] Start bulk loading %lu keys\n", numBulkKeys);
    for (uint64_t i = 0; i < bulk_requests.size(); ++i) {
      tree->insert(bulk_requests[i].key, (Value)key2int(bulk_requests[i].key));
    }
  }

  dsm->resetThread();

  tid_list = new pthread_t[threadNum];
  struct ThreadArgs* thread_args_list = new ThreadArgs[threadNum];

  pthread_barrier_init(&load_ready_barrier, NULL, threadNum);
  pthread_barrier_init(&load_done_barrier, NULL, threadNum);
  pthread_barrier_init(&txn_ready_barrier, NULL, threadNum);
  pthread_barrier_init(&txn_done_barrier, NULL, threadNum);

  load_time_list = new uint64_t[threadNum]{0};
  successed_requests_list = new uint64_t[threadNum]{0};
  txn_time_list = new uint64_t[threadNum]{0};

  for (int i = 0; i < threadNum; ++i) {
    thread_args_list[i].tid = i;

    pthread_t tid;
    pthread_create(&tid, NULL, run_thread, &thread_args_list[i]);

    tid_list[i] = tid;
  }

  uint64_t load_time = 0;
  uint64_t txn_time = 0;
  uint64_t total_load_requests = 0;
  uint64_t total_txn_requests = 0;
  uint64_t total_successed_requests = 0;
  for (int i = 0; i < threadNum; ++i) {
    pthread_join(tid_list[i], NULL);

    if (i == 0) {
      load_time = load_time_list[i];
      txn_time = txn_time_list[i];
    }

    total_load_requests += rr_load_requests[i].size();
    total_txn_requests += rr_txn_requests[i].size();
    total_successed_requests += successed_requests_list[i];
  }

  lw->LOG("Average load throughput(op/nsec): %.3e", (double)total_load_requests/(double)load_time);
  lw->LOG("Average txn throughput(op/nsec): %.3e (%lu/%lu successed)", (double)total_txn_requests/(double)txn_time, total_successed_requests, total_txn_requests);

  lw->LOG_client_cache_info(tree->get_cache_statistics());

  dsm->set_key("metric", "YCSB_THROUGHPUT");
  dsm->set_key("thread_num", (uint64_t)threadNum);
  dsm->set_key("cache_size", (uint64_t)(cache_size*define::MB));
  dsm->set_key("bulk_keys", numBulkKeys);
  dsm->set_key("load_workload_path", ycsbLoadPath);
  dsm->set_key("txn_workload_path", ycsbTxnPath);
  dsm->set_key("load_keys", total_load_requests);
  dsm->set_key("load_done_keys", total_load_requests);
  dsm->set_key("load_time", load_time);
  dsm->set_key("txn_keys", total_txn_requests);
  dsm->set_key("txn_done_keys", total_successed_requests);
  dsm->set_key("txn_time", txn_time);

  dsm->faa("end");

  delete lw;
  delete tree;

  return 0;
}
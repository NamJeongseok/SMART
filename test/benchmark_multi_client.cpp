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
std::vector<Request>* rr_i_requests;
std::vector<Request>* rr_s_requests;
//////////////////// workload parameters /////////////////////

DSM *dsm;
Tree *tree;

pthread_t* tid_list;

pthread_barrier_t insert_ready_barrier;
pthread_barrier_t insert_done_barrier;
pthread_barrier_t search_ready_barrier;
pthread_barrier_t search_done_barrier;
  
uint64_t* insert_time_list;
extern uint64_t* found_keys_list;
uint64_t* search_time_list;

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


void insert_func(Tree *tree, const Request& r, int tid, CoroContext *ctx = nullptr, int coro_id = 0) {
  tree->insert(r.key, (Value)key2int(r.key), ctx, coro_id);
}

void search_func(Tree *tree, const Request& r, int tid, CoroContext *ctx = nullptr, int coro_id = 0) {
  Value v;
  auto ret = tree->search(r.key, v);
  if (ret && v == (Value)key2int(r.key)) {
    found_keys_list[tid]++;
  }
}

void* run_thread(void* _thread_args) {
  struct ThreadArgs* thread_args = (struct ThreadArgs*)_thread_args;
  int tid = thread_args->tid;

  bindCore(tid);
  dsm->registerThread();

  struct timespec insert_start, insert_end;
  struct timespec search_start, search_end;

  if (tid == 0) {
    // Wait for all clients to initialize
    dsm->barrier("benchmark");
  }

  pthread_barrier_wait(&insert_ready_barrier);

  if (tid == 0) {
    fprintf(stdout, "[NOTICE] Start insert\n");
    clock_gettime(CLOCK_REALTIME, &insert_start); 
  }

#ifdef USE_CORO
  tree->custom_run_coroutine(coro_func, kCoroCnt, insert_func, rr_i_requests[tid]);
#else
  for (uint64_t i = 0; i < rr_i_requests[tid].size(); ++i) {
    insert_func(tree, rr_i_requests[tid][i], tid);
  }
#endif

  // Wait for all the threads to finish insert
  pthread_barrier_wait(&insert_done_barrier);

  if (tid == 0) {
    clock_gettime(CLOCK_REALTIME, &insert_end);  

    // Wait for all clients to finish insert
    dsm->barrier("insert");
  }

  // Wait for all the threads to be ready for search
  pthread_barrier_wait(&search_ready_barrier);

  if (tid == 0) {
    fprintf(stdout, "[NOTICE] Start search\n");
    clock_gettime(CLOCK_REALTIME, &search_start);    
  }

#ifdef USE_CORO
  tree->custom_run_coroutine(coro_func, kCoroCnt, search_func, rr_s_requests[tid]);
#else
  for (uint64_t i = 0; i < rr_s_requests[tid].size(); ++i) {
    search_func(tree, rr_s_requests[tid][i], tid);
  }
#endif

  // Wait for all the threads to finish search
  pthread_barrier_wait(&search_done_barrier);

  if (tid == 0) {
    clock_gettime(CLOCK_REALTIME, &search_end);
  
    insert_time_list[tid] = (insert_end.tv_sec - insert_start.tv_sec) * 1000000000 + (insert_end.tv_nsec - insert_start.tv_nsec); 
    search_time_list[tid] = (search_end.tv_sec - search_start.tv_sec) * 1000000000 + (search_end.tv_nsec - search_start.tv_nsec);

    // Wait for all clients to finish insert
    dsm->barrier("search");  
  }

  return NULL;
}

int main(int argc, char *argv[]) {
  if (argc != 7) {
    fprintf(stderr, "[ERROR] Six arguments are required but received %d\n", argc - 1);
    exit(1);
  }

  DSMConfig config;
  config.isCompute = true;
  config.computeNR = atoi(argv[1]);
  config.memoryNR = atoi(argv[2]);

  threadNum = atoi(argv[3]);
  std::string workloadPath = argv[4];
  uint64_t numKeys = atol(argv[5]);
  uint64_t cache_size = atol(argv[6]);

  // Should not exceed maximum thread number
  assert(threadNum < MAX_APP_THREAD);

  vector<Request> s_requests(numKeys); 

  ifstream ifs;
  ifs.open(workloadPath);

  if (!ifs.is_open()) {
    fprintf(stdout, "[ERROR] Failed opening file %s (error: %s)\n", workloadPath.c_str(), strerror(errno));
    exit(1);
  }

  if (skip_BOM(ifs)) {
    fprintf(stdout, "[NOTICE] Removed BOM in target workload\n");
  }

  fprintf(stdout, "[NOTICE] Start reading %lu keys\n", numKeys);

  uint64_t k;
  for (uint64_t i = 0; i < numKeys; ++i) {
    ifs >> k;
    s_requests[i].key = int2key(k);
  }

  uint64_t numBulkKeys = 102400;
  vector<Request> bulk_requests(s_requests.begin(), s_requests.begin() + numBulkKeys);
  vector<Request> i_requests(s_requests.begin() + numBulkKeys, s_requests.end());

  fprintf(stdout, "[NOTICE] Start multi client benchmark\n");
  dsm = DSM::getInstance(config);
 
#ifdef USE_CORO
  fprintf(stdout, "[NOTICE] Start dividing keys to %d threads (%u coroutine/thread)\n", threadNum, kCoroCnt);
#else
  fprintf(stdout, "[NOTICE] Start dividing keys to %d threads (coroutine disabled)\n", threadNum);
#endif

  KeyGenerator<Request, Value> key_gen;
  rr_i_requests = key_gen.gen_key_multi_client(i_requests, numKeys - numBulkKeys, config.computeNR, threadNum, 1, dsm->getMyNodeID());
  rr_s_requests = key_gen.gen_key_multi_client(s_requests, numKeys, config.computeNR, threadNum, 1, dsm->getMyNodeID()); 

  i_requests.clear();
  i_requests.shrink_to_fit();

  s_requests.clear();
  s_requests.shrink_to_fit();

  LogWriter* lw = new LogWriter("COMPUTE");
  lw->print_client_info(threadNum, cache_size*define::MB, workloadPath.c_str(), numBulkKeys, numKeys - numBulkKeys, numKeys);
  lw->LOG_client_info(threadNum, cache_size*define::MB, workloadPath.c_str(), numBulkKeys, numKeys - numBulkKeys, numKeys);

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

  pthread_barrier_init(&insert_ready_barrier, NULL, threadNum);
  pthread_barrier_init(&insert_done_barrier, NULL, threadNum);
  pthread_barrier_init(&search_ready_barrier, NULL, threadNum);
  pthread_barrier_init(&search_done_barrier, NULL, threadNum);

  insert_time_list = new uint64_t[threadNum]{0};
  found_keys_list = new uint64_t[threadNum]{0};
  search_time_list = new uint64_t[threadNum]{0};

  for (int i = 0; i < threadNum; ++i) {
    thread_args_list[i].tid = i;

    pthread_t tid;
    pthread_create(&tid, NULL, run_thread, &thread_args_list[i]);

    tid_list[i] = tid;
  }

  uint64_t insert_time = 0; 
  uint64_t search_time = 0;
  uint64_t insert_keys = 0;
  uint64_t search_keys = 0;
  uint64_t found_keys = 0;
  for (int i = 0; i < threadNum; ++i) {
    pthread_join(tid_list[i], NULL);

    if (i == 0) {
      insert_time = insert_time_list[i];
      search_time = search_time_list[i];
    }

    insert_keys += rr_i_requests[i].size();
    search_keys += rr_s_requests[i].size();
    found_keys += found_keys_list[i];
  }

  lw->LOG("Average insert throughput(op/nsec): %.3e", (double)insert_keys/(double)insert_time);
  lw->LOG("Average search throughput(op/nsec): %.3e (%lu/%lu found)", (double)search_keys/(double)search_time, found_keys, search_keys);

  lw->LOG_client_cache_info(tree->get_cache_statistics());

  dsm->set_key("metric", "REAL_THROUGHPUT");
  dsm->set_key("thread_num", (uint64_t)threadNum);
  dsm->set_key("cache_size", (uint64_t)(cache_size*define::MB));
  dsm->set_key("bulk_keys", numBulkKeys);
  dsm->set_key("load_workload_path", workloadPath);
  dsm->set_key("txn_workload_path", workloadPath);
  dsm->set_key("load_keys", insert_keys);
  dsm->set_key("load_done_keys", insert_keys);
  dsm->set_key("load_time", insert_time);
  dsm->set_key("txn_keys", search_keys);
  dsm->set_key("txn_done_keys", found_keys);
  dsm->set_key("txn_time", search_time);

  dsm->faa("end");

  delete lw;
  delete tree;

  return 0;
}
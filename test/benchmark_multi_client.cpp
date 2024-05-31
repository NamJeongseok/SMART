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
int  threadNum;
bool is_ycsb;
std::vector<Key> s_keys;
std::vector<Key>* rr_i_keys;
std::vector<Key>* rr_s_keys;
uint64_t numBulkKeys = 102400;

std::string ycsb_load_path;
std::string ycsb_trans_path;
std::string ycsb_bulk_path;
//////////////////// workload parameters /////////////////////

DSMConfig config;
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
uint64_t* insert_size_list;
uint64_t* search_size_list;

struct ThreadArgs {
  int tid;
};

void* run_ycsb_thread(void* _thread_args);
void* run_thread(void* _thread_args);

class RequsetGenBench : public RequstGen {
public:
  RequsetGenBench(int coro_id, DSM *dsm, int id, int coro_cnt, const std::vector<Key>& keys, bool is_search)
  : coro_id(coro_id), dsm(dsm), id(id), keys(keys), is_search(is_search) {
    start = keys.begin();
    uint64_t keys_per_coroutine = keys.size()/coro_cnt;

    for (int i = 0; i < coro_id; ++i) {
      start += (i < keys.size()%coro_cnt) ? keys_per_coroutine+1 : keys_per_coroutine;
    }

    end = (coro_id < keys.size()%coro_cnt) ? start+keys_per_coroutine+1 : start+keys_per_coroutine;
  }

  Request next() override {
    if (start == end) {
      Request r;
      
      r.k = *keys.end();

      return r;
    }

    Request r;

    r.k = *start;
    r.v = (Value)key2int(r.k);
    r.is_search = is_search;

    start++;

    return r;
  }

private:
  int coro_id;
  DSM *dsm;
  int id;

  bool is_search;
  std::vector<Key> keys;
  std::vector<Key>::const_iterator start;
  std::vector<Key>::const_iterator end;
};

RequstGen* coro_insert_func(int coro_id, DSM *dsm, int id, int coro_cnt, const std::vector<Key>& keys) {
  return new RequsetGenBench(coro_id, dsm, id, coro_cnt, keys, false);
}

RequstGen* coro_search_func(int coro_id, DSM *dsm, int id, int coro_cnt, const std::vector<Key>& keys) {
  return new RequsetGenBench(coro_id, dsm, id, coro_cnt, keys, true);
}

void run_ycsb_bench(std::string workloadName) {
  std::string workload_dir;
  std::ifstream workloads_dir_in("../workloads.conf");
  if (!workloads_dir_in.is_open()) {
    printf("Error opening workloads.conf\n");
    assert(false);
  }

  workloads_dir_in >> workload_dir;
  ycsb_load_path = workload_dir + "/load_randint_workload" + workloadName;
  ycsb_trans_path = workload_dir + "/txn_randint_workload" + workloadName;
  ycsb_bulk_path = workload_dir + "/load_randint_workload" + workloadName + "bulk";

  LogWriter* lw = new LogWriter("COMPUTE");
  lw->LOG_client_info("Multi Client", threadNum, "YCSB-"+workloadName, 0);

  printf("ycsb_bulk: %s\n", ycsb_bulk_path.c_str());
  printf("ycsb_load: %s\n", ycsb_load_path.c_str());
  printf("ycsb_trans: %s\n", ycsb_trans_path.c_str());


  if (dsm->getMyNodeID() == 0) {
    std::ifstream bulk_in(ycsb_bulk_path);
    if (bulk_in.is_open()) {
      fprintf(stdout, "[NOTICE] Start Bulkloading...\n");
      uint64_t int_k, bulk_num=0;
      std::string op;
      while (bulk_in >> op >> int_k) {
        assert(op == "INSERT");
        tree->insert(int2key(int_k), int_k, nullptr, 0, false, true);
        bulk_num++;
      }
      fprintf(stdout, "[NOTICE] Bulkedloaded %lu keys\n", bulk_num);
    }
  }

  fprintf(stdout, "[NOTICE] Start multi client benchmark\n");

  tid_list = new pthread_t[threadNum];
  struct ThreadArgs* thread_args_list = new ThreadArgs[threadNum];

  pthread_barrier_init(&insert_ready_barrier, NULL, threadNum);
  pthread_barrier_init(&insert_done_barrier, NULL, threadNum);
  pthread_barrier_init(&search_ready_barrier, NULL, threadNum);
  pthread_barrier_init(&search_done_barrier, NULL, threadNum);

  insert_time_list = new uint64_t[threadNum]{0};
  found_keys_list = new uint64_t[threadNum]{0};
  search_time_list = new uint64_t[threadNum]{0};
  insert_size_list = new uint64_t[threadNum]{0};
  search_size_list = new uint64_t[threadNum]{0};

  for (int i = 0; i < threadNum; ++i) {
    thread_args_list[i].tid = i;

    pthread_t tid;
    pthread_create(&tid, NULL, run_ycsb_thread, &thread_args_list[i]);

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

    insert_keys += insert_size_list[i];
    search_keys += search_size_list[i];
    found_keys += found_keys_list[i];
  }

  lw->LOG("Average insert throughput(op/nsec): %.3e", (double)insert_keys/(double)insert_time);
  lw->LOG("Average search throughput(op/nsec): %.3e (%lu/%lu found)", (double)search_keys/(double)search_time, found_keys, search_keys);

  dsm->set_key("metric", "THROUGHPUT");
  dsm->set_key("insert_keys", insert_keys);
  dsm->set_key("inserted_keys", insert_keys);
  dsm->set_key("insert_time", insert_time);
  dsm->set_key("search_keys", search_keys);
  dsm->set_key("searched_keys", found_keys);
  dsm->set_key("search_time", search_time);

  delete lw;
}

void run_key_only_bench(uint64_t numKeys, std::string path) {
  // Read Keys
  std::ifstream ifs;
  ifs.open(path);

  if (skip_BOM(ifs)) {
    fprintf(stdout, "[NOTICE] Removed BOM in target workload\n");
  }

  fprintf(stdout, "[NOTICE] Start reading %lu keys\n", numKeys);

  uint64_t k;
  for (uint64_t i = 0; i < numKeys; ++i) {
    ifs >> k;
    s_keys.push_back(int2key(k));
  }

  // Separate Bulk Keys and insert Keys.
  std::vector<Key> b_keys(s_keys.begin(), s_keys.begin()+numBulkKeys);
  std::vector<Key> i_keys(s_keys.begin()+numBulkKeys, s_keys.end());
  uint64_t numInsertKeys = numKeys - numBulkKeys;

  // Bulkload at Client Node 0.
  if (dsm->getMyNodeID() == 0) {
    fprintf(stdout, "[NOTICE] Start bulk loading %lu keys\n", numBulkKeys);
    for (size_t i = 0; i < b_keys.size(); ++i) {
      tree->insert(b_keys[i], (Value)key2int(b_keys[i]));
    }
  }

  dsm->resetThread();

  // Divide keys per client thread.
  // TODO: Pre-divided keys?
  fprintf(stdout, "[NOTICE] Start dividing keys to %d threads (coroutine disabled)\n", threadNum);
  rr_i_keys = gen_key_multi_client(i_keys, numInsertKeys, config.computeNR, threadNum, 1, dsm->getMyNodeID());
  rr_s_keys = gen_key_multi_client(s_keys, numKeys, config.computeNR, threadNum, 1, dsm->getMyNodeID()); 

  LogWriter* lw = new LogWriter("COMPUTE");

  lw->LOG_client_info("Multi Client", threadNum, path, numKeys);

  for (int i = 0; i < threadNum; ++i) {
    lw->LOG("[NOTICE] Num. of keys for thread %d: %d", i, rr_i_keys[i].size());
  }

  fprintf(stdout, "[NOTICE] Start multi client benchmark\n");

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

    insert_keys += rr_i_keys[i].size();
    search_keys += rr_s_keys[i].size();
    found_keys += found_keys_list[i];
  }

  lw->LOG("Average insert throughput(op/nsec): %.3e", (double)insert_keys/(double)insert_time);
  lw->LOG("Average search throughput(op/nsec): %.3e (%lu/%lu found)", (double)search_keys/(double)search_time, found_keys, search_keys);

  dsm->set_key("metric", "THROUGHPUT");
  dsm->set_key("insert_keys", insert_keys);
  dsm->set_key("inserted_keys", insert_keys);
  dsm->set_key("insert_time", insert_time);
  dsm->set_key("search_keys", search_keys);
  dsm->set_key("searched_keys", found_keys);
  dsm->set_key("search_time", search_time);

  delete lw;
}

void* run_ycsb_thread(void* _thread_args) {
  struct ThreadArgs* thread_args = (struct ThreadArgs*)_thread_args;
  int tid = thread_args->tid;

  bindCore(tid);
  dsm->registerThread();

  struct timespec insert_start, insert_end;
  struct timespec search_start, search_end;

  // Calculate the global tid to get correct file.
  uint64_t global_tid = threadNum*dsm->getMyNodeID() + tid;

  std::vector<uint64_t> int_keys;
  std::ifstream load_in(ycsb_load_path + std::to_string(global_tid));
  // TODO: Loader num?
  if (!load_in.is_open()) {
    fprintf(stderr, "[ERROR] Error opening the insert file at Global TID: %lu\n", global_tid);
    assert(false);
  }

  uint64_t int_k;
  std::string op;
  while (load_in >> op >> int_k) {
    if (op != "INSERT") {
      fprintf(stderr, "[ERROR] Error while reading the insert file.\n");
      assert(false);
    }
    uint64_t k = int_k;
    if (int_k > 0 && int_k < (1UL<<56))
      int_keys.push_back(k);
  }
  insert_size_list[tid] = int_keys.size();
  if (tid == 0) {
    dsm->barrier("benchmark");
  }

  pthread_barrier_wait(&insert_ready_barrier);

  if (tid == 0) {
    fprintf(stdout, "[NOTICE] Start insert\n");
    clock_gettime(CLOCK_REALTIME, &insert_start); 
  }

  for (const uint64_t i : int_keys) {
    tree->insert(int2key(i), i);
  }

  // Wait for all the threads to finish insert
  pthread_barrier_wait(&insert_done_barrier);

  if (tid == 0) {
    clock_gettime(CLOCK_REALTIME, &insert_end);  

    // Wait for all clients to finish insert
    dsm->barrier("insert");
  }

  int_keys.clear();

  std::vector<Request> requests;
  std::ifstream trans_in(ycsb_trans_path + std::to_string(global_tid));
  if (!trans_in.is_open()) {
    fprintf(stderr, "[ERROR] Error opening the trans file at Global TID: %lu\n", global_tid);
    assert(false);
  }

  uint64_t range_size;
  while (trans_in >> op >> int_k) {
    if (op == "SCAN") trans_in >> range_size;
    Request r;
    r.is_search = (op == "READ");
    r.is_insert = (op == "INSERT");
    r.is_update = (op == "UPDATE");
    r.range_size = (op == "SCAN") ? range_size : 0;
    r.k = int2key(int_k);
    r.v = int_k;

    requests.push_back(r);
  }
  search_size_list[tid] = requests.size();
  pthread_barrier_wait(&search_ready_barrier);

  if (tid == 0) {
    fprintf(stdout, "[NOTICE] Start search\n");
    clock_gettime(CLOCK_REALTIME, &search_start);    
  }

  Value v;
  for (Request r : requests) {
    if (r.is_search) {
      Value v;
      tree->search(r.k, v);
      if (v == key2int(r.k)) found_keys_list[tid]++;
    }
    else if (r.is_update || r.is_insert) {
      tree->insert(r.k, r.v, nullptr, 0, r.is_update);
    }
    else {
      std::map<Key, Value> ret;
      assert(r.range_size > 0);
      tree->range_query(r.k, r.k+r.range_size, ret);
    }
  }

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

  for (size_t i = 0; i < rr_i_keys[tid].size(); ++i) {
    tree->insert(rr_i_keys[tid][i], (Value)key2int(rr_i_keys[tid][i]));
  }

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

  Value v;
  for (size_t i = 0; i < rr_s_keys[tid].size(); ++i) {
    auto ret = tree->search(rr_s_keys[tid][i], v);
    if (ret && v == (Value)key2int(rr_s_keys[tid][i])) {
      found_keys_list[tid]++;
    }
  }

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
    fprintf(stderr, "[ERROR] Usage: computeNR, memoryNR, threadNum, workloadPath, numKeys, is_ycsb\n");
    exit(1);
  }

  config.isCompute = true;
  config.computeNR = atoi(argv[1]);
  config.memoryNR = atoi(argv[2]);

  threadNum = atoi(argv[3]);
  std::string workloadPath = argv[4];
  uint64_t numKeys = atol(argv[5]);
  is_ycsb = !!atoi(argv[6]);

  // Should not exceed maximum thread number
  assert(threadNum < MAX_APP_THREAD);

  // Initialize RDMA Connections.
  dsm = DSM::getInstance(config);
  dsm->registerThread();

  // Initialize Tree Structure.
  tree = new Tree(dsm);

  // Start Benchmark
  if (is_ycsb) run_ycsb_bench(workloadPath);
  else run_key_only_bench(numKeys, workloadPath);

  return 0;
}
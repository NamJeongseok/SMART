#if !defined(_TREE_H_)
#define _TREE_H_

#include "RadixCache.h"
#include "DSM.h"
#include "Common.h"
#include "LocalLockTable.h"

#include <atomic>
#include <city.h>
#include <functional>
#include <map>
#include <algorithm>
#include <queue>
#include <set>
#include <iostream>


/*
  Workloads
*/
enum OpType : uint8_t {
  INSERT,
  SEARCH,
  UPDATE,
  SCAN
};

struct Request {
  OpType op;
  uint64_t range_size;
  Key key;
};


class RequstGen {
public:
  RequstGen() = default;
  virtual Request next() { return Request{}; }
  virtual std::vector<Request>::const_iterator current_it() { return std::vector<Request>::const_iterator(0); }
  virtual std::vector<Request>::const_iterator end_it() { return std::vector<Request>::const_iterator(0); }
};


/*
  Tree
*/
using GenFunc = std::function<RequstGen *(DSM*, Request*, int, int, int)>;
using CustomCoroFunc = std::function<RequstGen* (int, DSM *, int, const std::vector<Request>&)>;

#define MAX_FLAG_NUM 12
enum {
  FIRST_TRY,
  CAS_NULL,
  INVALID_LEAF,
  CAS_LEAF,
  INVALID_NODE,
  SPLIT_HEADER,
  FIND_NEXT,
  CAS_EMPTY,
  INSERT_BEHIND_EMPTY,
  INSERT_BEHIND_TRY_NEXT,
  SWITCH_RETRY,
  SWITCH_FIND_TARGET,
};

class Tree {
public:
  Tree(DSM *dsm, uint64_t cache_size = define::kIndexCacheSize, uint16_t tree_id = 0);
  ~Tree(void);

  //using WorkFunc = std::function<void (Tree *, const Request&, CoroContext *, int)>;
  //void run_coroutine(GenFunc gen_func, WorkFunc work_func, int coro_cnt, Request* req = nullptr, int req_num = 0);
  
  /* JY: Added for custom coroutine execution. */
  using WorkFunc = std::function<void (Tree*, const Request&, int, CoroContext*, int)>;
  void custom_run_coroutine(CustomCoroFunc func, int coro_cnt, WorkFunc work_func, const std::vector<Request>& requests); 

  void insert(const Key &k, Value v, CoroContext *cxt = nullptr, int coro_id = 0, bool is_update = false, bool is_load = false);
  bool search(const Key &k, Value &v, CoroContext *cxt = nullptr, int coro_id = 0);
  void range_query(const Key &from, const Key &to, std::map<Key, Value> &ret);
  void statistics();
  CacheStats* get_cache_statistics();
  void clear_debug_info();

  GlobalAddress get_root_ptr_ptr();
  InternalEntry get_root_ptr(CoroContext *cxt, int coro_id);

private:
  //void coro_worker(CoroYield &yield, RequstGen *gen, WorkFunc work_func, int coro_id);
  void custom_coro_worker(CoroYield &yield, RequstGen *gen, int coro_id, WorkFunc work_func);
  //void coro_master(CoroYield &yield, int coro_cnt);
  void custom_coro_master(CoroYield &yield, int coro_cnt);

  bool read_leaf(const GlobalAddress &leaf_addr, char *leaf_buffer, int leaf_size, const GlobalAddress &p_ptr, bool from_cache, CoroContext *cxt, int coro_id);
  void in_place_update_leaf(const Key &k, Value &v, const GlobalAddress &leaf_addr, Leaf *leaf,
                           CoroContext *cxt, int coro_id);
  bool out_of_place_update_leaf(const Key &k, Value &v, int depth, GlobalAddress& leaf_addr, const GlobalAddress &e_ptr, InternalEntry &old_e, const GlobalAddress& node_addr,
                                CoroContext *cxt, int coro_id, bool disable_handover = false);
  bool out_of_place_write_leaf(const Key &k, Value &v, int depth, GlobalAddress& leaf_addr, uint8_t partial_key,
                               const GlobalAddress &e_ptr, const InternalEntry &old_e, const GlobalAddress& node_addr, uint64_t *ret_buffer,
                               CoroContext *cxt, int coro_id);

  bool read_node(InternalEntry &p, bool& type_correct, char *node_buffer, const GlobalAddress& p_ptr, int depth, bool from_cache,
                 CoroContext *cxt, int coro_id);
  bool out_of_place_write_node(const Key &k, Value &v, int depth, GlobalAddress& leaf_addr, int partial_len, uint8_t diff_partial,
                               const GlobalAddress &e_ptr, const InternalEntry &old_e, const GlobalAddress& node_addr, uint64_t *ret_buffer,
                               CoroContext *cxt, int coro_id);

  bool insert_behind(const Key &k, Value &v, int depth, GlobalAddress& leaf_addr, uint8_t partial_key, NodeType node_type,
                     const GlobalAddress &node_addr, uint64_t *ret_buffer, int& inserted_idx,
                     CoroContext *cxt, int coro_id);
  void search_entries(const Key &from, const Key &to, int target_depth, std::vector<ScanContext> &res,
                      CoroContext *cxt, int coro_id);
  void cas_node_type(NodeType next_type, GlobalAddress p_ptr, InternalEntry p, Header hdr,
                     CoroContext *cxt, int coro_id);
  void range_query_on_page(InternalPage* page, bool from_cache, int depth,
                           GlobalAddress p_ptr, InternalEntry p,
                           const Key &from, const Key &to, State l_state, State r_state,
                           std::vector<ScanContext>& res);
  void get_on_chip_lock_addr(const GlobalAddress &leaf_addr, GlobalAddress &lock_addr, uint64_t &mask);
#ifdef TREE_TEST_ROWEX_ART
  void lock_node(const GlobalAddress &node_addr, CoroContext *cxt, int coro_id);
  void unlock_node(const GlobalAddress &node_addr, CoroContext *cxt, int coro_id);
#endif

private:
  DSM *dsm;
// #ifdef CACHE_ENABLE_ART
  RadixCache *index_cache;
// #else
//   NormalCache *index_cache;
// #endif
  LocalLockTable *local_lock_table;

  static thread_local CoroCall worker[MAX_CORO_NUM];
  static thread_local CoroCall master;
  static thread_local CoroQueue busy_waiting_queue;

  uint64_t tree_id;
  GlobalAddress root_ptr_ptr; // the address which stores root pointer;
};


#endif // _TREE_H_

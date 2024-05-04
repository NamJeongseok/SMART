#include <random>
#include <vector>
#include <algorithm>
#include <fstream>

#include "Common.h"
#include "Key.h"

using namespace std;

bool skip_BOM(ifstream& in) {
  char test[4] = {0};

  in.read(test, 3);
  if (strcmp(test, "\xEF\xBB\xBF") == 0) {
    return true;
  } 
  in.seekg(0);

  return false;
}

/* 
** [SEQ] Generate sorted keys and allocate it sequentially to clients.
**       e.g., { (1 2 3) , (4 5 6) } 
** [RAND] Generate sorted keys and allocate it randomly to clients. 
**        e.g., { (2 5 1) , (4 3 6) }
** [RAND_ALLOW_DUP] Generate duplicated keys and allocate it randomly to clients.
**                  e.g., { (2 5 5) , (2 3 9) }
** [RR] Generate sorted keys and allocate it in a round-robin manner to clients.
**      e.g., { (1 3 5) , (2 4 6) }
** [RR_SHUF] Generate sorted keys, allocate it in a round-robin manner to clients and shuffle within clients.
**           e.g., { (3 5 1) , (2 6 4) }
*/ 
enum KeyGenType {
  SEQ, 
  RAND,
  RAND_ALLOW_DUP,
  RR, 
  RR_SHUF 
};

enum KeyGenDistribution {
  UNIFORM
};

std::mt19937_64 get_rand(std::random_device{}());

Value make_value(Key key, bool random = false) {
  if (!random) return (Value)key2int(key);
  else {
    uint64_t rand = (uint64_t)get_rand();
    return (Value)rand;
  }
}

void print_keys(vector<Key>* keys, int num_threads, int num_coroutines) {
  for (int i = 0; i < num_threads; ++i) {
    fprintf(stdout, "[Thread %d]\n", i);

    uint64_t mod = keys[i].size() % num_coroutines;
    uint64_t div = keys[i].size() / num_coroutines;

    uint64_t key_idx = 0;
    for (uint16_t j = 0; j < num_coroutines; ++j) {
      fprintf(stdout, "  >>> Coroutine %u\n", j);

      uint64_t keys_per_coroutine = (j < mod) ? div+1 : div;
      for (uint64_t k = 0; k < keys_per_coroutine; ++k) {
        fprintf(stdout, "      Key: %lu\n", keys[i][key_idx++]);
      }
    }
  }
}

vector<Key>* gen_key_single_client(vector<Key> keys, uint64_t num_keys, int num_threads, int num_coroutines, bool shuf = false) {
  if (shuf) {
    random_device rd;
    shuffle(keys.begin(), keys.end(), std::default_random_engine(rd()));    
  }

  uint64_t period = num_threads * (uint64_t)num_coroutines;
  uint64_t num_period = (num_keys % period) ? num_keys/period + 1 : num_keys/period;

  vector<Key>* result = new vector<Key>[num_threads];

  for (int i = 0; i < num_threads; ++i) {
    for (int j = 0; j < num_coroutines; ++j) {
      for (int k = 0; k < num_period; ++k) {
        if (i*num_coroutines + j + k*period < keys.size()) {
          result[i].push_back(keys[i*num_coroutines + j + k*period]);
        }
      }
    }
  }

  return result;     
}

vector<Key>* gen_seq_keys(uint64_t num_keys, uint32_t num_clients, int num_threads, int num_coroutines, uint16_t current_cid) {
  uint64_t mod = num_keys % (uint64_t)num_clients;
  uint64_t div = num_keys / (uint64_t)num_clients;

  uint64_t keys_per_client = (current_cid < mod) ? div+1 : div;

  uint64_t start = 0;
  for (uint64_t i = 0; i < current_cid; ++i) {
    if (i < mod) {
      start += div + 1;
    } else {
      start += div;
    }
  }

  // Generate keys for current client
  vector<Key> client_keys;
  for (uint64_t i = 0; i < keys_per_client; ++i) {
    client_keys.push_back(int2key(start + i));
  }

  return gen_key_single_client(client_keys, client_keys.size(), num_threads, num_coroutines);
}

vector<Key>* gen_rand_keys(uint64_t num_keys, uint32_t num_clients, int num_threads, int num_coroutines, uint16_t current_cid) {
  vector<Key> keys;
  for (int i = 0 ; i < num_keys; ++i) {
    keys.push_back(int2key(i));
  }
  
  random_device rd;
  shuffle(keys.begin(), keys.end(), std::default_random_engine(rd()));  

  uint64_t mod = num_keys % (uint64_t)num_clients;
  uint64_t div = num_keys / (uint64_t)num_clients;

  uint64_t keys_per_client = (current_cid < mod) ? div+1 : div;

  uint64_t start = 0;
  for (uint64_t i = 0; i < current_cid; ++i) {
    if (i < mod) {
      start += div + 1;
    } else {
      start += div;
    }
  }  

  // Generate keys for current client
  vector<Key> client_keys;
  for (uint64_t i = 0; i < keys_per_client; ++i) {
    client_keys.push_back(keys[start + i]);
  }

  return gen_key_single_client(client_keys, client_keys.size(), num_threads, num_coroutines);
}

vector<Key>* gen_rand_dup_keys(uint64_t num_keys, uint32_t num_clients, int num_threads, int num_coroutines, uint16_t current_cid) {
  uint64_t mod = num_keys % (uint64_t)num_clients;
  uint64_t div = num_keys / (uint64_t)num_clients;

  uint64_t keys_per_client = (current_cid < mod) ? div+1 : div;

  // Generate keys for current client
  vector<Key> client_keys;
  for (uint64_t i = 0; i < keys_per_client; ++i) {
    client_keys.push_back(int2key((uint64_t)get_rand()));
  }

  return gen_key_single_client(client_keys, client_keys.size(), num_threads, num_coroutines);
}

vector<Key>* gen_rr_keys(uint64_t num_keys, uint32_t num_clients, int num_threads, int num_coroutines, uint16_t current_cid, bool shuf) {
  // Generate keys for current client
  vector<Key> client_keys;
  for (uint64_t i = current_cid; i < num_keys; i += num_clients) {
    client_keys.push_back(int2key(i));
  }

  return gen_key_single_client(client_keys, client_keys.size(), num_threads, num_coroutines, shuf);
}

/*
** num_keys       -> Total number of keys among all clients
** num_clients    -> Total number of clients among the cluster
** num_threads    -> Number of threads in each client
** num_coroutines -> Number of coroutines in each thread of each client
** current_cid    -> ID of current client
** genType        -> Key generation type
** 
** e.g., gen_key_multi_client(20, 2, 1, 2, 1, InputGenType::RR_SHUF) 
**       -> Total 20 keys
**       -> Total 2 clients
**       -> 1 threads per client
**       -> 2 coroutines per thread
**       -> Currently on client 1 (second client)
** 
** Client 1 result)
** [Thread 0]
**  >>> Coroutine 0
**    Key: 11 
**    Key: 19
**    Key: 5
**    Key: 17
**    Key: 3
** [Thread 0]
**  >>> Coroutine 1
**    Key: 15 
**    Key: 7
**    Key: 4
**    Key: 9
**    Key: 13
*/

/* Function for generating keys from scratch. */
vector<Key>* gen_key_multi_client(uint64_t num_keys, uint32_t num_clients, int num_threads, 
                                  int num_coroutines, uint16_t current_cid, KeyGenType genType = KeyGenType::RR_SHUF) {
  switch (genType) {
    case KeyGenType::SEQ:
      return gen_seq_keys(num_keys, num_clients, num_threads, num_coroutines, current_cid);
    case KeyGenType::RAND:
      return gen_rand_keys(num_keys, num_clients, num_threads, num_coroutines, current_cid);
    case KeyGenType::RAND_ALLOW_DUP:
      return gen_rand_dup_keys(num_keys, num_clients, num_threads, num_coroutines, current_cid);
    case KeyGenType::RR:
      return gen_rr_keys(num_keys, num_clients, num_threads, num_coroutines, current_cid, false);
    case KeyGenType::RR_SHUF:
      return gen_rr_keys(num_keys, num_clients, num_threads, num_coroutines, current_cid, true);
    default:
      return gen_rr_keys(num_keys, num_clients, num_threads, num_coroutines, current_cid, true);
  }
}

/* Function for generating keys from a given dataset. */
vector<Key>* gen_key_multi_client(vector<Key> keys, uint64_t num_keys, uint32_t num_clients,
                                  int num_threads, int num_coroutines, uint16_t current_cid) {
  // Extract keys for current client
  vector<Key> client_keys;
  for (uint64_t i = current_cid; i < num_keys; i += num_clients) {
    client_keys.push_back(keys[i]);
  }

  // Generate keys for current client
  return gen_key_single_client(client_keys, client_keys.size(), num_threads, num_coroutines);
}
#ifndef _LOG_WRITER_H
#define _LOG_WRITER_H

#include <cstdarg>
#include <string>
#include <sstream>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>

using namespace std;

class LogWriter {
private:
  FILE* fpt;

public:
  LogWriter(string name) {
    time_t timer = time(NULL);
    struct tm* t = localtime(&timer);

    char date_c_str[20];
    strftime(date_c_str, 20, "%Y%m%d_%H:%M:%S", t);
    string date(date_c_str);

    ostringstream ss;
    ss << "../test/result/SMART_" << name << "_LOG_" << date << ".txt";

    fpt = fopen(ss.str().c_str(), "w");
  };

  ~LogWriter(void) {
    fclose(fpt);
  };

  void LOG(const char* format, ...) {
    char new_format[256];
    sprintf(new_format, "%s\n", format);

    va_list args;
    va_start(args, format);
    vfprintf(fpt, new_format, args);
    va_end(args);
  };

  void LOG_error(const char* format, ...) {
    char new_format[256];
    sprintf(new_format, "[ERROR] %s\n", format);

    va_list args;
    va_start(args, format);
    vfprintf(fpt, new_format, args);
    va_end(args);
  };

  void LOG_notice(const char* format, ...) {
    char new_format[256];
    sprintf(new_format, "[NOTICE] %s\n", format);

    va_list args;
    va_start(args, format);
    vfprintf(fpt, new_format, args);
    va_end(args);
  };

  void LOG_server_info(uint16_t server_num, uint16_t client_num, uint64_t thread_num, uint64_t cache_size, const char* workload_path, uint64_t bulk_keys) {
    fprintf(fpt, "==================== Server Information ====================\n");
    fprintf(fpt, "Server number        : %u\n", server_num);
    fprintf(fpt, "Client number        : %u (total %lu threads)\n", client_num, thread_num);
    fprintf(fpt, "Cache size           : %lu MB\n", cache_size);
    fprintf(fpt, "Workload             : %s\n", workload_path);
    fprintf(fpt, "Bulk load key number : %lu\n", bulk_keys);
    fprintf(fpt, "============================================================\n");
  }

  void LOG_ycsb_server_info(uint16_t server_num, uint16_t client_num, uint64_t thread_num, uint64_t cache_size, const char* load_workload_path, const char* txn_workload_path, uint64_t bulk_keys) {
    fprintf(fpt, "==================== Server Information ====================\n");
    fprintf(fpt, "Server number        : %u\n", server_num);
    fprintf(fpt, "Client number        : %u (total %lu threads)\n", client_num, thread_num);
    fprintf(fpt, "Cache size           : %lu MB\n", cache_size);
    fprintf(fpt, "Load workload        : %s\n", load_workload_path);
    fprintf(fpt, "Txn workload         : %s\n", txn_workload_path);
    fprintf(fpt, "Bulk load key number : %lu\n", bulk_keys);
    fprintf(fpt, "============================================================\n");
  }

  void LOG_client_info(uint64_t thread_num, uint64_t cache_size, const char* workload_path, uint64_t bulk_keys, uint64_t insert_keys, uint64_t search_keys) {
    fprintf(fpt, "==================== Client Information ====================\n");
    fprintf(fpt, "Thread number        : %lu\n", thread_num);
    fprintf(fpt, "Cache size           : %lu MB\n", cache_size);
    fprintf(fpt, "Workload             : %s\n", workload_path);
    fprintf(fpt, "Bulk load key number : %lu\n", bulk_keys);
    fprintf(fpt, "Insert key number    : %lu\n", insert_keys);
    fprintf(fpt, "Search key number    : %lu\n", search_keys);
    fprintf(fpt, "============================================================\n");
  }

  void LOG_ycsb_client_info(uint64_t thread_num, uint64_t cache_size, const char* load_workload_path, const char* txn_workload_path, uint64_t load_keys, uint64_t txn_keys, uint64_t bulk_keys) {
    fprintf(fpt, "==================== Client Information ====================\n");
    fprintf(fpt, "Thread number        : %lu\n", thread_num);
    fprintf(fpt, "Cache size           : %lu MB\n", cache_size);
    fprintf(fpt, "Load workload        : %s (%lu keys)\n", load_workload_path, load_keys);
    fprintf(fpt, "Txn workload         : %s (%lu keys)\n", txn_workload_path, txn_keys);
    fprintf(fpt, "Bulk load key number : %lu\n", bulk_keys);
    fprintf(fpt, "============================================================\n");
  }

  void LOG_client_cache_info(CacheStats* cache_stats) {
    fprintf(fpt, "\n[Cache statistics]\n");
    fprintf(fpt, "Total cache size: %luB\n", cache_stats->cache_size); 
    fprintf(fpt, "  >>> Free cache size: %ld\n", cache_stats->free_cache_size);
    fprintf(fpt, "  >>> Used cache size: %ld\n", cache_stats->cache_size - cache_stats->free_cache_size);
    fprintf(fpt, "Cached node number: %ld\n", cache_stats->cache_node_num); 
  }

  void print_client_info(uint64_t thread_num, uint64_t cache_size, const char* workload_path, uint64_t bulk_keys, uint64_t insert_keys, uint64_t search_keys) {
    fprintf(stdout, "==================== Client Information ====================\n");
    fprintf(stdout, "Thread number        : %lu\n", thread_num);
    fprintf(stdout, "Cache size           : %lu MB\n", cache_size);
    fprintf(stdout, "Workload             : %s\n", workload_path);
    fprintf(stdout, "Bulk load key number : %lu\n", bulk_keys);
    fprintf(stdout, "Insert key number    : %lu\n", insert_keys);
    fprintf(stdout, "Search key number    : %lu\n", search_keys);
    fprintf(stdout, "============================================================\n");
  }

  void print_ycsb_client_info(uint64_t thread_num, uint64_t cache_size, const char* load_workload_path, const char* txn_workload_path, uint64_t load_keys, uint64_t txn_keys, uint64_t bulk_keys) {
    fprintf(stdout, "==================== Client Information ====================\n");
    fprintf(stdout, "Thread number        : %lu\n", thread_num);
    fprintf(stdout, "Cache size           : %lu MB\n", cache_size);
    fprintf(stdout, "Load workload        : %s (%lu keys)\n", load_workload_path, load_keys);
    fprintf(stdout, "Txn workload         : %s (%lu keys)\n", txn_workload_path, txn_keys);
    fprintf(stdout, "Bulk load key number : %lu\n", bulk_keys);
    fprintf(stdout, "============================================================\n");
  }
};

#endif // _LOG_WRITER_H
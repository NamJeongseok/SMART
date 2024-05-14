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

  void LOG_server_info(uint16_t server_num, uint16_t client_num) {
    fprintf(fpt, "==================== Server Information ====================\n");
    fprintf(fpt, "Number of servers: %u\n", server_num);
    fprintf(fpt, "Number of clients: %u\n", client_num);
    fprintf(fpt, "============================================================\n");
  }

  void LOG_client_info(string bench_type, uint64_t thread_num, string workload_path, uint64_t num_keys) {
    fprintf(fpt, "==================== Client Information ====================\n");
    fprintf(fpt, "Type: %s (%lu threads)\n", bench_type.c_str(), thread_num);
    fprintf(fpt, "Workload: %s\n", workload_path.c_str());
    fprintf(fpt, "Number of keys: %lu\n", num_keys);
    fprintf(fpt, "============================================================\n");
  }
};

#endif // _LOG_WRITER_H
#ifndef __CONFIG_H__
#define __CONFIG_H__

#include "Common.h"

class CacheConfig {
public:
  uint32_t cacheSize;

  CacheConfig(uint32_t cacheSize = define::rdmaBufferSize) : cacheSize(cacheSize) {}
};

class DSMConfig {
public:
  CacheConfig cacheConfig;
  bool     isCompute;
  uint32_t computeNR;
  uint32_t memoryNR;
  uint64_t dsmSize;       // G

  DSMConfig(const CacheConfig &cacheConfig = CacheConfig(),
            uint32_t memoryNR = 2, uint32_t computeNR = 2, uint64_t dsmSize = define::dsmSize)
      : cacheConfig(cacheConfig), computeNR(computeNR), memoryNR(memoryNR), dsmSize(dsmSize) {}
};

#endif /* __CONFIG_H__ */

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
  uint64_t dsmSizeCompute;       // G
  uint64_t dsmSizeMemory;        // G

  DSMConfig(const CacheConfig &cacheConfig = CacheConfig(),
            uint32_t memoryNR = 12, uint32_t computeNR = 12, uint64_t dsmSizeCompute = define::dsmSizeCompute, uint64_t dsmSizeMemory = define::dsmSizeMemory)
      : cacheConfig(cacheConfig), computeNR(computeNR), memoryNR(memoryNR), dsmSizeCompute(dsmSizeCompute), dsmSizeMemory(dsmSizeMemory) {}
};

#endif /* __CONFIG_H__ */

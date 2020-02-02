// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <memory>
#include <string>

#include <stdlib.h>

#include "arrow/util/logging.h"

#include "plasma/vmemcache_store.h"
#include "plasma/plasma_allocator.h"

#include <libvmemcache.h>

//#define CACHE_MAX_SIZE (462 * 1024 * 1024 * 1024L)
#define CACHE_MAX_SIZE (1024 * 1024 * 1024L)
#define CACHE_EXTENT_SIZE 512

namespace plasma {

// Connect here is like something initial
Status VmemcacheStore::Connect(const std::string &endpoint) {
  auto size_start = endpoint.find("size:") + 5;
  auto size_end = endpoint.size();
   ARROW_LOG(DEBUG) << endpoint << "start:"<< size_start << "end:" <<size_end;
  std::string sizeStr = endpoint.substr(size_start, size_end);
  unsigned long long size = std::stoull(sizeStr);
  if(size == 0) size = CACHE_MAX_SIZE;
  ARROW_LOG(DEBUG) << "vmemcache size is " << size;
  for (int i = 0; i < totalNumaNodes; i++) {
    // initial vmemcache on numa node i
    VMEMcache *cache = vmemcache_new();
    if (!cache) {
      ARROW_LOG(FATAL) << "Initial vmemcache failed!";
      return Status::UnknownError("Initial vmemcache failed!");
    }
    // TODO: how to find path and bind numa?
    std::string s = "/mnt/pmem" + std::to_string(i);
    ARROW_LOG(DEBUG) << "initial vmemcache on " << s << ", size"
                     << size << ", extent size" << CACHE_EXTENT_SIZE;

    if (vmemcache_set_size(cache, size)) {
      ARROW_LOG(DEBUG) << "vmemcache_set_size error:" << vmemcache_errormsg();
      ARROW_LOG(FATAL) << "vmemcache_set_size failed!";
    }

    if (vmemcache_set_extent_size(cache, CACHE_EXTENT_SIZE)) {
      ARROW_LOG(DEBUG) << "vmemcache_set_extent_size error:"
                       << vmemcache_errormsg();
      ARROW_LOG(FATAL) << "vmemcache_set_extent_size failed!";
    }

    if (vmemcache_add(cache, s.c_str()))
      ARROW_LOG(FATAL) << "Initial vmemcache failed!" << vmemcache_errormsg();

    caches.push_back(cache);

    std::shared_ptr<numaThreadPool> pool(new numaThreadPool(i, threadInPools));
    threadPools.push_back(pool);

    ARROW_LOG(DEBUG) << "initial vmemcache success!";

    srand((unsigned int)time(NULL));
  }
  // try not use lambda function
    //   threadPools[0]->enqueue( [& ] () {
    //   std::this_thread::sleep_for(std::chrono::milliseconds(10));
    //   while(true) {
    //     if(evictionPolicy_->RemainingCapacity() <= evictionPolicy_->Capacity() / 3 * 2) {
    //       auto tic = std::chrono::steady_clock::now();
    //       std::vector<ObjectID> objIds;
    //       evictionPolicy_->ChooseObjectsToEvict(evictionPolicy_->Capacity() / 3, &objIds);
    //       ARROW_LOG(DEBUG)<<"will evict " << objIds.size() << " objects.";
    //       std::vector<std::future<int>> ret;
    //       for(auto objId : objIds) {
    //         // ARROW_LOG(DEBUG)<<"evict "<< objId.hex();
    //         if(Exist(objId).ok()){
    //           //change states
    //           ARROW_LOG(DEBUG)<<"This obj is already in external store, no need to evict again.";
    //           auto entry = GetObjectTableEntry(evictionPolicy_->getStoreInfo(), objId);
    //           entry->state = ObjectState::PLASMA_EVICTED; 
    //           PlasmaAllocator::Free(entry->pointer, entry->data_size + entry->metadata_size);
    //           entry->pointer = nullptr;
    //           continue;              
    //         }
    //         int node = rand() % totalNumaNodes;
    //         ret.push_back(threadPools[node]->enqueue( [&, objId] () {
    //           auto entry = GetObjectTableEntry(evictionPolicy_->getStoreInfo(), objId);
    //           ARROW_CHECK(entry != nullptr) << "To evict an object it must be in the object table.";
    //           ARROW_CHECK(entry->state == ObjectState::PLASMA_SEALED)
    //             << "To evict an object it must have been sealed.";
    //           ARROW_CHECK(entry->ref_count == 0)
    //             << "To evict an object, there must be no clients currently using it.";
    //           entry->numaNodePostion = node;
    //           entry->state = ObjectState::PLASMA_EVICTED; //does state need lock?
    //           Put({objId}, {std::make_shared<arrow::Buffer>(
    //             entry->pointer, entry->data_size + entry->metadata_size)}, node);
    //           PlasmaAllocator::Free(entry->pointer, entry->data_size + entry->metadata_size);
    //           entry->pointer = nullptr;
    //           // int state = 1 ;
    //           // if(entry->state == ObjectState::PLASMA_SEALED)
    //           //   state = 2;
    //           // else if(entry->state == ObjectState::PLASMA_EVICTED)
    //           //   state = 3;
    //           ARROW_LOG(DEBUG)<<"Return";
    //           return 0;
    //         })
    //         );
    //       }
    //       for (int i=0; i< ret.size(); i++)
    //         ret[i].get();
    //       auto toc = std::chrono::steady_clock::now();
    //       std::chrono::duration<double> time_ = toc - tic;
    //       ARROW_LOG(DEBUG)<<"Eviction done, takes" << time_.count() * 1000 << " ms";
    //     }
    //   }
    // });
  ARROW_LOG(DEBUG) << "vmemcache store start!";

  return Status::OK();
}

Status VmemcacheStore::Put(const std::vector<ObjectID> &ids,
                           const std::vector<std::shared_ptr<Buffer>> &data,
                           int numaId) {
  auto tic = std::chrono::steady_clock::now();
  int total = ids.size();
  for (int i = 0; i < total; i++) {
    if (Exist(ids[i]).ok())
      continue;
    auto cache = caches[numaId];
    int ret = vmemcache_put(cache, ids[i].data(), ids[i].size(), 
      (char *)data[i]->data(), data[i]->size());
    if (ret != 0)
      ARROW_LOG(DEBUG) << "vmemcache_put error:" << vmemcache_errormsg();      
  }
  auto toc = std::chrono::steady_clock::now();
  std::chrono::duration<double> time_ = toc - tic;
  ARROW_LOG(DEBUG) << "Put " << total << " objects takes "
                   << time_.count() * 1000 << " ms";
  return Status::OK();
}

// maintain a thread-pool conatins all numa node threads
Status VmemcacheStore::Put(const std::vector<ObjectID> &ids,
                           const std::vector<std::shared_ptr<Buffer>> &data) {
  auto tic = std::chrono::steady_clock::now();
  int total = ids.size();
  std::vector<std::future<int>> results;
  for (int i = 0; i < total; i++) {
    if (Exist(ids[i]).ok())
      continue;
    // find a random instansce to put
    int numaId = rand() % totalNumaNodes;
    auto pool = threadPools[numaId];
    auto cache = caches[numaId];
    size_t keySize = ids[i].size();
    char *key = new char[keySize];
    memcpy(key, ids[i].data(), keySize);

    putParam *param = new putParam(cache, key, keySize, (char *)data[i]->data(),
                                   data[i]->size());
    results.emplace_back(pool->enqueue([&, param]() {
      if (param != nullptr) {
        auto tic_ = std::chrono::steady_clock::now();
        int ret =
            vmemcache_put(param->cache, (char *)(param->key), param->keySize,
                          (char *)(param->value), param->valueSize);
        // ARROW_LOG(DEBUG) << "put "<< hex((char*)param->key) <<" result is "
        // << ret;
        if (ret != 0)
          ARROW_LOG(DEBUG) << "vmemcache_put error:" << vmemcache_errormsg();
        delete[](char *) param->key;
        delete param;

        auto toc_ = std::chrono::steady_clock::now();
        std::chrono::duration<double> time_ = toc_ - tic_;
        ARROW_LOG(DEBUG) << "Put 1 " << "objects takes "
                   << time_.count() * 1000 << " ms";

        return ret;
      } else {
        // may leak here
        ARROW_LOG(FATAL) << "ptr is null !!!";
        return -1;
      }
    }));
  }

  for (int i = 0; i < (int)results.size(); i++) {
    if (results[i].get() != 0)
      ARROW_LOG(DEBUG) << "Put " << i << " failed";
  }
  auto toc = std::chrono::steady_clock::now();
  std::chrono::duration<double> time_ = toc - tic;
  ARROW_LOG(DEBUG) << "Put " << total << " objects takes "
                   << time_.count() * 1000 << " ms";
  return Status::OK();
}

std::string VmemcacheStore::hex(char *id) {

  char hex[] = "0123456789abcdef";
  std::string result;
  for (int i = 0; i < 20; i++) {
    unsigned char val = *(id + i);
    result.push_back(hex[val >> 4]);
    result.push_back(hex[val & 0xf]);
  }
  return result;
}

Status VmemcacheStore::Get(const std::vector<ObjectID> &ids,
                           std::vector<std::shared_ptr<Buffer>> buffers,
                           ObjectTableEntry *entry) {
  int total = ids.size();
  
  for (int i = 0; i < total; i++) {
    auto id = ids[i];
    auto buffer = buffers[i];
    threadPools[entry->numaNodePostion]->enqueue( [&, entry, id, buffer]() {
      auto cache = caches[entry->numaNodePostion];
      size_t *vSize = new size_t(0);
      int ret = 0;
      ret = vmemcache_get(cache, id.data(), id.size(),
       (void *)buffer->mutable_data(), buffer->size(), 0, vSize);
      // ARROW_LOG(DEBUG) << "vmemcache get returns "<<ret;
      if(ret <= 0) {
        ARROW_LOG(WARNING) << "vmemcache get fails! err msg " << vmemcache_errormsg();
      }
      entry->state = ObjectState::PLASMA_SEALED;
    });
  }

  return Status::OK();
}

Status VmemcacheStore::Get(const std::vector<ObjectID> &ids,
                           std::vector<std::shared_ptr<Buffer>> buffers) {
  auto tic = std::chrono::steady_clock::now();
  int total = ids.size();
  std::vector<std::future<int>> results;
  for (int i = 0; i < total; i++) {
    auto id = ids[i];
    auto buffer = buffers[i];

    size_t valueSize = 0;
    for (int j = 0; j < totalNumaNodes; j++) {
      // ARROW_LOG(DEBUG) << "get objectID " << id.hex();
      size_t keySize = id.size();
      char *key = new char[keySize];
      memcpy(key, id.data(), keySize);
      auto cache = caches[j];
      if (vmemcache_exists(cache, key, keySize, &valueSize) == 1) {
        auto pool = threadPools[j];

        size_t *vSize = new size_t(0);
        // char* value = new char[buffer->size()];
        // fprintf(stderr, "cache %p key %p ksize %zu value %p value_size %zu
        // vSize %p\n", cache,
        //   key, keySize,buffer->mutable_data(), buffer->size(), vSize);
        getParam *param =
            new getParam(cache, key, keySize, (void *)buffer->mutable_data(),
                         buffer->size(), 0, vSize);
        results.emplace_back(pool->enqueue([&, param]() {
          if (param != nullptr) {
            int ret = vmemcache_get(param->cache, param->key, param->key_size,
                                    param->vbuf, param->vbufsize, param->offset,
                                    param->vsize);
            // ARROW_LOG(DEBUG)
            //     << "vmemcache_get " << hex((char *)(param->key)) << " returns "
            //     << ret << " vsize " << *(param->vsize);
            delete[](char *) param->key;
            delete (getParam *)param;
            return ret;
          } else {
            return -1;
          }
        }));
        break;
      } else {
        ARROW_LOG(DEBUG) << id.hex() << " not exist in Vmemcache instance" << j;
      }
    }
  }

  // for (int i = 0; i < total; i++) {
  //   if (results[i].get() <= 0)
  //     ARROW_LOG(DEBUG) << "Get " << i << " failed";
  // }

  auto toc = std::chrono::steady_clock::now();
  std::chrono::duration<double> time_ = toc - tic;
  ARROW_LOG(DEBUG) << "Get " << total << " objects takes "
                   << time_.count() * 1000 << " ms";
  return Status::OK();
}

Status VmemcacheStore::Exist(ObjectID id) {
  for (auto cache : caches) {
    size_t valueSize = 0;
    int ret = vmemcache_exists(cache, id.data(), id.size(), &valueSize);
    // ARROW_LOG(DEBUG) << "object " << id.hex() << " exist return " << ret;
    if (ret == 1) {
      return Status::OK();
    }
  }
  return Status::NotImplemented("aaa");
}

Status VmemcacheStore::RegisterEvictionPolicy(EvictionPolicy* eviction_policy) {
  evictionPolicy_ = eviction_policy;
  return Status::OK();
}

// void VmemcacheStore::Evict(std::vector<ObjectID> &ids, std::vector<std::shared_ptr<Buffer>> &datas) {
//   threadpool.enqueue([]()
//   { //async
//     if(!vmemcache_exists(objectId))
//       vmemcache_put(objectId, data);
//     Allocator.free(data.ptr);
//   }
//   );
// }

REGISTER_EXTERNAL_STORE("vmemcache", VmemcacheStore);

} // namespace plasma

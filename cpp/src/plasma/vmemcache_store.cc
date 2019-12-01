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

#include "arrow/util/logging.h"

#include <libvmemcache.h>
#include "plasma/vmemcache_store.h"

//#define CACHE_MAX_SIZE (462 * 1024 * 1024 * 1024L)
#define CACHE_MAX_SIZE (100 * 1024 * 1024L)
#define CACHE_EXTENT_SIZE 512

namespace plasma {

// Connect here is like something initial
Status VmemcacheStore::Connect(const std::string& endpoint) {
  for (int i = 0; i < totalNumaNodes; i++) {
    // initial vmemcache on numa node i
    VMEMcache* cache = vmemcache_new();
    if (!cache) {
      ARROW_LOG(FATAL) << "Initial vmemcache failed!";
      return Status::UnknownError("Initial vmemcache failed!");
    }
    // TODO: how to find path and bind numa?
    std::string s = "/mnt/pmem" + std::to_string(i);
    if (vmemcache_add(cache, s.c_str())) ARROW_LOG(FATAL) << "Initial vmemcache failed!";
    vmemcache_set_extent_size(cache, CACHE_EXTENT_SIZE);
    vmemcache_set_size(cache, CACHE_MAX_SIZE);
    caches.push_back(cache);

    // initial worker thread on numa node i
    // get a coreID on numa node i
    int coreId = i;
    std::shared_ptr<WorkerThread> thread(new WorkerThread(coreId));
    threads.push_back(thread);
    ARROW_LOG(DEBUG) << "initial vmemcache success!";
  }

  return Status::OK();
}

// maintain a thread-pool conatins all numa node threads
Status VmemcacheStore::Put(const std::vector<ObjectID>& ids,
                           const std::vector<std::shared_ptr<Buffer>>& data) {
  ARROW_LOG(DEBUG) << "call Put";
  auto total = ids.size();
  for (int i = 0; i < total; i++) {
    ARROW_LOG(DEBUG) << "Try Put " << ids[i].hex();
    if (Exist(ids[i]).ok()) continue;
    // find a instansce to put
    int numaId = 0;
    auto thread = threads[numaId];
    auto cache = caches[numaId];
    // fprintf(stderr, "cache ptr %p\n", cache);
    fprintf(stderr, "_id ptr %p\n", ids[i].data());
    // fprintf(stderr, "value ptr %p\n", data[i]->data());

    // ARROW_LOG(DEBUG) << "id ptr is " << ids[i].data() << " value ptr is "
    //  << data[i]->data();
    putParam* param = new putParam(cache, (char*)(ids[i].data()), ids[i].size(),
                                   (char*)data[i]->data(), data[i]->size());
    thread->addJob(
        [](void* param) {
          if (param != nullptr) {
            putParam* p = (putParam*)param;
            // ARROW_LOG(DEBUG) << "id ptr is " << p->key << " value ptr is " << p->value;
            // fprintf(stderr, "_cache ptr %p\n", p->cache);
            fprintf(stderr, "__id ptr %p\n", p->key);
            // fprintf(stderr, "_value ptr %p\n", p->value);
            int ret = vmemcache_put(p->cache, (const void*)(p->key), p->keySize,
                                    (const void*)(p->value), p->valueSize);
            // const char* key = "KEY";
            // const char* value = "VALUE";
            // int ret = vmemcache_put(p->cache, key, 4, value, 6);
            ARROW_LOG(DEBUG) << "put result is " << ret;

            std::this_thread::sleep_for(std::chrono::milliseconds(200));

            size_t valueSize = 0;
            // ret = vmemcache_exists(p->cache, key, 4, &valueSize);
            ret = vmemcache_exists(p->cache, (const void*)(p->key), p->keySize, &valueSize);
            ARROW_LOG(DEBUG) << "exist result is " << ret;

            delete (putParam*)param;
          } else {
            ARROW_LOG(FATAL) << "ptr is null !!!";
          }
        },
        param);
  }
  return Status::OK();
}

Status VmemcacheStore::Get(const std::vector<ObjectID>& ids,
                           std::vector<std::shared_ptr<Buffer>> buffers) {
  int total = ids.size();
  for (int i = 0; i < total; i++) {
    auto id = ids[i];
    auto buffer = buffers[i];

    int numaTotal = caches.size();
    size_t valueSize = 0;
    for (int j = 0; j < numaTotal; j++) {
      ARROW_LOG(DEBUG) << "get objectID " << id.hex();
      if (vmemcache_exists(caches[i], id.data(), id.size(), &valueSize) == 1) {
        ARROW_LOG(DEBUG) << "call vmemcache_get " << id.hex();
        auto thread = threads[i];
        getParam* param = new getParam(caches[i], id.data(), id.size(),
                                       buffer->mutable_data(), buffer->size(), 0);
        thread->addJob(
            [](void* param) {
              if (param != nullptr) {
                auto p = (getParam*)param;
                vmemcache_get(p->cache, p->key, p->key_size, p->vbuf, p->vbufsize,
                              p->offset, p->vsize);
              }
              delete (getParam*)param;
            },
            param);
      }
    }
  }
  return Status::OK();
}

Status VmemcacheStore::Exist(ObjectID id) {
  ARROW_LOG(DEBUG) << "call Exist objectID is " << id.hex();
  for (auto cache : caches) {
    size_t valueSize = 0;
    int ret = vmemcache_exists(cache, id.data(), id.size(), &valueSize);
    ARROW_LOG(DEBUG) << "object " << id.hex() << " exist return " << ret;
    if (ret == 1) {
      return Status::OK();
    }
  }
  return Status::NotImplemented("aaa");
}

REGISTER_EXTERNAL_STORE("vmemcache", VmemcacheStore);

}  // namespace plasma
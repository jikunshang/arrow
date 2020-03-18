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

#include "plasma/eviction_manager.h"

#include <thread>

namespace plasma {

EvictionManager::EvictionManager(EvictionPolicy* eviction_policy,
                                 ExternalStore* external_store)
    : eviction_policy_(eviction_policy), external_store_(external_store) {}

void EvictionManager::start() {
  std::thread t = std::thread([&]() {
    while (running) {
      if (needEvict()) {
        doEvict();
      }
    }
    // log
  });
}

bool EvictionManager::needEvict() {
  // if(used_size > threshold) return true;
  return false;
}

void EvictionManager::doEvict() {
  uint64_t required_size = calculateRequiredSize();

  doEvict(required_size);
}

void EvictionManager::doEvict(const uint64_t required_size) {
  // 1.Choose which objects to evict
  std::vector<ObjectID> object_ids;
  chooseObjects(required_size, object_ids);
  // 2.Put these objects to external store
  doEvict(object_ids);
}

void EvictionManager::doEvict(const std::vector<ObjectID>& object_ids) {}

void EvictionManager::chooseObjects(uint64_t required_size,
                                    std::vector<ObjectID>& object_ids) {}

uint64_t EvictionManager::calculateRequiredSize() { return 0; }

void EvictionManager::stop() { running = false; }

}  // namespace plasma

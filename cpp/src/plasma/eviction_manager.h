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

#ifndef PLASMA_EVICTION_MANAGER_H
#define PLASMA_EVICTION_MANAGER_H

#include <vector>

#include "plasma/common.h"
#include "plasma/eviction_policy.h"
#include "plasma/external_store.h"

namespace plasma {

class EvictionManager {
 public:
  explicit EvictionManager(EvictionPolicy* eviction_policy,
                           ExternalStore* external_store_);

  void start();

  void stop();

  void doEvict();

  void doEvict(const std::vector<ObjectID>& object_ids);

  void doEvict(const uint64_t required_size);

 private:
  bool needEvict();
  uint64_t calculateRequiredSize();
  void chooseObjects(uint64_t required_size, std::vector<ObjectID>& object_ids);
  EvictionPolicy* eviction_policy_;
  ExternalStore* external_store_;
  bool running = true;
};

}  // namespace plasma

#endif  // PLASMA_EVICTION_MANAGER_H
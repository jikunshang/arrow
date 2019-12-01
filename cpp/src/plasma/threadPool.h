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

#include <condition_variable>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <thread>

#include "arrow/util/logging.h"
#include "libvmemcache.h"
#include "plasma/client.h"
#include "plasma/common.h"
#include "plasma/external_store.h"
#include "plasma/vmemcache_store.h"

namespace plasma {

typedef std::function<void(int i, int j)> job_t0;
typedef std::function<void(void*)> job_t;

class WorkerThread {
 public:
  WorkerThread(int _coreId) {
    coreId = _coreId;
    wantExit = false;
    thread = std::unique_ptr<std::thread>(
        new std::thread(std::bind(&WorkerThread::Entry, this, coreId)));
  }

  ~WorkerThread() {
    {
      std::lock_guard<std::mutex> lock(queueMutex);
      wantExit = true;
      queuePending.notify_one();
    }
    thread->join();
  }

  void addJob(job_t job, void* param) {
    std::lock_guard<std::mutex> lock(queueMutex);
    jobQueue.push_back(std::make_pair(job, param));
    queuePending.notify_one();
  }

 private:
  void Entry(int coreId) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(coreId, &cpuset);
    job_t job;
    void* param;

    while (true) {
      {
        std::unique_lock<std::mutex> lock(queueMutex);
        queuePending.wait(lock, [&]() { return wantExit || !jobQueue.empty(); });

        if (wantExit) return;

        job = jobQueue.front().first;
        param = jobQueue.front().second;
        jobQueue.pop_front();
      }
      job(param);
    }
  }

 private:
  std::unique_ptr<std::thread> thread;
  std::condition_variable queuePending;
  std::mutex queueMutex;
  std::list<std::pair<job_t, void*>>
      jobQueue;  // jobQueue contains a std::function and it's param
  bool wantExit;
  int coreId;

  WorkerThread(const WorkerThread&);             // no copying!
  WorkerThread& operator=(const WorkerThread&);  // no copying!
};
}  // namespace plasma
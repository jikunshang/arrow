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

#include <assert.h>
#include <signal.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <chrono>
#include <memory>
#include <thread>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"

#include "plasma/client.h"
#include "plasma/common.h"
#include "plasma/external_store.h"
#include "plasma/plasma.h"
#include "plasma/protocol.h"
#include "plasma/test_util.h"

namespace plasma {

using arrow::internal::TemporaryDir;

std::string external_test_executable; // NOLINT

void AssertObjectBufferEqual(const ObjectBuffer &object_buffer,
                             const std::string &metadata,
                             const std::string &data) {
  arrow::AssertBufferEqual(*object_buffer.metadata, metadata);
  ARROW_LOG(DEBUG) << "metadata";
  arrow::AssertBufferEqual(*object_buffer.data, data);
}

class TestPlasmaStoreWithExternalVmem : public ::testing::Test {
public:
  // TODO(pcm): At the moment, stdout of the test gets mixed up with
  // stdout of the object store. Consider changing that.
  void SetUp() override {
    ARROW_CHECK_OK(TemporaryDir::Make("ext-test-", &temp_dir_));
    store_socket_name_ = temp_dir_->path().ToString() + "store";
    // store_socket_name_ = "/tmp/plasmaStore";
    std::string plasma_directory = external_test_executable.substr(
        0, external_test_executable.find_last_of('/'));
    std::string plasma_command = plasma_directory +
                                 "/plasma-store-server -m 1024000 -e " +
                                 "vmemcache://size:100000000000 -s " + store_socket_name_ +
                                 " 1> /tmp/log.stdout 2> /tmp/log.stderr & " +
                                 "echo $! > " + store_socket_name_ + ".pid";
    PLASMA_CHECK_SYSTEM(system(plasma_command.c_str()));
    ARROW_CHECK_OK(client_.Connect(store_socket_name_, ""));
  }

  void TearDown() override {
    ARROW_CHECK_OK(client_.Disconnect());
    // Kill plasma_store process that we starteds
#ifdef COVERAGE_BUILD
    // Ask plasma_store to exit gracefully and give it time to write out
    // coverage files
    std::string plasma_term_command =
        "kill -TERM `cat " + store_socket_name_ + ".pid` || exit 0";
    PLASMA_CHECK_SYSTEM(system(plasma_term_command.c_str()));
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
#endif
    std::string plasma_kill_command =
        "kill -KILL `cat " + store_socket_name_ + ".pid` || exit 0";
    PLASMA_CHECK_SYSTEM(system(plasma_kill_command.c_str()));
  }

  static void Thread_Func(std::shared_ptr<PlasmaClient> client,
                          std::vector<ObjectID> object_ids, int thread_id,
                          int thread_nums, int objects_per_thread, int64_t data_size) {
    std::chrono::duration<double> time_;

    std::string data(10 * 1024 * 1024, 'x');
    std::string metadata;

    int objStart = (thread_id  ) * objects_per_thread;
    std::vector<ObjectID> object_ids_create;
    for (int i = 0; i < objects_per_thread; i++)
      object_ids_create.push_back(object_ids[objStart + i]);

    auto tic = std::chrono::steady_clock::now();
    for (auto object_id : object_ids_create) {
      std::shared_ptr<Buffer> data_buffer;
      ARROW_CHECK_OK(client->CreateAndSeal(object_id, data, metadata));
    }

    auto toc = std::chrono::steady_clock::now();
    time_ = toc - tic;
    std::cout << "Thread id : " << thread_id << "  write time: " << time_.count() << " s."
              << std::endl;


    // objStart = ((thread_id + 1) % thread_nums) * objects_per_thread;
    // // int objStop = objStart + objects_per_thread - 1;
    // std::vector<ObjectID> object_ids_get;
    // for (int i = 0; i < objects_per_thread; i++)
    //   object_ids_get.push_back(object_ids[objStart + i]);
    // std::vector<ObjectBuffer> get_objects;
    // // start
    // tic = std::chrono::steady_clock::now();
    // std::cout<<thread_id<<std::endl;

    // for(int i =0; i < objects_per_thread; i++){
    //   std::vector<ObjectBuffer> get_objects;
    //   bool has_object = false;
    //   ARROW_CHECK_OK(client->Contains(object_ids_get[i], &has_object));
    //   ASSERT_TRUE(has_object);
    //   ARROW_CHECK_OK(client->Get({object_ids_get[i]}, -1, &get_objects));
    //   ASSERT_EQ(get_objects.size(), 1);
    //   ASSERT_EQ(get_objects[0].device_num, 0);
    //   ASSERT_TRUE(get_objects[0].data);
    //   AssertObjectBufferEqual(get_objects[0], metadata, data);
    //   // ASSERT_EQ(memcmp(get_objects[0].data->data(), origin_buffer, data_size), 0);
    // }

    // toc = std::chrono::steady_clock::now();
    // time_ = toc - tic;
    // std::cout << "Thread id : " << thread_id << " get and compare time: " << time_.count()
    //           << " s." << std::endl;
  }

protected:
  PlasmaClient client_;
  std::unique_ptr<TemporaryDir> temp_dir_;
  std::string store_socket_name_;
};

// TEST_F(TestPlasmaStoreWithExternalVmem, EvictionTest) {
//   std::vector<ObjectID> object_ids;
//   std::string data(100 * 1024, 'x');
//   std::string metadata;
//   for (int i = 0; i < 20; i++) {
//     ObjectID object_id = random_object_id();

//     object_ids.push_back(object_id);
//     // Test for object non-existence.
//     bool has_object;
//     ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
//     ASSERT_FALSE(has_object);

//     // Test for the object being in local Plasma store.
//     // Create and seal the object.
//     ARROW_CHECK_OK(client_.CreateAndSeal(object_id, data, metadata));
//     // Test that the client can get the object.
//     ARROW_CHECK_OK(client_.Contains(object_id, &has_object));
//     ASSERT_TRUE(has_object);
//     std::cerr << "this is " << i << "put" << std::endl;
//   }
//   std::this_thread::sleep_for(std::chrono::milliseconds(2000));
//   for (int i = 1; i < 20; i++) {
//     // Since we are accessing objects sequentially, every object we
//     // access would be a cache "miss" owing to LRU eviction.
//     // Try and access the object from the plasma store first, and then try
//     // external store on failure. This should succeed to fetch the object.
//     // However, it may evict the next few objects.
//     std::cerr << "this is " << i << "get" << std::endl;
//     std::vector<ObjectBuffer> object_buffers;
//     bool has_object = false;
//     ARROW_CHECK_OK(client_.Contains(object_ids[i], &has_object));
//     ASSERT_TRUE(has_object);
//     ARROW_CHECK_OK(client_.Get({object_ids[i]}, -1, &object_buffers));
//     ASSERT_EQ(object_buffers.size(), 1);
//     ASSERT_EQ(object_buffers[0].device_num, 0);
//     ASSERT_TRUE(object_buffers[0].data);
//     AssertObjectBufferEqual(object_buffers[0], metadata, data);
//   }

//   // Make sure we still cannot fetch objects that do not exist
//   std::vector<ObjectBuffer> object_buffers;
//   ARROW_CHECK_OK(client_.Get({random_object_id()}, 100, &object_buffers));
//   ASSERT_EQ(object_buffers.size(), 1);
//   ASSERT_EQ(object_buffers[0].device_num, 0);
//   ASSERT_EQ(object_buffers[0].data, nullptr);
//   ASSERT_EQ(object_buffers[0].metadata, nullptr);
// }

//50 client, 20 objects, 10M per object, total 10GB data, plasma 1GB DRAM
TEST_F(TestPlasmaStoreWithExternalVmem, MultiThreadEvictionTest) {
  int thread_nums = 50;
  int objects_per_thread = 20;
  std::chrono::duration<double> time_;
  std::vector<ObjectID> object_Ids(thread_nums * objects_per_thread);
  for (long unsigned int i = 0; i < object_Ids.size(); i++)
    object_Ids[i] = random_object_id();
  std::vector<std::shared_ptr<PlasmaClient>> clients;
  for (int i = 0; i < thread_nums; i++) {
    auto client_ = std::make_shared<PlasmaClient>();
    clients.push_back(client_);
    client_->Connect(store_socket_name_, "");
  }
  int64_t data_size = (int64_t)10 * 1024 * 1024;
  std::vector<std::thread> threads(thread_nums);

  auto tic = std::chrono::steady_clock::now();
  for (int i = 0; i < thread_nums; i++) {
    threads[i] = std::thread(&TestPlasmaStoreWithExternalVmem::Thread_Func, clients[i], object_Ids, i,
                             thread_nums, objects_per_thread, data_size);
  }

  for (int i = 0; i < thread_nums; i++) threads[i].join();
  auto toc = std::chrono::steady_clock::now();
  time_ = toc - tic;
  std::cout << "total write time: " << time_.count() << " ms" << std::endl;

}
} // namespace plasma

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  plasma::external_test_executable = std::string(argv[0]);
  return RUN_ALL_TESTS();
}

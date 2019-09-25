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
#include "plasma/plasma.h"
#include "plasma/protocol.h"
#include "plasma/test_util.h"

namespace plasma {

using arrow::internal::TemporaryDir;

std::string test_executable;  // NOLINT

void AssertObjectBufferEqual(const ObjectBuffer& object_buffer,
                             const std::vector<uint8_t>& metadata,
                             const std::vector<uint8_t>& data) {
  arrow::AssertBufferEqual(*object_buffer.metadata, metadata);
  arrow::AssertBufferEqual(*object_buffer.data, data);
}

class TestPlasmaStoreAEP : public ::testing::Test {
 public:
  // TODO(pcm): At the moment, stdout of the test gets mixed up with
  // stdout of the object store. Consider changing that.

  void SetUp() {
    ARROW_CHECK_OK(TemporaryDir::Make("cli-test-", &temp_dir_));
    store_socket_name_ = temp_dir_->path().ToString() + "store";

    std::string plasma_directory =
        test_executable.substr(0, test_executable.find_last_of("/"));
    std::string plasma_command = plasma_directory +
                                 "/plasma-store-server -m 400000000000 -s " +
                                 store_socket_name_ + " -d /mnt/pmem0/plasma/" +
                                 //" 1> /dev/null 2> /dev/null  " +
                                 " & " + "echo $! > " + store_socket_name_ + ".pid";
    PLASMA_CHECK_SYSTEM(system(plasma_command.c_str()));
    ARROW_CHECK_OK(client_.Connect(store_socket_name_, ""));
    ARROW_CHECK_OK(client2_.Connect(store_socket_name_, ""));
  }

  virtual void TearDown() {
    ARROW_CHECK_OK(client_.Disconnect());
    ARROW_CHECK_OK(client2_.Disconnect());
    // Kill plasma_store process that we started
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

  void CreateObject(PlasmaClient& client, const ObjectID& object_id,
                    const std::vector<uint8_t>& metadata,
                    const std::vector<uint8_t>& data, bool release = true) {
    std::shared_ptr<Buffer> data_buffer;
    ARROW_CHECK_OK(client.Create(object_id, data.size(), &metadata[0], metadata.size(),
                                 &data_buffer));
    for (size_t i = 0; i < data.size(); i++) {
      data_buffer->mutable_data()[i] = data[i];
    }
    ARROW_CHECK_OK(client.Seal(object_id));
    if (release) {
      ARROW_CHECK_OK(client.Release(object_id));
    }
  }

 protected:
  PlasmaClient client_;
  PlasmaClient client2_;
  std::unique_ptr<TemporaryDir> temp_dir_;
  std::string store_socket_name_;
};

TEST_F(TestPlasmaStoreAEP, SingleThreadBenchMark) {
  // create 1 client and create 100 large objects (2GB per object)
  std::vector<ObjectID> object_ids(100);
  std::vector<std::shared_ptr<Buffer>> data_buffers;
  std::chrono::duration<double> time_;
  auto client = std::make_shared<PlasmaClient>();
  auto client2 = std::make_shared<PlasmaClient>();
  ARROW_CHECK_OK(client->Connect(store_socket_name_, ""));

  // create 10 objects
  auto tic = std::chrono::steady_clock::now();
  int64_t data_size = (int64_t)2 * 1024 * 1024 * 1024;
  uint8_t metadata[] = {5};
  int64_t metadata_size = sizeof(metadata);
  for (int i = 0; i < 100; i++) {
    auto object_id = random_object_id();
    object_ids[i] = object_id;
    std::shared_ptr<Buffer> data;
    ARROW_CHECK_OK(client->Create(object_id, data_size, metadata, metadata_size, &data));
    data_buffers.push_back(data);
  }

  auto toc = std::chrono::steady_clock::now();
  time_ = toc - tic;
  std::cout << "Create time: " << time_.count() << " ms" << std::endl;

  uint8_t* origin_buffer = (uint8_t*)malloc(data_size);
  for (int64_t i = 0; i < data_size; i++) origin_buffer[i] = i % 256;
  // write data to objects
  tic = std::chrono::steady_clock::now();
  for (auto data : data_buffers) memcpy(data->mutable_data(), origin_buffer, data_size);
  toc = std::chrono::steady_clock::now();
  time_ = toc - tic;
  std::cout << "Write time: " << time_.count() << " ms" << std::endl;

  // seal objects
  tic = std::chrono::steady_clock::now();
  for (auto object_id : object_ids) ARROW_CHECK_OK(client->Seal(object_id));
  toc = std::chrono::steady_clock::now();
  time_ = toc - tic;
  std::cout << "Seal time: " << time_.count() << " ms" << std::endl;

  ARROW_CHECK_OK(client2->Connect(store_socket_name_, ""));
  std::vector<ObjectBuffer> get_objects;
  // client2 get objects
  tic = std::chrono::steady_clock::now();
  client2->Get(object_ids, -1, &get_objects);
  //  for(auto object_id : object_ids)
  //  {
  //    ObjectBuffer obj_buf;
  //    client2->Get(object_id,1, -1, &obj_buf);
  //    get_objects.push_back(obj_buf);
  //  }
  toc = std::chrono::steady_clock::now();
  time_ = toc - tic;
  std::cout << "Get time: " << time_.count() << " ms" << std::endl;

  // compare to origin buffer
  tic = std::chrono::steady_clock::now();
  for (auto get_object : get_objects)
    ASSERT_EQ(memcmp(get_object.data->data(), origin_buffer, data_size), 0);
  toc = std::chrono::steady_clock::now();
  time_ = toc - tic;
  std::cout << "compare time: " << time_.count() << " ms" << std::endl;

  ARROW_CHECK_OK(client->Disconnect());
  ARROW_CHECK_OK(client2->Disconnect());
}

TEST_F(TestPlasmaStoreAEP, MultiThreadBenchMark) {
  // create 16 thread and create 10 huge objects, 2GB for each
  // write join read
  int thread_nums = 16;
  int objects_per_thread = 10;
  std::vector<ObjectID> object_Ids(thread_nums * objects_per_thread);
  for( int i=0;i<object_Ids.size(); i++)
    object_Ids[i] = random_object_id();
  std::vector<std::shared_ptr<PlasmaClient>> clients;
  for (int i = 0; i < thread_nums; i++) {
    auto client_ = std::make_shared<PlasmaClient>();
    clients.push_back(client_);
  }

  std::chrono::duration<double> time_;


  // 1.create objects
  auto tic = std::chrono::steady_clock::now();

  auto toc = std::chrono::steady_clock::now();
  time_ = toc -tic;
  std::cout<< "XX time: "<< time_.count() << " s."<<std::endl;


  // 2. create 
  tic = std::chrono::steady_clock::now();

  toc = std::chrono::steady_clock::now();
  time_ = toc -tic;
  std::cout<< "XX time: "<< time_.count() << " s."<<std::endl;

  // 3. seal object
  tic = std::chrono::steady_clock::now();

  toc = std::chrono::steady_clock::now();
  time_ = toc -tic;
  std::cout<< "XX time: "<< time_.count() << " s."<<std::endl;
  
  
  // 4. get and read time
  tic = std::chrono::steady_clock::now();

  toc = std::chrono::steady_clock::now();
  time_ = toc -tic;
  std::cout<< "XX time: "<< time_.count() << " s."<<std::endl;

}

}  // namespace plasma

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  plasma::test_executable = std::string(argv[0]);
  return RUN_ALL_TESTS();
}

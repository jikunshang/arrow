#include <assert.h>
#include <signal.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <chrono>
#include <memory>
#include <thread>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"

#include "plasma/client.h"
#include "plasma/common.h"
#include "plasma/external_store.h"
#include "plasma/plasma.h"
#include "plasma/protocol.h"
#include "plasma/test_util.h"
#include "plasma/vmemcache_store.h"

using arrow::util::ArrowLog;
using arrow::util::ArrowLogLevel;

namespace plasma {

using arrow::internal::TemporaryDir;

std::string external_test_executable;

class TestPlasmaStoreBenchMark : public ::testing::Test {
public:
    void SetUp() override {
        ;
    }

    void TearDown() override {
        ;
    }

};

TEST_F(TestPlasmaStoreBenchMark, EvictionTest) {

    VmemcacheStore vmemcacheStore;
    std::string s= "vmemcache://size:100000000000 ";
    vmemcacheStore.Connect(s);
    std::vector<ObjectID> ids;
    std::vector<std::shared_ptr<Buffer>> buffers;
    int total = 1000;
    int buffer_size = 10*1024*1024;
    for(int i =0; i < total; i++) {
        ids.push_back(random_object_id());
        uint8_t* ptr = (uint8_t*)malloc(buffer_size);
        memset(ptr, i % 256, buffer_size );
        buffers.push_back(std::make_shared<arrow::Buffer>(ptr, buffer_size));
    }
    std::vector<std::shared_ptr<Buffer>> getBuffers;
    vmemcacheStore.Put(ids, buffers);
    for(int i=0; i< total; i++) {
        uint8_t* ptr = (uint8_t*)malloc(buffer_size);
        getBuffers.emplace_back(new arrow::MutableBuffer(ptr, buffer_size));
    }
    vmemcacheStore.Get(ids, getBuffers);

    for(int i=0; i< total; i++) {
        if(memcmp(buffers[i]->data(), getBuffers[i]->data(), buffer_size) !=0 )
            std::cout<<"vmemcache_get error!"<<std::endl;
    }

}
}

int main(int argc, char **argv) {
    ArrowLog::StartArrowLog(argv[0], ArrowLogLevel::ARROW_DEBUG);
    ArrowLog::InstallFailureSignalHandler();
    ::testing::InitGoogleTest(&argc, argv);
    plasma::external_test_executable = std::string(argv[0]);
    return RUN_ALL_TESTS();

}




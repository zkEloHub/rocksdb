//
//
//

#pragma once

#include <fcntl.h>
#include <libpmem.h>

#include "common.h"
#include "bitmap.h"
#include "my_log.h"

namespace rocksdb {

// 1. NVM 空间预分配
// 2. 以 table 形式管理 NVM 空间 (利用 bitmap, table 申请/释放)
class PersistentSstable {
 public:
   PersistentSstable(std::string &path, uint64_t each_size,
                             uint64_t number) {
    char* pmemaddr=nullptr;
    size_t mapped_len;
    int is_pmem;
    uint64_t total_size = each_size * number;   // byte

    bool file_exist = false;
    if(nvm_file_exists(path.c_str()) == 0) { //文件存在
      file_exist = true;
    }
    pmemaddr = (char *)(pmem_map_file(path.c_str(), total_size,
                                                PMEM_FILE_CREATE, 0666,
                                                &mapped_len, &is_pmem));
    RECORD_LOG("%s PersistentSstable path:%s map_len:%f MB is:%d each_size:%f MB number:%lu total_size:%f MB\n",file_exist ? "open" : "crest",
      path.c_str(),mapped_len/1048576.0,is_pmem,each_size/1048576.0,number,total_size/1048576.0);
    assert(pmemaddr != nullptr);
    raw_ = pmemaddr;
    bitmap_ = new BitMap(number);
    each_size_ = each_size;
    num_ = number;
    use_num_ = 0;
    mapped_len_ = mapped_len;
    is_pmem_ = is_pmem;
  }
  ~PersistentSstable() {
    //Sync();
    delete bitmap_;
    pmem_unmap(raw_, mapped_len_);
  }
  char* AllocSstable(int& index) {
    char* alloc = nullptr;
    for(unsigned int i=0; i < num_; i++){
      if(bitmap_->get(i) == 0) {
        index = (int)i;
        alloc = raw_ + index * each_size_;
        bitmap_->set(index);
        use_num_ = use_num_ + 1;
        return alloc;
      }
    }
    return alloc;
  }

  char* GetIndexPtr(int index) {
    assert((uint64_t)index < num_ && index >= 0);
    return raw_ + index * each_size_;
  }

  void DeleteSstable(int index) {
    size_t pos = index;
    bitmap_->clr(pos);
    use_num_ = use_num_ - 1;
  }
  void Sync(){
    if (is_pmem_)
		pmem_persist(raw_, mapped_len_);
	else
		pmem_msync(raw_, mapped_len_);
  }

  uint64_t GetUseNum() { return use_num_; }
  uint64_t GetNum() { return num_; }
  void Reset() {
    bitmap_->reset();
    use_num_ = 0;
  }
  uint64_t GetEachSize(){ return each_size_;}

  void RecoverAddSstable(int index) {
    if ((uint64_t)index >= num_) {
      RECORD_LOG("[Error] recover file index >= nvm index!\n");
      return;
    }
    if (bitmap_->get(index) != 0 ) {
      RECORD_LOG("[Error] recover file index is used !\n");
      return;
    }
    bitmap_->set(index);
    use_num_ = use_num_ + 1;
  }

 private:
  char* raw_;           // nvm 起始地址
  BitMap* bitmap_;      // 管理 nvm 中 table 使用情况
  size_t mapped_len_;
  int is_pmem_;
  uint64_t total_size_; // each_size_ * num_:  128M * 256 = 32G
  uint64_t each_size_;  // write buffer size + 64 = 64 + 64 = 128M
  uint64_t num_;        // L0 table number: 256:  (Level0_column_compaction_stop_size / write_buffer_size + 1) * 2
  uint64_t use_num_;    // L0 已使用 table 数量
};

}  // namespace rocksdb
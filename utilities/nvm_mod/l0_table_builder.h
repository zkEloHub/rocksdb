//
//功能：
//

#pragma once

#include <memory>
#include <string>

#include "common.h"


#include "sstable_meta.h"
#include "nvm_cf_mod.h"


namespace rocksdb {

class L0TableBuilder{
public:
    L0TableBuilder(NvmCfModule* nvm_cf,
                   FileEntry* file,
                   char* raw);
    ~L0TableBuilder();
    void Add(const Slice& key, const Slice& value);
    Status Finish();

    uint64_t GetFileSize(){
        return offset_;
    }

private:
    NvmCfModule * nvm_cf_;
    FileEntry* file_;
    char* raw_;
    std::vector<KeysMetadata *> keys_;
    uint64_t keys_num_;
    uint64_t offset_;

};

class L0TableBuilderWithBuffer{
public:
    L0TableBuilderWithBuffer(NvmCfModule* nvm_cf,
                   FileEntry* file,
                   char* raw);
    ~L0TableBuilderWithBuffer();
    void Add(const Slice& key, const Slice& value);
    Status Finish();

    uint64_t GetFileSize(){
        return offset_;
    }

    uint64_t GetKeysMetaSize() {
        return keys_meta_size_;
    }


private:
 NvmCfModule* nvm_cf_;
 FileEntry* file_;                  // Table 信息, nvm 分配空间时创建
 char* raw_;                        // nvm 空间起始地址
 std::vector<KeysMetadata*> keys_;  // 记录每个 key 的信息(offset_, size)
 uint64_t keys_num_;
 uint64_t offset_;  // 当前 Table 总的 offset_
 char* buf_;
 uint64_t keys_meta_size_;  // 当前 table 所有 key 的 meta data 大小
 uint64_t max_size_;        // buf的最大值
};

}

#pragma once

#include "common.h"

#include "sstable_meta.h"

#include "db/dbformat.h"

namespace rocksdb {
struct FileMetaData;
class InternalKey;

struct ColumnCompactionItem{
public:
    ColumnCompactionItem(){};
    ~ColumnCompactionItem(){};

    std::vector<FileEntry*> files;  //新的先插入，旧的在后面; 本次 compaction 的 FileEntry 信息
    std::vector<uint64_t> keys_num; //对应files的需要compaction的key个数
    std::vector<uint64_t> keys_size; //对应的key-value的总大小
    uint64_t L0select_size;         // level0 中本次 compaction 的总数据量

    std::vector<FileMetaData*> L0compactionfiles;       // 本次 compaction 的 FileMetaData 信息
    std::vector<FileMetaData*> L1compactionfiles;

    InternalKey L0smallest; //  smallest <= compaction的范围  <= largest       
    InternalKey L0largest;
};


}
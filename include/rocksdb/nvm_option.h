//
//
//

#pragma once

#include <memory>
#include <string>

//#include "db/dbformat.h"

namespace rocksdb {
struct ColumnFamilyOptions;

struct NvmSetup {
  bool use_nvm_module = false;

  //bool reset_nvm_storage = false;

  std::string pmem_path;  //目录

  uint64_t Level0_column_compaction_trigger_size = 7ul * 1024 * 1024 * 1024;  //7G trigger
  uint64_t Level0_column_compaction_slowdown_size = 7ul * 1024 * 1024 * 1024 + 512ul * 1024 * 1024;  //7.5G slowdown
  uint64_t Level0_column_compaction_stop_size = 8ul * 1024 * 1024 * 1024;   //8G stop

  int Column_compaction_no_L1_select_L0 = 4;     //column compaction时没有L1文件交集时,至少选择L0数据量进行column compaction的文件个数
  int Column_compaction_have_L1_select_L0 = 2;   //column compaction时有L1文件交集时,至少选择L0数据量进行column compaction的文件个数
 

  //uint64_t pmem_size = 0;  //主管理模块大小

  NvmSetup& operator=(const NvmSetup& setup) = default;
  NvmSetup() = default;
};

struct NvmCfOptions {
  NvmCfOptions() = delete;

  NvmCfOptions(const std::shared_ptr<NvmSetup> setup,
               uint64_t s_write_buffer_size, int s_max_write_buffer_number,
               int s_level0_stop_writes_trigger,
               uint64_t s_target_file_size_base);

  NvmCfOptions(const std::shared_ptr<NvmSetup> setup, const ColumnFamilyOptions& cf_options);

  ~NvmCfOptions() {}

  bool use_nvm_module;
  //bool reset_nvm_storage;
  std::string pmem_path;
  //uint64_t cf_pmem_size;

  uint64_t Level0_column_compaction_trigger_size = 7ul * 1024 * 1024 * 1024;  //7G trigger
  uint64_t Level0_column_compaction_slowdown_size = 7ul * 1024 * 1024 * 1024 + 512ul * 1024 * 1024;  //7.5G slowdown
  uint64_t Level0_column_compaction_stop_size = 8ul * 1024 * 1024 * 1024;   //8G stop

  int Column_compaction_no_L1_select_L0 = 4;     //column compaction时没有L1文件交集时,至少选择L0数据量进行column compaction的文件个数
  int Column_compaction_have_L1_select_L0 = 2;   //column compaction时有L1文件交集时,至少选择L0数据量进行column compaction的文件个数


  uint64_t write_buffer_size;
  int max_write_buffer_number;
  int level0_stop_writes_trigger;
  uint64_t target_file_size_base;

  int Level0_column_compaction_trigger_file_num = 2096;

};

}  // namespace rocksdb
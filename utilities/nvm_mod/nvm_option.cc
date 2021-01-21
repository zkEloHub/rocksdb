#include "rocksdb/nvm_option.h"
#include "my_log.h"
#include "rocksdb/options.h"

// 初始化 Column compaction 配置
namespace rocksdb {
NvmCfOptions::NvmCfOptions(const std::shared_ptr<NvmSetup> setup,uint64_t s_write_buffer_size,int s_max_write_buffer_number,int s_level0_stop_writes_trigger,uint64_t s_target_file_size_base) {
  use_nvm_module = setup->use_nvm_module;
  //reset_nvm_storage = setup->reset_nvm_storage;
  pmem_path = setup->pmem_path;

  Level0_column_compaction_trigger_size = setup->Level0_column_compaction_trigger_size;
  Level0_column_compaction_slowdown_size = setup->Level0_column_compaction_slowdown_size;  
  Level0_column_compaction_stop_size = setup->Level0_column_compaction_stop_size;

  Column_compaction_no_L1_select_L0 = setup->Column_compaction_no_L1_select_L0;     
  Column_compaction_have_L1_select_L0 = setup->Column_compaction_have_L1_select_L0;


  write_buffer_size = s_write_buffer_size;
  max_write_buffer_number = s_max_write_buffer_number;
  level0_stop_writes_trigger = s_level0_stop_writes_trigger;
  //cf_pmem_size = 1ul * 1024 * 1024 * 1024;
  target_file_size_base = s_target_file_size_base;
  Level0_column_compaction_trigger_file_num = Level0_column_compaction_stop_size / write_buffer_size + 20;
  RECORD_LOG("[Info] Column Family message: use_nvm_module:%d pmem_path:%s write_buffer_size:%f MB \n \
            Level0_column_compaction_trigger_size:%f MB  \n \
            Level0_column_compaction_slowdown_size:%f MB  \n \
            Level0_column_compaction_stop_size:%f MB  \n \
            Column_compaction_no_L1_select_L0:%d \n \
            Column_compaction_have_L1_select_L0:%d \n \
            level0_stop_writes_trigger:%d target_file_size_base:%f MB\n",
    use_nvm_module,pmem_path.c_str(),write_buffer_size/1048576.0, Level0_column_compaction_trigger_size/1048576.0,
    Level0_column_compaction_slowdown_size/1048576.0, Level0_column_compaction_stop_size/1048576.0,
    Column_compaction_no_L1_select_L0, Column_compaction_have_L1_select_L0,
    level0_stop_writes_trigger, target_file_size_base/1048576.0);
}

NvmCfOptions::NvmCfOptions(const std::shared_ptr<NvmSetup> setup,
                           const ColumnFamilyOptions& cf_options)
    : use_nvm_module(setup->use_nvm_module),
      pmem_path(setup->pmem_path),
      Level0_column_compaction_trigger_size(cf_options.Level0_column_compaction_trigger_size),
      Level0_column_compaction_slowdown_size(cf_options.Level0_column_compaction_slowdown_size),
      Level0_column_compaction_stop_size(cf_options.Level0_column_compaction_stop_size),
      Column_compaction_no_L1_select_L0(setup->Column_compaction_no_L1_select_L0),
      Column_compaction_have_L1_select_L0(setup->Column_compaction_have_L1_select_L0),
      write_buffer_size(cf_options.write_buffer_size),
      max_write_buffer_number(cf_options.max_write_buffer_number),
      level0_stop_writes_trigger(cf_options.level0_stop_writes_trigger),
      target_file_size_base(cf_options.target_file_size_base) {

  if (Level0_column_compaction_trigger_size <= 0) {
    Level0_column_compaction_trigger_size = setup->Level0_column_compaction_trigger_size;
  }
  if (Level0_column_compaction_slowdown_size <= 0) {
    Level0_column_compaction_slowdown_size = setup->Level0_column_compaction_slowdown_size;  
  }
  if (Level0_column_compaction_stop_size <= 0) {
    Level0_column_compaction_stop_size = setup->Level0_column_compaction_stop_size;
  }

  if (cf_options.with_num_trigger) {
      Level0_column_compaction_trigger_file_num = (Level0_column_compaction_stop_size / write_buffer_size) * 1.5;
  }

  RECORD_LOG("[Info] Column Family message: use_nvm_module:%d pmem_path:%s write_buffer_size:%f MB \n \
            Level0_column_compaction_trigger_size:%f MB  \n \
            Level0_column_compaction_slowdown_size:%f MB  \n \
            Level0_column_compaction_stop_size:%f MB  \n \
            Column_compaction_no_L1_select_L0:%d \n \
            Column_compaction_have_L1_select_L0:%d \n \
            level0_stop_writes_trigger:%d target_file_size_base:%f MB\n",
    use_nvm_module,pmem_path.c_str(),write_buffer_size/1048576.0, Level0_column_compaction_trigger_size/1048576.0,
    Level0_column_compaction_slowdown_size/1048576.0, Level0_column_compaction_stop_size/1048576.0,
    Column_compaction_no_L1_select_L0, Column_compaction_have_L1_select_L0,
    level0_stop_writes_trigger, target_file_size_base/1048576.0);
}

}  // namespace rocksdb
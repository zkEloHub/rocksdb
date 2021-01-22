#include "nvm_cf_mod.h"

#include "db/version_set.h"
#include "column_compaction_iterator.h"
#include "global_statistic.h"

namespace rocksdb {

// 1. PMDK 初始化 column family
// 2. 初始化 sst_meta_ (记录所有 Level0(matrix container) 的 sst(rowtable) 信息)
NvmCfModule::NvmCfModule(NvmCfOptions* nvmcfoption, const std::string& cf_name,
                         uint32_t cf_id, const InternalKeyComparator* icmp)
    : nvmcfoption_(nvmcfoption),open_by_creat_(true),icmp_(icmp), cf_name_(cf_name), cf_id_(cf_id) {
  
  nvm_dir_no_exists_and_creat(nvmcfoption_->pmem_path);
  char buf[100];
  snprintf(buf, sizeof(buf), "%s/cf_%u_%s_sstable.pool",
           nvmcfoption_->pmem_path.c_str(), cf_id, cf_name.c_str());
  std::string pol_path(buf, strlen(buf));

  if(nvm_file_exists(pol_path.c_str()) == 0) { //文件存在
    open_by_creat_ = false;
  }
  // buffer size: 64M
  // stop_size: 8192M
  uint64_t level0_table_num = (nvmcfoption_->Level0_column_compaction_stop_size / nvmcfoption_->write_buffer_size + 1)*2;
  ptr_sst_ = new PersistentSstable(
      pol_path,
      nvmcfoption_->write_buffer_size + nvmcfoption_->meta_buffer_size,
      level0_table_num);

  sst_meta_ = new SstableMetadata(icmp_, nvmcfoption_);
}

NvmCfModule::~NvmCfModule() {
  RECORD_LOG("compaction_num:%lu pick_compaction_time:%lu l0_get_time:%lu l0_find_num:%lu\n",global_stats.compaction_num, global_stats.pick_compaction_time,
      global_stats.l0_get_time, global_stats.l0_find_num);
  RECORD_LOG("NvmCfModule:close\n");
  delete ptr_sst_;
  delete sst_meta_;
}
void NvmCfModule::Delete() {}

// 1. 从 nvm 中分配空间(ptr_sst_)
// 2. 更新 sst_meta_ 信息
bool NvmCfModule::AddL0TableRoom(uint64_t filenum, char** raw,
                    FileEntry** file) {
  int index = -1;
  char* tmp = nullptr;
  tmp = ptr_sst_->AllocSstable(index);
  if (index == -1 || tmp == nullptr) {
    RECORD_LOG("error:AddL0TableRoom AllocSstable error!, num_: %lu, used num: "
        "%lu\n",ptr_sst_->GetNum(), ptr_sst_->GetUseNum());
    return false;
  }
  *raw = tmp;
  FileEntry* filetmp = nullptr;
  filetmp = sst_meta_->AddFile(filenum, index);
  if (filetmp == nullptr) {
    RECORD_LOG("[Error] AddL0TableRoom AddFile error!\n");
    return false;
  } else {
    *file = filetmp;
  }
  RECORD_LOG("nvm_path: %s, cf_id: %ld, cf_name: %s ;add L0 table:%lu index:%d\n", GetNvmCfOptions()->pmem_path.c_str(), GetCfId(), GetCfName().c_str(), filenum,index);
  return true;
}

// [Important]
ColumnCompactionItem* NvmCfModule::PickColumnCompaction(VersionStorageInfo* vstorage) {
  ColumnCompactionItem* c = nullptr;
  //todo:选择数据
  // 根据 vstorage 中的 L0Files, 更新 sst_meta_ 的 compaction_files 信息
  auto L0files = vstorage->LevelFiles(0);
  UpdateCompactionState(L0files);     // sst_meta_

  uint64_t comfiles_num = sst_meta_->compaction_files.size();   //compaction files number
  if(comfiles_num == 0){
    RECORD_LOG("[Error] comfiles_num == 0, l0:%ld\n",L0files.size());
    return nullptr;
  }
  
  std::vector<FileEntry*> comfiles;                  //compaction files
  std::vector<uint64_t> first_key_indexs;            //file <-> first_key_indexs
  // 记录每个 file 需要 compaction 的 key num, key size
  uint64_t *keys_num = new uint64_t[comfiles_num];
  uint64_t *keys_size = new uint64_t[comfiles_num];
  comfiles.reserve(comfiles_num);
  first_key_indexs.reserve(comfiles_num);

  FileEntry* tmp = nullptr;
  unsigned int j = 0;
  bool find_file = false;
  
  // 1. 根据 sst_meta_ 中记录的 number 信息(compaction_files), 找到对应的 FileEntry (comfiles)
  // 2. 检验 FileEntry->filenum 是否在 L0files 中
  // 3. 记录 first_key_index
  // TODO: UpdateCompactionState() 已经检验一致，是否多线程
  for(unsigned int i = 0; i < comfiles_num; i++) {
    tmp = FindFile(sst_meta_->compaction_files[i]);      // sst_meta_->compaction_files: 记录所有 file_ 的 number.
    assert(tmp != nullptr);
    comfiles.push_back(tmp);

    //j = 0;
    find_file = false;
    while(j < L0files.size()) {
      if(L0files.at(j)->fd.GetNumber() == tmp->filenum) {
        find_file = true;
        break;
      }
      j++;
    }
    if(find_file) {
      first_key_indexs.push_back(L0files.at(j)->first_key_index);  //对应文件的 first_key_index 插入
      j++;
    }
    else {
      RECORD_LOG("[Error] L0files no find_file:%ld",tmp->filenum);
      return nullptr;    //未找到对应文件，错误
    }

    // first_key_indexs.push_back(L0files.at(i)->first_key_index);  //对应文件的 first_key_index 插入
  }
  
  RECORD_LOG("compaction comfiles:[");
  for(unsigned int i = 0; i < comfiles_num; i++) {
    RECORD_LOG("%ld ",comfiles[i]->filenum);
    keys_num[i] = 0;
    keys_size[i] = 0;
  }
  RECORD_LOG("]\n");

  // 记录所有需要 compaction 的 FileEntry, key_num, key_size, L0-key_range, L0 file, L1 file
  c = new ColumnCompactionItem();

  // 根据要 compaction 的 file: comfiles, 生成 KeysMergeIterator
  uint64_t all_comption_size = 0;
  auto user_comparator = icmp_->user_comparator(); // 比较只根据 user key 比较
  KeysMergeIterator* k_iter = new KeysMergeIterator(&comfiles, &first_key_indexs, user_comparator);
  
  // 与 L1 有交集; 与 L1 无交集
  uint64_t L1NoneCompactionSizeStop = nvmcfoption_->Column_compaction_no_L1_select_L0 * nvmcfoption_->target_file_size_base - 6ul*1024*1024 * nvmcfoption_->Column_compaction_no_L1_select_L0;  //每个文件减去6MB是为了防止小文件的产生
  uint64_t L1HaveCompactionSizeStop = nvmcfoption_->Column_compaction_have_L1_select_L0 * nvmcfoption_->target_file_size_base - 6ul*1024*1024 * nvmcfoption_->Column_compaction_have_L1_select_L0;
  int files_index = -1;
  int keys_index = -1;
  uint64_t itemsize = 0;

  // 记录 L0 的 key range
  InternalKey minsmallest; //  smallest <= comfiles  <= largest       
  InternalKey maxlargest;

  k_iter->SeekToLast();     // current_ 将指向 key 最大的 file_ 下标
  if(k_iter->Valid()){
    k_iter->GetCurret(files_index, keys_index);
    maxlargest = comfiles.at(files_index)->keys_meta[keys_index].key;
  }

  k_iter->SeekToFirst();   // current_ 将指向 key 最小的 file_ 下标
  if(k_iter->Valid()) {
    k_iter->GetCurret(files_index, keys_index);
    minsmallest = comfiles.at(files_index)->keys_meta[keys_index].key;
    RECORD_LOG("L0 minsmallest:%s maxlargest:%s\n",minsmallest.DebugString(true).c_str(),maxlargest.DebugString(true).c_str());
  }

  auto L1files = vstorage->LevelFiles(1);
  for(unsigned int i = 0;i < L1files.size();i++){
    RECORD_LOG("L1table:%lu [%s-%s]\n",L1files.at(i)->fd.GetNumber(), L1files.at(i)->smallest.DebugString(true).c_str(), L1files.at(i)->largest.DebugString(true).c_str());
  }

  // 根据 level-0 的 key range, 获取 level-1 中与 level-0 有 overlap 的 files
  std::vector<FileMetaData*> L1overlapfiles;
  vstorage->GetOverlappingInputs(1, &minsmallest, &maxlargest, &L1overlapfiles);
  for(unsigned int i = 0;i < L1overlapfiles.size();i++){
    RECORD_LOG("L1over:%lu [%s-%s]\n",L1overlapfiles.at(i)->fd.GetNumber(),L1overlapfiles.at(i)->smallest.DebugString(true).c_str(),L1overlapfiles.at(i)->largest.DebugString(true).c_str());
  }

  c->L0smallest = minsmallest;
  if(L1overlapfiles.size() == 0) {  //L1没有交集文件，根据数据量选取
    RECORD_LOG("[INFO] column compaction: nvm cf pick no L1\n");
    // if(k_iter->Valid()) {
    //   k_iter->GetCurret(files_index, keys_index);
    //   c->L0smallest = comfiles.at(files_index)->keys_meta[keys_index].key;      // 前面 current_ 已经指向最小 key 的 file_
    // }

    // compaction 的 key 数据量达到 L1NoneCompactionSizeStop 退出
    while(k_iter->Valid()) {
      // TODO: 只有当 all_comption_size >= L1NoneCompactionSizeStop 时才继续加入重复 key ?
      if(all_comption_size >= L1NoneCompactionSizeStop) {
        int next_files_index, next_keys_index;
        k_iter->GetCurret(next_files_index, next_keys_index);

        if(user_comparator->Compare(ExtractUserKey(comfiles[files_index]->keys_meta[keys_index].key.Encode()), ExtractUserKey(comfiles[next_files_index]->keys_meta[next_keys_index].key.Encode())) == 0) {
          //如果与下一个key相等，则继续加入compaction，不论超过大小与否
          k_iter->GetCurret(files_index, keys_index);
          itemsize = comfiles.at(files_index)->keys_meta[keys_index].size;
          keys_num[files_index]++;
          keys_size[files_index] += itemsize;
          all_comption_size += itemsize;
          k_iter->Next();
          continue;
        }
        c->L0largest = comfiles.at(files_index)->keys_meta[keys_index].key;
        break;
      }
      k_iter->GetCurret(files_index,keys_index);
      itemsize = comfiles.at(files_index)->keys_meta[keys_index].size;
      keys_num[files_index]++;
      keys_size[files_index] += itemsize;
      all_comption_size += itemsize;
      k_iter->Next();
    }
    // if(all_comption_size > 0 && !k_iter->Valid()) {
    //   c->L0largest = comfiles.at(files_index)->keys_meta[keys_index].key;
    // }
    // // 将需要 compaction 的 FileEntry 以及对应的 keys_num, keys_size 加入 ColumnCompactionItem
    // for(unsigned int index = 0; index < comfiles_num; index++) {
    //   if(keys_num[index] != 0){
    //     tmp = comfiles.at(index);
    //     c->files.push_back(tmp);
    //     c->keys_num.push_back(keys_num[index]);
    //     c->keys_size.push_back(keys_size[index]);
    //   }
    // }
    // c->L0select_size = all_comption_size;

    // FileMetaData* ftmp = nullptr;
    // uint64_t filenum = 0;
    // L0files = vstorage->LevelFiles(0);
    // // c->files: 需要 compaction 的 FileEntry
    // // c->L0compactionfiles: 需要 compaction 的 FileMetaData
    // for(unsigned int i = 0; i < c->files.size(); i++) {
    //   ftmp = nullptr;
    //   filenum = c->files.at(i)->filenum;
    //   j = 0;
    //   while(j < L0files.size()) {
    //     if(L0files.at(j)->fd.GetNumber() == filenum) {
    //       ftmp = L0files.at(j);
    //       break;
    //     }
    //     j++;
    //   }
    //   if(ftmp != nullptr) {
    //     c->L0compactionfiles.push_back(ftmp);
    //   }
    //   else {
    //     RECORD_LOG("[Error] no find L0:%lu table!\n",filenum);
    //   }
    // }
    // delete k_iter;
    // delete []keys_num;
    // delete []keys_size;
    // return c;
  }
  else {  //L1有交集文件，根据文件分隔选取
    RECORD_LOG("[INFO] column compaction: nvm cf pick have L1\n");
    std::vector<InternalKey> L1Ranges;      // 与 L0 有交集的 L1 文件组成分隔范围  ---|f1.smallest|---|f1.largest|---|f2.smallest|---|f2.largest|---
    for(unsigned int i = 0; i < L1overlapfiles.size(); i++) {
      L1Ranges.emplace_back(L1overlapfiles.at(i)->smallest);
      L1Ranges.emplace_back(L1overlapfiles.at(i)->largest);
    }
    unsigned int L1Range_index = 0;
    InternalKey key_current;
    
    // if(k_iter->Valid()) {
    //   k_iter->GetCurret(files_index, keys_index);
    //   c->L0smallest = comfiles.at(files_index)->keys_meta[keys_index].key;
    // }
    while(k_iter->Valid()) {
      k_iter->GetCurret(files_index, keys_index);
      key_current = comfiles.at(files_index)->keys_meta[keys_index].key;
      if( ((L1Range_index != L1Ranges.size()) && (L1Range_index % 2 == 0) && (user_comparator->Compare(ExtractUserKey(key_current.Encode()), ExtractUserKey(L1Ranges.at(L1Range_index).Encode())) >= 0)) || \
            ((L1Range_index % 2 == 1) && (user_comparator->Compare(ExtractUserKey(key_current.Encode()), ExtractUserKey(L1Ranges.at(L1Range_index).Encode())) > 0)) ) {
        if((L1Range_index % 2 == 1) && all_comption_size >= L1HaveCompactionSizeStop) {  //在文件范围内时，超过边界满足大小即可
          c->L0largest = comfiles.at(files_index)->keys_meta[keys_index].key;
          break;
        }
        L1Range_index++;
        continue;
      }

      itemsize = comfiles.at(files_index)->keys_meta[keys_index].size;
      keys_num[files_index]++;
      keys_size[files_index] += itemsize;
      all_comption_size += itemsize;
      if((L1Range_index % 2 == 0) && all_comption_size >= L1HaveCompactionSizeStop) {  //不在文件范围内时，满足数据量即可
          c->L0largest = comfiles.at(files_index)->keys_meta[keys_index].key;
          break;
      }
      k_iter->Next();
    }
    // if(all_comption_size > 0 && !k_iter->Valid()) {
    //   c->L0largest = comfiles.at(files_index)->keys_meta[keys_index].key;
    // }

    // for(unsigned int index = 0; index < comfiles_num; index++) {
    //   if(keys_num[index] != 0) {
    //     tmp = comfiles.at(index);
    //     c->files.push_back(tmp);
    //     c->keys_num.push_back(keys_num[index]);
    //     c->keys_size.push_back(keys_size[index]);
    //   }
    // }
    // c->L0select_size = all_comption_size;

    // FileMetaData* ftmp = nullptr;
    // uint64_t filenum = 0;
    // L0files = vstorage->LevelFiles(0);
    // for(unsigned int i = 0;i < c->files.size();i++){
    //   ftmp = nullptr;
    //   filenum = c->files.at(i)->filenum;
    //   j = 0;
    //   while(j < L0files.size()) {
    //     if(L0files.at(j)->fd.GetNumber() == filenum) {
    //       ftmp = L0files.at(j);
    //       break;
    //     }
    //     j++;
    //   }
    //   if(ftmp != nullptr) {
    //     c->L0compactionfiles.push_back(ftmp);
    //   }
    //   else{
    //     RECORD_LOG("[Error] no find L0:%lu table!\n",filenum);
    //   }
    // }
    RECORD_LOG("L1Range_index:%u\n",L1Range_index);
    for(unsigned int i = 0;i < ((L1Range_index + 1)/2);i++) {
      c->L1compactionfiles.push_back(L1overlapfiles.at(i));
    }
    // delete k_iter;
    // delete []keys_num;
    // delete []keys_size;
    // return c;
  }
  if(all_comption_size > 0 && !k_iter->Valid()) {
    c->L0largest = comfiles.at(files_index)->keys_meta[keys_index].key;
  }
  // 将需要 compaction 的 FileEntry 以及对应的 keys_num, keys_size 加入 ColumnCompactionItem
  for(unsigned int index = 0; index < comfiles_num; index++) {
    if(keys_num[index] != 0){
      tmp = comfiles.at(index);
      c->files.push_back(tmp);
      c->keys_num.push_back(keys_num[index]);
      c->keys_size.push_back(keys_size[index]);
    }
  }
  c->L0select_size = all_comption_size;

  FileMetaData* ftmp = nullptr;
  uint64_t filenum = 0;
  L0files = vstorage->LevelFiles(0);
  // c->files: 需要 compaction 的 FileEntry
  // c->L0compactionfiles: 需要 compaction 的 FileMetaData
  for(unsigned int i = 0; i < c->files.size(); i++) {
    ftmp = nullptr;
    filenum = c->files.at(i)->filenum;
    j = 0;
    while(j < L0files.size()) {
      if(L0files.at(j)->fd.GetNumber() == filenum) {
        ftmp = L0files.at(j);
        break;
      }
      j++;
    }
    if(ftmp != nullptr) {
      c->L0compactionfiles.push_back(ftmp);
    }
    else {
      RECORD_LOG("[Error] no find L0:%lu table!\n",filenum);
    }
  }
  delete k_iter;
  delete []keys_num;
  delete []keys_size;
  return c;
}

// 返回 compaction score
double NvmCfModule::GetCompactionScore() {
  double score = 0;
  uint64_t compactionfilenum = sst_meta_->GetFilesNumber();
  score = 1.0 + (double)compactionfilenum/4;
  return score;
}

// 从 ptr_sst_(nvm) 和 sst_meta_(元数据) 中移除 filenumber 对应信息
void NvmCfModule::DeleteL0file(uint64_t filenumber) {
  FileEntry* tmp = sst_meta_->FindFile(filenumber);
  if(tmp == nullptr) return;
  RECORD_LOG("[INFO] delete l0 table:%lu\n",filenumber);
  ptr_sst_->DeleteSstable(tmp->sstable_index);
  sst_meta_->DeteleFile(filenumber);
}

// 1. 从 sst_meta_ 中获取当前匹配的 L0files: findfiles
// 2. 遍历所有 findfiles, 知道找到 lkey 对应的 value
bool NvmCfModule::Get(VersionStorageInfo* vstorage, Status *s, const LookupKey &lkey, std::string *value) {
  auto L0files = vstorage->LevelFiles(0);
  std::vector<FileEntry*> findfiles;
  std::vector<uint64_t> first_key_indexs;

  sst_meta_->GetL0Files(L0files, findfiles);
  for(unsigned int i = 0; i < L0files.size(); i++) {
    first_key_indexs.push_back(L0files.at(i)->first_key_index);
  }
  
  Slice user_key = lkey.user_key();
  FileEntry* file = nullptr;
  int find_index = -1;
  int pre_left = -1;
  int pre_right = -1;
  uint64_t next_file_num = 0;
  for(unsigned int i = 0; i < findfiles.size(); i++) {
    file = findfiles.at(i);
    if(next_file_num != file->filenum) {
      pre_left = -1;
      pre_right = -1;
    }

    int com_result = UserKeyCompareRange(&user_key, &(file->keys_meta[first_key_indexs[i]].key), &(file->keys_meta[file->keys_num - 1].key));
    // 目标 key 在 file 中: 1. 二分查找, 2. 从 nvm 中读取 value
    if( com_result == 0) {
      if(BinarySearchInFile(file, first_key_indexs[i], &user_key, &find_index, &pre_left, &pre_right)) {
        GetValueInFile(file, find_index, value);
        *s = Status::OK();
        return true;
      }
      if(pre_left >= (int)first_key_indexs[i] && pre_left < (int)file->keys_num) {
        pre_left = (int)file->keys_meta[pre_left].next - 1;  // 在 BinarySearchInFile 中会有检测范围，无需担心 -1 带来越界
      }
      if(pre_right >= (int)first_key_indexs[i] && pre_right < (int)file->keys_num) {
        pre_right = (int)file->keys_meta[pre_right].next;
      }
      next_file_num = file->key_point_filenum;
    }
    else if(com_result < 0) {  //小于该范围
      pre_left = -1;
      pre_right = (int)file->keys_meta[first_key_indexs[i]].next;  
      next_file_num = file->key_point_filenum;
    }
    else {  //大于该范围
      pre_left = (int)file->keys_meta[file->keys_num - 1].next - 1;  // 在 BinarySearchInFile 中会有检测范围，无需担心 -1 带来越界
      pre_right = -1;  
      next_file_num = file->key_point_filenum;
    }
  }
  return false;

}

// 检查 user_key 是否位于 [start, end] 区间内
bool NvmCfModule::UserKeyInRange(Slice *user_key, InternalKey *start, InternalKey *end) {
  auto user_comparator = icmp_->user_comparator();
  if(user_comparator->Compare(*user_key, start->user_key()) < 0 || user_comparator->Compare(*user_key, end->user_key()) > 0 ) {
    return false;
  }
  return true;
}

// user_key 与 [start, end] 范围比较
int NvmCfModule::UserKeyCompareRange(Slice *user_key, InternalKey *start, InternalKey *end) {   // user_key < start,return -1; user_key > end, return 1; start < user_key < end, return 0;
  auto user_comparator = icmp_->user_comparator();
  if(user_comparator->Compare(*user_key, start->user_key()) < 0) {
    return -1;
  }
  else if(user_comparator->Compare(*user_key, end->user_key()) > 0) {
    return 1;
  }
  else {
    return 0;
  }
}

// 从 nvm 中 二分查找 目标 key
bool NvmCfModule::BinarySearchInFile(FileEntry* file, int first_key_index, Slice *user_key, int *find_index, int *pre_left , int *pre_right) {
  auto user_comparator = icmp_->user_comparator();
  int left = first_key_index;
  if(pre_left != nullptr && *pre_left >= first_key_index && *pre_left < (int)file->keys_num) {
    left = *pre_left;
  }
  int right = file->keys_num - 1;
  if(pre_right != nullptr && *pre_right >= first_key_index && *pre_right < (int)file->keys_num){
    right = *pre_right;
  }

  int mid = 0;
  while(left <= right) {  //有等号可确定跳出循环时 right < left
    mid = (left + right)/2;
    //RECORD_LOG("left:%d right:%d mid:%d\n",left,right,mid);
    int compare_result = user_comparator->Compare(file->keys_meta[mid].key.user_key(), *user_key);  //优化后字符串只比较一次
    if(compare_result > 0) {
      right = mid - 1;
    }
    else if(compare_result < 0) {
      left = mid + 1;
    }
    else {   //找到的情况次数最少，放在最后，减少比较次数
      *find_index = mid;
      return true;
    }
  }
  *pre_right = left;
  *pre_left = right;
  return false;
}

// 从 nvm 中, 计算出地址, 取出 value 值
bool NvmCfModule::GetValueInFile(FileEntry* file, int find_index, std::string *value) {
  char* data_addr = GetIndexPtr(file->sstable_index);
  uint64_t key_value_offset = file->keys_meta[find_index].offset;
  uint64_t key_size = DecodeFixed64(data_addr + key_value_offset);
  key_value_offset += 8;
  key_value_offset += key_size;
  uint64_t value_size = DecodeFixed64(data_addr + key_value_offset);
  key_value_offset += 8;
  value->assign(data_addr + key_value_offset, value_size);
  return true;
}

// 1. 根据 L0files 找到对应 FileEntry
// 2. 根据 FileEntry, 计算剩余 key (key_num), 生成 MergeIteratorBuilder
void NvmCfModule::AddIterators(VersionStorageInfo* vstorage, MergeIteratorBuilder* merge_iter_builder) {
  auto L0files = vstorage->LevelFiles(0);
  std::vector<FileEntry*> findfiles;
  std::vector<uint64_t> first_key_indexs;
  
  FileEntry* tmp = nullptr;
  for(unsigned int i = 0; i < L0files.size(); i++) {
    tmp = sst_meta_->FindFile(L0files.at(i)->fd.GetNumber());
    findfiles.push_back(tmp);
    first_key_indexs.push_back(L0files.at(i)->first_key_index);
  }
  FileEntry* file = nullptr;
  uint64_t key_num = 0;
  for(unsigned int i = 0; i < findfiles.size(); i++) {
    file = findfiles.at(i);
    key_num = file->keys_num - first_key_indexs[i];
    merge_iter_builder->AddIterator(NewNVMLevel0ReadIterator(icmp_, GetIndexPtr(file->sstable_index), file, first_key_indexs[i], key_num));
  }
}

// 从 VersionStorageInfo 中恢复 sst_meta_ 和 ptr_sst_
void NvmCfModule::RecoverFromStorageInfo(VersionStorageInfo* vstorage) {
  auto L0files = vstorage->LevelFiles(0);
  if( L0files.size() == 0) return;
  if( open_by_creat_ ) {
    RECORD_LOG("[Error] open nvm cf file by creat, can't recover!\n");
    return;
  }

  for(int i = L0files.size() - 1; i >= 0; i--) { //保证顺序
    FileEntry* tmp = sst_meta_->FindFile(L0files[i]->fd.GetNumber(), true, false);
    if( tmp != nullptr) {  //find, how to do ?
      RECORD_LOG("[Warn] recover file:%lu , but it exist!\n", L0files[i]->fd.GetNumber());
      continue;
    }
    //add to nvm cf
    ptr_sst_->RecoverAddSstable(L0files[i]->nvm_sstable_index);
    FileEntry* file = sst_meta_->AddFile(L0files[i]->fd.GetNumber(), L0files[i]->nvm_sstable_index);
    file->keys_num = L0files[i]->keys_num;
    file->key_point_filenum = L0files[i]->key_point_filenum;

    //恢复keys元数据
    char * file_ptr = ptr_sst_->GetIndexPtr(file->sstable_index);
    uint64_t offset = L0files[i]->raw_file_size;
    uint64_t keys_meta_size = L0files[i]->nvm_meta_size;

    char buf[keys_meta_size];
    memcpy(buf, file_ptr + offset, keys_meta_size);   //读取keys元数据

    file->keys_meta = new KeysMetadata[file->keys_num];
    offset = 0;
    uint64_t key_size = 0;

    for(unsigned int j = 0; j < file->keys_num; j++) {
      Slice ptr_key_size(buf + offset, 8);
      GetFixed64(&ptr_key_size, &key_size);
      offset += 8;

      Slice key(buf + offset, key_size);
      offset += key_size;
      file->keys_meta[j].key.DecodeFrom(key);

      Slice ptr_next(buf + offset, 4);
      GetFixed32(&ptr_next, (uint32_t*)&file->keys_meta[j].next);
      offset += 4;

      Slice ptr_offset(buf + offset, 8);
      GetFixed64(&ptr_offset, &file->keys_meta[j].offset);
      offset += 8;

      Slice ptr_size(buf + offset, 8);
      GetFixed64(&ptr_size, &file->keys_meta[j].size);
      offset += 8;
    }
    RECORD_LOG("[INFO] recover file:%lu key_point_filenum:%lu keys_num:%lu sstable_index:%lu\n",file->filenum, file->key_point_filenum, file->keys_num, file->sstable_index);
  }
}

NvmCfModule* NewNvmCfModule(NvmCfOptions* nvmcfoption,const std::string &cf_name,uint32_t cf_id,const InternalKeyComparator* icmp){
  return new NvmCfModule(nvmcfoption,cf_name,cf_id,icmp);
}


}  // namespace rocksdb
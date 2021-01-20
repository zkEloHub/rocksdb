#include "sstable_meta.h"

#include "db/dbformat.h"

namespace rocksdb {


SstableMetadata::SstableMetadata(const InternalKeyComparator* icmp, const NvmCfOptions* nvmcfoption)
    :icmp_(icmp), nvmcfoption_(nvmcfoption) {
    mu_ = new MyMutex();
    RECORD_LOG("creat SstableMetadata\n");
}

SstableMetadata::~SstableMetadata() {
    delete mu_;
    std::vector<FileEntry*>::iterator it = files_.begin();
    while(it != files_.end()){
        delete (*it);
        it = files_.erase(it);
    }
}

// 添加记录到 files_
FileEntry* SstableMetadata::AddFile(uint64_t filenumber, int index) {
    FileEntry* tmp = new FileEntry(filenumber, index);
    mu_->Lock();
    files_.insert(files_.begin(),tmp);
    mu_->Unlock();
    return tmp;
}

// files_ 中移除 filenumber 对应的 FE
bool SstableMetadata::DeteleFile(uint64_t filenumber) {
    mu_->Lock();
    std::vector<FileEntry*>::iterator it = files_.begin();
    for(; it != files_.end(); it++) {
        if((*it)->filenum == filenumber) {
            delete (*it);
            files_.erase(it);
            mu_->Unlock();
            return true;
        }
    }
    mu_->Unlock();
    RECORD_LOG("[Warn]: delete no file:%ld\n",filenumber);
    return true;
}

// 返回 filenumber 对应的 files_[x]
FileEntry* SstableMetadata::FindFile(uint64_t filenumber, bool forward, bool have_error_print) {
    mu_->Lock();
    // forward: 遍历顺序
    if(forward) {
        for(unsigned int i = 0;i < files_.size();i++) {
            if(files_[i]->filenum == filenumber ) {
                mu_->Unlock();
                return files_[i];
            }
        }
    }
    else {
        for(int i = files_.size() - 1;i >= 0;i--){
            if(files_[i]->filenum == filenumber ) {
                mu_->Unlock();
                return files_[i];
            }
        }
    }
    mu_->Unlock();
    if(have_error_print) {
        RECORD_LOG("[Error]: no find FileEntry:%lu \n", filenumber);
    }
    return nullptr;
}

// 返回 L0files 中存在于 files_ 中的 FileEntry
void SstableMetadata::GetL0Files(std::vector<FileMetaData*>& L0files, std::vector<FileEntry*> &findfiles) { //有先后顺序的L0
    bool find_file = false;
    unsigned int j = 0;
    mu_->Lock();
    for(unsigned int i = 0; i < L0files.size(); i++) {
        find_file = false;
        // TODO: j = 0 ?
        while(j < files_.size()){
            if(L0files[i]->fd.GetNumber() == files_[j]->filenum) {
                find_file = true;
                break;
            }
            j++;
        }
        if(find_file) {
            findfiles.push_back(files_[j]);
            j++;
        }
        else {
            RECORD_LOG("[Error]: no find L0 file:%lu\n", L0files[i]->fd.GetNumber());
            mu_->Unlock();
            return;
        }
    }
    mu_->Unlock();
}

// [更新前向指针]: 每个 key_mata.next 指向下一个 Table 中 `>= 它本身的` key_meta
void SstableMetadata::UpdateKeyNext(FileEntry* file) {
    if(file == nullptr || file->keys_num == 0) {
        return ;
    }
    mu_->Lock();
    // 在 files_ 中找到 file 的下一个
    std::vector<FileEntry*>::iterator it = files_.begin();
    for(; it != files_.end(); it++) {
        if((*it)->filenum == file->filenum) {
            it++;
            break;
        }
    }
    if(it == files_.end()) {
        mu_->Unlock();
        return;
    }
    mu_->Unlock();
    FileEntry* next_file = (*it);
    struct KeysMetadata* new_keys = file->keys_meta;
    struct KeysMetadata* old_keys = next_file->keys_meta;
    uint64_t new_key_num = file->keys_num;
    uint64_t old_key_num = next_file->keys_num;
    int32_t new_index = 0;
    int32_t old_index = 0;
    while((uint64_t)new_index < new_key_num && (uint64_t)old_index < old_key_num) {
        // old_key >= new_key
        if(icmp_->Compare(old_keys[old_index].key, new_keys[new_index].key) >= 0) {   //相同user key，新插入的会比旧的小
            new_keys[new_index].next = old_index;
            new_index++;
        }
        else {
            old_index++;
        }
    }
    file->key_point_filenum = next_file->filenum;       // key_point_filenum: 指向下一个文件的 file num
}

// 1. 检验 L0files 与 compaction_files 的一致性
// 2. 一致: 返回
// 3. 不一致:
//   (1) 清空 compaction_files
//   (2) 将 L0files(number信息) 添加到 compaction_files
void SstableMetadata::UpdateCompactionState(std::vector<FileMetaData*>& L0files) {
    if (!compaction_files.empty()) {   //不为空,则检测是否一致
        int comfile = compaction_files.size() - 1;
        int L0file = L0files.size() - 1;
        bool consistency = true;
        for(;comfile >= 0 && L0file >= 0;) {
            if(compaction_files[comfile] != L0files[L0file]->fd.GetNumber()) {  // 不一致
                consistency = false;
                break;
            }
            L0file--;
            comfile--;
        }
        if (consistency) {  //一致则继续
            return;
        }
        else {  //不一致，清空compaction_files，重选
            RECORD_LOG("UpdateCompactionState warn:L0:[");
            for(unsigned int i = 0; i < L0files.size(); i++){
                RECORD_LOG("%ld ",L0files[i]->fd.GetNumber());
            }
            RECORD_LOG("] compaction_files:[");
            for(unsigned int i = 0; i < compaction_files.size(); i++){
                RECORD_LOG("%ld ",compaction_files[i]);
            }
            RECORD_LOG("]\n");

            compaction_files.clear();
        }
    }
    // TODO: Level0_column_compaction_slowdown_size 未使用

    // uint64_t l0_files_size = 0;
    // for(unsigned int i = 0; i < L0files.size(); i++){
    //     l0_files_size += L0files[i]->fd.GetFileSize();
    // }
    // if (l0_files_size < nvmcfoption_->Level0_column_compaction_trigger_size) {
    //     RECORD_LOG("warn:L0 l0_files_size:%f MB < Level0_column_compaction_trigger_size:%f MB\n",l0_files_size/1048576.0, nvmcfoption_->Level0_column_compaction_trigger_size/1048576.0);
    // }
    //int level0_stop_writes_trigger = level0_stop_writes_trigger_;

    int file_num = L0files.size() - 1;
    uint64_t compaction_files_size = 0;
    for(;file_num >= 0; file_num--) {  //目前所有table加入compaction_files，后面可设置数量
        compaction_files.insert(compaction_files.begin(),L0files[file_num]->fd.GetNumber());
        compaction_files_size += L0files[file_num]->fd.GetFileSize();
        //可将所有L0加入column compaction
        /* if (compaction_files_size >= Level0_column_compaction_slowdown_size ) { //最大加入的column compaction的数据，可调整，或者全加入
            break;
        } */
    }
    // 未达到 Level0_column_compaction_trigger_size 触发 (这里可能是达到table文件个数限制)
    if (compaction_files_size < nvmcfoption_->Level0_column_compaction_trigger_size) {
        RECORD_LOG("[Warn] L0 l0_files_size:%f MB < Level0_column_compaction_trigger_size:%f MB\n",compaction_files_size/1048576.0, nvmcfoption_->Level0_column_compaction_trigger_size/1048576.0);
    }
    
    RECORD_LOG("UpdateCompactionState:select:%.2f MB num:%ld[",compaction_files_size/1048576.0,compaction_files.size());
    for(unsigned int i=0; i < compaction_files.size(); i++){
        RECORD_LOG("%ld ",compaction_files[i]);
    }
    RECORD_LOG("]\n");
}

// 将 filenumber 从 compaction_files 中移除
void SstableMetadata::DeleteCompactionFile(uint64_t filenumber) {
    std::vector<uint64_t>::iterator it = compaction_files.begin();
    for(; it != compaction_files.end(); it++) {
        if((*it) == filenumber) {
            compaction_files.erase(it);
            return;
        }
    }
    RECORD_LOG("[Warn] no delete compaction file:%ld\n",filenumber);
}

uint64_t SstableMetadata::GetFilesNumber(){
    mu_->Lock();
    uint64_t num = files_.size();
    mu_->Unlock();
    return num;
}

}
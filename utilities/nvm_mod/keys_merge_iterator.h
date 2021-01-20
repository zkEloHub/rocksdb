
#pragma once

#include "common.h"

namespace rocksdb {

// 多个 FileEntry 的 KV 迭代器的封装 (Merge)
class KeysMergeIterator{
public:
    KeysMergeIterator(std::vector<FileEntry*> *files,std::vector<uint64_t> *first_key_indexs, const Comparator* user_comparator)
    :files_(files), first_key_indexs_(first_key_indexs), user_comparator_(user_comparator) {
        files_num = files_->size();
        child_current_ = new int[files_num];
        current_ = -1;
        for(int i = 0;i < files_num; i++) {
            child_current_[i] = -1;
        }
    }
    
    ~KeysMergeIterator(){
        delete []child_current_;
    }

    // index 设置为各文件待处理的第一个 key 下标 (first key index)
    void SeekToFirst(){
        for(int i = 0;i < files_num; i++){
            child_current_[i] = first_key_indexs_->at(i);
        }
        FindSmallest();
    }

    // index 设置为各文件待处理的最后一个 key 下标
    void SeekToLast(){
        for(int i = 0; i < files_num; i++){
            child_current_[i] = files_->at(i)->keys_num - 1;
        }
        FindLargest();
    }

    bool Valid(){
        return (current_ != -1);
    }

    void Next(){
        assert(Valid());
        child_current_[current_]++;
        if((uint64_t)child_current_[current_] >= files_->at(current_)->keys_num){
            child_current_[current_] = -1;
        }
        FindSmallest();
    }

    // 当前正在处理的 file 及 key 信息
    void GetCurret(int &files_index, int &key_index){
        files_index = current_;
        key_index = child_current_[current_];
    }


private:
    // 各文件待处理的 最小 key 
    void FindSmallest(){
        int smallest = -1;
        for (int i = 0; i < files_num; i++) {
            if(child_current_[i] != -1){
                if(smallest == -1){
                    smallest = i;
                }
                else if(user_comparator_->Compare(ExtractUserKey(files_->at(i)->keys_meta[child_current_[i]].key.Encode()),ExtractUserKey(files_->at(smallest)->keys_meta[child_current_[smallest]].key.Encode())) < 0 ){
                    smallest = i;
                }
            }
        }
        current_ = smallest;
    }

    // 各文件待处理的 最大 key 
    void FindLargest(){
        int largest = -1;
        for (int i = files_num - 1; i >= 0; i--) {
            if(child_current_[i] != -1){
                if(largest == -1){
                    largest = i;
                }
                else if(user_comparator_->Compare(ExtractUserKey(files_->at(i)->keys_meta[child_current_[i]].key.Encode()), ExtractUserKey(files_->at(largest)->keys_meta[child_current_[largest]].key.Encode())) > 0 ){
                    largest = i;
                }
            }
        }
        current_ = largest;
    }

    std::vector<FileEntry*> *files_;
    std::vector<uint64_t> *first_key_indexs_;
    const Comparator* user_comparator_;
    int files_num;
    int *child_current_;        // 管理多个文件，当前处理的 key index
    int current_;               // 当前正在处理的 FileEntry 下标
};

}
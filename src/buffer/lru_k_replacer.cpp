//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
    access_record.resize(num_frames);
    replacer_pool.resize(0);
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    if(replacer_pool.size()==0){
        frame_id=nullptr;
        return false;
    }
    size_t max_record_k=0;
    size_t less_k_recent=0;
    size_t interval;
    for(auto i:replacer_pool){
        if(access_record[i].size()<k_){
            max_record_k=(1<<16)-1;
            if(access_record[i].front()>less_k_recent){
                less_k_recent=access_record[i].front();
                *frame_id=i;
            }
        }
        else{
            interval=access_record[i].front()-access_record[i].back();
            if(interval>max_record_k){
                max_record_k=interval;
                *frame_id=i;
            }
        }
    } 
    return true; 
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    if(size_t(frame_id)>=replacer_size_-1){
        throw std::string("access frame out of memory");
    }
    if(access_record[frame_id].size()==k_){
        access_record[frame_id].pop_back();
    }
    access_record[frame_id].push_front(current_timestamp_);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    if(size_t(frame_id)<0||size_t(frame_id)>=replacer_size_){
        throw std::string("access frame out of memory");
    }
    if(set_evictable){
        replacer_pool.push_back(frame_id);
        return;
    }
    else{
        for(auto p=replacer_pool.begin();p!=replacer_pool.end();p++){
            if(*p==size_t(frame_id)){
                replacer_pool.erase(p);
                return;
            }
        }
    exit(-1);
    }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    for(auto p=replacer_pool.begin();p!=replacer_pool.end();p++){
        if(*p==size_t(frame_id)){
            replacer_pool.erase(p);
            access_record[frame_id].clear();
            return;
        }
    }
    throw std::string("remove a non_evictable frame");
}

auto LRUKReplacer::Size() -> size_t { 
    return replacer_pool.size(); 
    }

}  // namespace bustub

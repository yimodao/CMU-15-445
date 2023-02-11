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
#include <climits>
#include <stdexcept>

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
    replacer_pool.resize(num_frames,false);
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    if(Size()==0){
        frame_id=nullptr;
        return false;
    }
    latch_.lock();
    size_t max_record_k=0;
    size_t less_k_recent=ULONG_MAX;
    size_t interval;
    for(size_t i=0;i<replacer_size_;i++){
        if(replacer_pool[i]){
            if(access_record[i].size()<k_){
                max_record_k=ULONG_MAX;
                if(access_record[i].front()<less_k_recent){
                    less_k_recent=access_record[i].front();
                    *frame_id=frame_id_t(i);
                }
            }
            else{
                interval=(access_record[i].front())-(access_record[i].back());
                if(interval>max_record_k){
                    max_record_k=interval;
                    *frame_id=frame_id_t(i);
                }
            }
        }
    }
    access_record.erase(size_t(*frame_id));
    replacer_pool[size_t(*frame_id)]=false;
    latch_.unlock();
    return true; 
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    latch_.lock();
    if(size_t(frame_id)>replacer_size_-1){
        latch_.unlock();
        throw std::runtime_error("access frame out of memory");
    }
    current_timestamp_++;
    if(access_record.count(size_t(frame_id))==0){
        access_record.insert({size_t(frame_id),std::list<size_t>()});
    }
    if((access_record[size_t(frame_id)].size())==k_){
        access_record[size_t(frame_id)].pop_back();
    }
    access_record[size_t(frame_id)].push_front(current_timestamp_);
    latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    latch_.lock();
    if(size_t(frame_id)>=replacer_size_){
        latch_.unlock();
        throw std::runtime_error("access frame out of memory");
    }
    replacer_pool[size_t(frame_id)]=set_evictable;
    latch_.unlock();
    return;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    latch_.lock();
    if(replacer_pool[size_t(frame_id)]){
        replacer_pool[size_t(frame_id)]=false;
        access_record.erase(size_t(frame_id));
        latch_.unlock();
        return;
    }
    latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t {
    latch_.lock();
    size_t res=0;
    for(size_t i=0;i<replacer_size_;i++){
        if(replacer_pool[i]){
            res++;
        }
    }
    latch_.unlock(); 
    return res;
}
auto LRUKReplacer::Data_view()->std::string{
    latch_.lock();
    std::string res;
    for(size_t i=0;i<replacer_size_;i++){
        if(replacer_pool[i]){
            res+=('0'+i);
            res+=",";
        }
    }
    res+= "\n";
    latch_.unlock();
    return res;
}

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
    for(int i=0;i<static_cast<int>(num_pages);i++){
        clock_volume.push_back(new frame_info{false,false});
    }
    clock_hand=0;
    clocksize=0;
    buffer_pool_size=num_pages;
}

ClockReplacer::~ClockReplacer() = default;

auto ClockReplacer::Victim(frame_id_t* frame_id) -> bool {
    clock_replacer_lock.lock();
    int start= clock_hand;
    while(true){
        frame_info* now_p=clock_volume[clock_hand];
        if(now_p->is_member){
            if(now_p->flag){
                now_p->flag=false;
            }
            else{
                clocksize--;
                *frame_id=clock_hand;
                clock_replacer_lock.unlock();
                return true;
            }
        }
        clock_hand++;
        clock_hand%=buffer_pool_size;
        if(clock_hand==start){
            clock_replacer_lock.unlock();
            return false;
        }
    }
    clock_replacer_lock.unlock();
    return true; 
}
void ClockReplacer::Pin(frame_id_t frame_id) {
    clock_replacer_lock.lock();
    frame_info* now_p=clock_volume[frame_id];
    now_p->is_member=false;
    clocksize--;
    clock_replacer_lock.unlock();
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
    clock_replacer_lock.lock();
    frame_info* now_p=clock_volume[frame_id];
    now_p->is_member=true;
    clocksize++;
    clock_replacer_lock.unlock();
}

auto ClockReplacer::Size() -> size_t { 
    return clocksize; 
    }

}  // namespace bustub

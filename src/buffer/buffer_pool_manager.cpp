//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  latch_.lock();
  frame_id_t search_frame;
  if(!free_list_.empty()){
    *page_id=AllocatePage();
    search_frame=free_list_.front();
    free_list_.pop_front();
  }
  else{
    if(!replacer_->Evict(&search_frame)){
      page_id=nullptr;
      latch_.unlock();
      return nullptr;
    }
    *page_id=AllocatePage();
    page_table_->Remove(pages_[search_frame].GetPageId());
    if(pages_[search_frame].is_dirty_){
      pages_[search_frame].pin_count_=0;
      disk_manager_->WritePage(pages_[search_frame].GetPageId(), pages_[search_frame].GetData());
    }
  }
  page_table_->Insert(*page_id, search_frame);
  pages_[search_frame].ResetMemory();
  pages_[search_frame].page_id_=*page_id;
  pages_[search_frame].pin_count_=1;
  pages_[search_frame].is_dirty_=false;
  replacer_->SetEvictable(search_frame,false);
  replacer_->RecordAccess(search_frame);
  latch_.unlock();
  return &pages_[search_frame]; 
}

auto BufferPoolManager::FetchPage(page_id_t page_id) -> Page * {
  latch_.lock();
  frame_id_t search_frame;
  if (page_table_->Find(page_id, search_frame)) {
    replacer_->RecordAccess(search_frame);
    replacer_->SetEvictable(search_frame,false);
    latch_.unlock();
    return &pages_[search_frame];
  }
  if(!free_list_.empty()){
    search_frame = free_list_.front();
    free_list_.pop_front();
  }
  else{
    if(!replacer_->Evict(&search_frame)){
      latch_.unlock();
      return nullptr;
    }
    if(pages_[search_frame].is_dirty_){
      pages_[search_frame].pin_count_=0;
      disk_manager_->WritePage(pages_[search_frame].GetPageId(), pages_[search_frame].GetData());
    }
    replacer_->Remove(search_frame);
    page_table_->Remove(pages_[search_frame].GetPageId());
  }
  disk_manager_->ReadPage(page_id, pages_[search_frame].GetData());
  pages_[search_frame].is_dirty_=false;
  pages_[search_frame].pin_count_=1;
  pages_[search_frame].page_id_=page_id;
  replacer_->SetEvictable(search_frame,false);
  replacer_->RecordAccess(search_frame);
  page_table_->Insert(page_id, search_frame); 
  latch_.unlock();
  return &pages_[search_frame];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) -> bool {
  latch_.lock();
  frame_id_t search_frame;
  if (page_table_->Find(page_id, search_frame)) {
    if (pages_[search_frame].pin_count_ > 0) {
      pages_[search_frame].pin_count_--;
      if (pages_[search_frame].pin_count_ == 0) {
        replacer_->SetEvictable(search_frame,true);
        pages_[search_frame].is_dirty_ = is_dirty;
      }
      latch_.unlock();
      return true;
    }
  }
  latch_.unlock();
  return false;
}
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  latch_.lock();
  frame_id_t search_frame;
  if(page_table_->Find(page_id, search_frame)){
    disk_manager_->WritePage(page_id, pages_[search_frame].GetData());
    pages_[search_frame].is_dirty_ = false;
    latch_.unlock();
    return true;
  }
  latch_.unlock();
  return false;
}

void BufferPoolManager::FlushAllPages() {
  latch_.lock();
  frame_id_t search_frame;
  for (size_t i = 0; i < pool_size_; i++) {
    if(page_table_->Find(pages_[i].GetPageId(),search_frame)){
      disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
      pages_[i].is_dirty_ = false;
    }
  }
  latch_.unlock();
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  latch_.lock();
  frame_id_t search_frame;
  if(page_table_->Find(page_id, search_frame)){
    if (pages_[search_frame].pin_count_ > 0) {
      latch_.unlock();
      return false;
    }
    replacer_->Remove(search_frame);
    page_table_->Remove(page_id);
    pages_[search_frame].ResetMemory();
    free_list_.push_back(search_frame);
    DeallocatePage(page_id);
  }
  latch_.unlock();
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub

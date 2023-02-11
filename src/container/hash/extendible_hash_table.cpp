//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {
  std::scoped_lock<std::mutex> lock(latch_);
  global_dir.push_back(0);
  std::shared_ptr<Bucket> new_bucket=std::make_shared<Bucket>(bucket_size,global_depth_);
  dir_.push_back(new_bucket);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  latch_.lock();
  size_t index=IndexOf(key);
  bool res=dir_[global_dir[index]]->Find(key,value);
  latch_.unlock();
  return res;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  latch_.lock();
  size_t index=IndexOf(key);
  bool res=dir_[global_dir[index]]->Remove(key);
  latch_.unlock();
  return res;
}
template <typename K, typename V>
void ExtendibleHashTable<K, V>::RedistributeBucket
(std::shared_ptr<Bucket> bucket)
{
  int index;
  int mask = 1 << (bucket->GetDepth()-1);
  std::list<std::pair<K, V>>& target_list=bucket->GetItems();
  auto p=target_list.begin();
  for(;p!=target_list.end();){
    index=IndexOf(p->first);
    if(index&mask){
      dir_.back()->Insert(p->first,p->second);
      target_list.erase(p++);
      global_dir[index]=num_buckets_-1;
    }
    else{
      p++;
    }
  }
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  latch_.lock();
  size_t index=IndexOf(key);
  std::shared_ptr<Bucket> find_bucket(dir_[global_dir[index]]);
  if(find_bucket->Insert(key,value)){
    latch_.unlock();
    return;
  }
  if((find_bucket->GetDepth())==global_depth_){
    global_depth_++;
    global_dir.resize(1<<global_depth_,0);
    int add_index=1<<(global_depth_-1);
    for(int i=global_dir.size()/2-1;i>=0;i--){
        global_dir[i+add_index]=global_dir[i];
    }
  }
  find_bucket->IncrementDepth();
  int bucket_size=find_bucket->GetItems().size();
  std::shared_ptr<Bucket>new_bucket=std::make_shared<Bucket>(bucket_size,find_bucket->GetDepth());
  dir_.push_back(new_bucket);
  num_buckets_++;
  RedistributeBucket(find_bucket);
  latch_.unlock();
  Insert(key,value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for(auto &p:list_){
    if(p.first==key){
      value=p.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  int number=0;
  for(auto p=list_.begin();p!=list_.end();){
    if((p->first)==key){
      list_.erase(p++);
      number++;
    }
    else{
      p++;
    }
  }
  if(number){
    return true;
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for(auto &p:list_){
    if(p.first==key){
      p.second=value;
      return true;
    }
  }
  if(list_.size()<size_){
    list_.push_back(std::pair<K,V>{key,value});
    return true;
  }
  return false;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub

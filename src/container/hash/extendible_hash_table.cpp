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
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size) : bucket_size_(bucket_size) {}

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
auto ExtendibleHashTable<K, V>::Find_bucket_index(const K &key)->int{
  std::scoped_lock<std::mutex> lock(latch_);
  size_t hash_num=IndexOf(key);
  int index=hash_num>>(32-GetGlobalDepth());
  return index;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  int index=Find_bucket_index(key);
  auto find_bucket=dir_[global_dir[index]];
  return find_bucket->Find(key,value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  int index=Find_bucket_index(key);
  auto find_bucket=dir_[global_dir[index]];
  return find_bucket->Remove(key);
  
}
template <typename K, typename V>
void ExtendibleHashTable<K, V>::RedistributeBucket
(std::shared_ptr<Bucket> bucket){
  std::shared_ptr<Bucket> new_add_bucket(new Bucket(bucket_size_,bucket->GetDepth()));
  int index;
  auto target_list=bucket->GetItems();
  for(auto p=target_list.begin();p!=target_list.end();p++){
    index=Find_bucket_index(p->first);
    if(index%2){
      new_add_bucket->Insert(p->first,p->second);
      bucket->Remove(p->first);
    }
  }
  dir_.push_back(new_add_bucket);
  global_dir[index/2*2+1]=dir_.size()-1;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  int index=Find_bucket_index(key);
  auto find_bucket=dir_[global_dir[index]];
  if(find_bucket->Insert(key,value)){
    return;
  }
  if(GetLocalDepth(global_dir[index])==GetGlobalDepth()){
    global_depth_++;
    global_dir.resize(2<<global_depth_,-1);
    for(int i=global_dir.size()/2-1;i>=0;i--){
      if(global_dir[i]!=-1){
        global_dir[2*i]=global_dir[i];
        global_dir[2*i+1]=global_dir[i];
        if(i!=0){
          global_dir[i]=-1;
        }
      }
    }
  }
  find_bucket->IncrementDepth();
  RedistributeBucket(find_bucket);
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
  for(auto p=list_.begin();p!=list_.end();p++){
    if(p->first==key){
      list_.erase(p);
      return true;
    }
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

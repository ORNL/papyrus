#include <papyrus/kv.h>
#include "Cache.h"
#include "DB.h"
#include "Debug.h"
#include "Slice.h"

namespace papyruskv {

Cache::Cache(DB* db, size_t capacity, bool local) {
    db_ = db;
    capacity_ = capacity;
    local_ = local;
    size_ = 0UL;
    enable_ = true;
    rank_ = db_->rank();
    hit_ = 0UL;
    miss_ = 0UL;
    pthread_mutex_init(&mutex_, NULL);
}

Cache::~Cache() {
    pthread_mutex_lock(&mutex_);
    for (auto it = lru_.begin(); it != lru_.end(); ++it) delete it->second;
    pthread_mutex_unlock(&mutex_);
    pthread_mutex_destroy(&mutex_);

    if (enable_ && hit_ + miss_ > 0) {
        _trace("[%s] hit[%lu] miss[%lu] hitratio[%lf]", local_ ? "local" : "remote" , hit_, miss_, (double) (hit_) / (hit_ + miss_));
    }
}

void Cache::Put(const char* key, size_t keylen, char* val, size_t vallen, int rank, bool tombstone) {
    if (!enable_) return;

    pthread_mutex_lock(&mutex_);
    Invalidate(key, keylen, false);

    Slice* slice = new Slice(key, keylen, val, vallen, rank, tombstone);
    std::string keystr = std::string(slice->key(), slice->keylen());
    lru_.push_front(pair_t(keystr, slice));
    table_[keystr] = lru_.begin();
    size_ += slice->size();

    if (size_ > capacity_) {
        auto last = lru_.end();
        last--;
        Slice* slice = last->second;
        table_.erase(last->first);
        lru_.pop_back();
        size_ -= slice->size();
        delete slice;
    }
    pthread_mutex_unlock(&mutex_);
}

int Cache::Get(const char* key, size_t keylen, char** valp, size_t* vallenp) {
    if (!enable_) return PAPYRUSKV_SLICE_NOT_FOUND;

    pthread_mutex_lock(&mutex_);
    std::string keystr = std::string(key, keylen);
    auto it = table_.find(keystr);
    if (it == table_.end()) {
        miss_++;
        pthread_mutex_unlock(&mutex_);
        return PAPYRUSKV_SLICE_NOT_FOUND;
    }
    hit_++;
    lru_.splice(lru_.begin(), lru_, it->second);
    Slice* slice = it->second->second;
    if (slice->tombstone()) {
        pthread_mutex_unlock(&mutex_);
        return PAPYRUSKV_SLICE_TOMBSTONE;
    }
    slice->CopyValue(valp, vallenp);
    pthread_mutex_unlock(&mutex_);
    return PAPYRUSKV_SLICE_FOUND;
}

bool Cache::Invalidate(const char* key, size_t keylen, bool lock) {
    if (!enable_) return false;

    if (lock) pthread_mutex_lock(&mutex_);
    auto it = table_.find(std::string(key, keylen));
    if (it == table_.end()) {
        if (lock) pthread_mutex_unlock(&mutex_);
        return false;
    }

    Slice* slice = it->second->second;
    lru_.erase(it->second);
    table_.erase(it);
    size_ -= slice->size();
    delete slice;
    if (lock) pthread_mutex_unlock(&mutex_);

    return true;
}

void Cache::InvalidateAll() {
    if (!enable_) return;

    pthread_mutex_lock(&mutex_);
    auto it = table_.begin();
    for (auto it = table_.begin(); it != table_.end(); ++it) {
        Slice* slice = it->second->second;
        delete slice;
    }
    table_.clear();
    pthread_mutex_unlock(&mutex_);
}

} /* namespace papyruskv */

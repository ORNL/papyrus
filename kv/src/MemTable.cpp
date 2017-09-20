#include <papyrus/kv.h>
#include "MemTable.h"
#include "Platform.h"
#include "Slice.h"
#include "Command.h"
#include "Hasher.h"
#include "DB.h"
#include "Debug.h"
#include <string.h>

namespace papyruskv {

MemTable::MemTable(DB* db, bool local_mt) {
    if (local_mt) mid_ = Platform::NewMID();
    ref_ = 1;
    db_ = db;
    hasher_ = db->hasher();
    size_ = 0UL;
    head_ = NULL;
    tail_ = NULL;
    bucket_ = NULL;
    bucket_size_ = 0UL;
    next_ = NULL;
    cmd_ = NULL;
    pthread_mutex_init(&mutex_, NULL);
}

MemTable::~MemTable() {
    for (auto it = table_.begin(); it != table_.end(); ++it) delete it->second;
    pthread_mutex_destroy(&mutex_);
}

size_t MemTable::Put(Slice* slice) {
    _trace("key[%s] keylen[%lu] val[%s] vallen[%lu] tombstone[%d]", slice->key(), slice->keylen(), slice->val(), slice->vallen(), slice->tombstone());
    pthread_mutex_lock(&mutex_);
    std::string keystr = std::string(slice->key(), slice->keylen());
    auto old_pair = table_.find(keystr);
    if (old_pair != table_.end()) {
        Slice* old = old_pair->second;
        _trace("key[%s] keylen[%lu] val[%s] vallen[%lu] tombstone[%d]", slice->key(), slice->keylen(), slice->val(), slice->vallen(), slice->tombstone());
        _trace("key[%s] keylen[%lu] val[%s] vallen[%lu] count[%lu] tombstone[%d]", old->key(), old->keylen(), old->val(), old->vallen(), table_.count(keystr), old->tombstone());
        if (slice->tombstone() && old->tombstone()) {
            delete slice;
        } else if (slice->tombstone() && !old->tombstone()) {
            old->set_tombstone(true);
            delete slice;
        } else {
            size_ -= old->cb();
            size_ += old->Update(slice->val(), slice->vallen());
            delete slice;
        }
    } else {
        _trace("key[%s] size[%lu] tombstone[%d]", slice->key(), slice->cb(), slice->tombstone());
        table_.insert(std::pair<std::string, Slice*>(keystr, slice));
        slice->set_mt(this);
        size_ += slice->cb();
    }
    pthread_mutex_unlock(&mutex_);
    return size_;
}

bool MemTable::Empty() {
    pthread_mutex_lock(&mutex_);
    bool empty = table_.empty();
    pthread_mutex_unlock(&mutex_);
    return empty;
}

int MemTable::Get(const char* key, size_t keylen, char** valp, size_t* vallenp, papyruskv_pos_t* pos) {
    if (bucket_) return GetHash(key, keylen, valp, vallenp, pos);
    return GetTable(key, keylen, valp, vallenp, pos);
}

int MemTable::GetHash(const char* key, size_t keylen, char** valp, size_t* vallenp, papyruskv_pos_t* pos) {
    int idx = BucketIdx(key, keylen);
    Slice* slice = bucket_[idx];
    while (slice) {
        if (slice->Match(key, keylen)) {
            slice->SetPos(pos);
            if (slice->tombstone()) return PAPYRUSKV_SLICE_TOMBSTONE;
            slice->CopyValue(valp, vallenp);
            return PAPYRUSKV_SLICE_FOUND;
        }
        slice = slice->buc_next();
    }
    return PAPYRUSKV_SLICE_NOT_FOUND;
}

int MemTable::GetTable(const char* key, size_t keylen, char** valp, size_t* vallenp, papyruskv_pos_t* pos) {
    std::string keystr = std::string(key, keylen);
    pthread_mutex_lock(&mutex_);
    auto it = table_.find(keystr);
    if (it == table_.end()) {
        pthread_mutex_unlock(&mutex_);
        return PAPYRUSKV_SLICE_NOT_FOUND;
    }
    Slice* slice = it->second;
    slice->SetPos(pos);
    if (slice->tombstone()) {
        pthread_mutex_unlock(&mutex_);
        return PAPYRUSKV_SLICE_TOMBSTONE;
    }
    slice->CopyValue(valp, vallenp);
    pthread_mutex_unlock(&mutex_);
    return PAPYRUSKV_SLICE_FOUND;
}

Slice* MemTable::SortByKey() {
    head_ = NULL;
    tail_ = NULL;
    for (auto it = table_.begin(); it != table_.end(); ++it) {
        Slice* slice = it->second;
        if (head_ == NULL) head_ = slice;
        if (tail_ != NULL) tail_->set_next(slice);
        tail_ = slice;
    }
    return head_;
}

void MemTable::Hash() {
    ClearBucket();
    bucket_size_ = hasher_->BucketSize(table_.size());
    _trace("table[%lu] bucket_size[%lu][%lx]", table_.size(), bucket_size_, bucket_size_);
    bucket_ = new Slice*[bucket_size_];
    for (size_t i = 0; i < bucket_size_; i++) bucket_[i] = NULL;

    for (auto it = table_.begin(); it != table_.end(); ++it) {
        Slice* slice = it->second;
        int idx = BucketIdx(slice->key(), slice->keylen());
        if (bucket_[idx] == NULL) bucket_[idx] = slice;
        else {
            Slice* old = bucket_[idx];
            bucket_[idx] = slice;
            slice->set_buc_next(old);
        }
    }
}

void MemTable::ClearBucket() {
    if (bucket_ == NULL) return;
    delete[] bucket_;
    bucket_ = NULL;
    for (auto it = table_.begin(); it != table_.end(); ++it) {
        Slice* slice = it->second;
        slice->set_buc_next(NULL);
    }
}

int MemTable::BucketIdx(const char* key, size_t keylen) {
    return hasher_->KeyBucket(key, keylen, bucket_size_);
}

void MemTable::Retain() {
    int cur_ref;
    do {
        cur_ref = ref_;
    } while (!__sync_bool_compare_and_swap(&ref_, cur_ref, cur_ref + 1));
}

void MemTable::Release(MemTable* mt) {
    int cur_ref;
    do {
        cur_ref = mt->ref_;
    } while (!__sync_bool_compare_and_swap(&mt->ref_, cur_ref, cur_ref - 1));

    if (mt->ref_ == 0) delete mt;
}

void MemTable::Print() {
    if (table_.empty()) _debug("mt[%p], %s", this, "empty");
    for (auto it = table_.begin(); it != table_.end(); ++it) {
        Slice* slice = it->second;
        _debug("mt[%p] key[%s] keylen[%lu] val[%s] vallen[%lu]", this, slice->key(), slice->keylen(), slice->val(), slice->vallen());
    }
}

MemTable* MemTable::Duplicate(MemTable* mt) {
    MemTable* copy = new MemTable(mt->db_);
    copy->set_mid(mt->mid());
    pthread_mutex_lock(&mt->mutex_);
    for (auto it = mt->table_.begin(); it != mt->table_.end(); ++it) {
        Slice* slice = Slice::Duplicate(it->second);
        copy->Put(slice);
    }
    pthread_mutex_unlock(&mt->mutex_);
    return copy;
}

} /* namespace papyruskv */

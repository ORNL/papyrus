#ifndef PAPYRUS_KV_SRC_SLICE_H
#define PAPYRUS_KV_SRC_SLICE_H

#include <papyrus/kv.h>
#include "Pool.h"
#include <stddef.h>

namespace papyruskv {

class MemTable;

class Slice {
public:
    Slice(size_t keylen, size_t vallen, int rank, bool tombstone = false);
    Slice(const char* key, size_t keylen, const char* val, size_t vallen, int rank, bool tombstone = false);
    Slice(const char* key, size_t keylen, size_t vallen, const char** vals, size_t* vallens, size_t bs, int rank, bool tombstone = false);
    ~Slice();
    void CopyValue(char** valp, size_t* vallenp);
    void CopyIter(papyruskv_iter_t iter);
    size_t Update(const char* val, size_t vallen);
    bool Match(const char* key, size_t keylen);
    void SetPos(papyruskv_pos_t* pos);

    char* buf() const { return buf_; }
    char* key() const { return key_; }
    size_t keylen() const { return keylen_; }
    char* val() const { return vallen_ ? val_ : NULL; }
    size_t vallen() const { return vallen_; }
    size_t kvsize() const { return keylen_ + vallen_; }
    size_t size() const { return size_; }
    size_t cb() const { return keylen_ + sizeof(keylen_) + vallen_ + sizeof(vallen_) + 1; }
    int rank() const { return rank_; }
    bool tombstone() const;
    void set_tombstone(bool tombstone);
    Slice* next() const { return next_; }
    void set_next(Slice* s) { next_ = s; }
    Slice* buc_next() const { return buc_next_; }
    void set_buc_next(Slice* s) { buc_next_ = s; }
    MemTable* mt() const { return mt_; }
    void set_mt(MemTable* mt) { mt_ = mt; }

private:
    char* buf_;
    char* key_;
    size_t keylen_;
    char* val_;
    size_t vallen_;
    size_t size_;
    int rank_;
    Slice* next_;
    Slice* buc_next_;

    MemTable* mt_;
    Pool* pool_;

public:
    static Slice* CreateTombstone(const char* key, size_t keylen, int rank);
    static Slice* Duplicate(Slice* slice);

};

} /* namespace papyruskv */

#endif /* PAPYRUS_KV_SRC_SLICE_H */

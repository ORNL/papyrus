#include "Slice.h"
#include "Platform.h"
#include "Debug.h"
#include <string.h>

namespace papyruskv {

Slice::Slice(size_t keylen, size_t vallen, int rank, bool tombstone) {
    if (posix_memalign((void**) &buf_, 0x10, keylen + vallen + 1) != 0)
        _error("failed to alloc size[%lu]", keylen + vallen + 1);
    key_ = buf_;
    val_ = buf_ + keylen;
    keylen_ = keylen;
    vallen_ = vallen;
    rank_ = rank;
    set_tombstone(tombstone);
    size_ = keylen_ + vallen_ + 1;
    next_ = NULL;
    buc_next_ = NULL;

    pool_ = Platform::GetPlatform()->pool();
}

Slice::Slice(const char* key, size_t keylen, const char* val, size_t vallen, int rank, bool tombstone) : Slice(keylen, vallen, rank, tombstone) {
    memcpy(buf_, key, keylen);
    if (val != NULL && vallen > 0) memcpy(val_, val, vallen);
}

Slice::Slice(const char* key, size_t keylen, size_t vallen, const char** vals, size_t* vallens, size_t bs, int rank, bool tombstone) : Slice(keylen, vallen, rank, tombstone) {
    char* off = val_;
    memcpy(buf_, key, keylen);
    for (size_t i = 0; i < bs; i++) {
        memcpy(off, vals[i], vallens[i]);
        off += vallens[i];
    }
}

Slice::~Slice() {
    free(buf_);
}

void Slice::CopyValue(char** valp, size_t* vallenp) {
    if (valp) {
        if (*valp == NULL) *valp = pool_->AllocVal(vallen_);
        memcpy(*valp, val_, vallen_);
    }
    if (vallenp) *vallenp = vallen_;
}

void Slice::CopyIter(papyruskv_iter_t iter) {
    iter->key = key_;
    iter->keylen = keylen_;
    iter->val = val_;
    iter->vallen = vallen_;
    iter->handle = (void*) this;
}

size_t Slice::Update(const char* val, size_t vallen) {
    if (vallen == vallen_) {
        memcpy(val_, val, vallen);
        return cb();
    }
    bool ts = tombstone();
    char* new_buf;
    if (posix_memalign((void**) &new_buf, 0x10, keylen_ + vallen + 1) != 0)
        _error("failed to alloc size[%lu]", keylen_ + vallen + 1);
    memcpy(new_buf, buf_, keylen_);
    memcpy(new_buf + keylen_, val, vallen);
    free(buf_);
    buf_ = new_buf;
    key_ = buf_;
    val_ = buf_ + keylen_;
    vallen_ = vallen;
    set_tombstone(ts);
    size_ = keylen_ + vallen_ + 1;
    return cb();
}

bool Slice::Match(const char* key, size_t keylen) {
    return (keylen == keylen_) && (memcmp(key_, key, keylen) == 0);
}

void Slice::SetPos(papyruskv_pos_t* pos) {
    if (!pos) return;
    pos->handle = this;
}

bool Slice::tombstone() const {
    return buf_[keylen_ + vallen_] ==  1;
}

void Slice::set_tombstone(bool tombstone) {
    buf_[keylen_ + vallen_] = tombstone ? 1 : 0;
}

Slice* Slice::CreateTombstone(const char* key, size_t keylen, int rank) {
    return new Slice(key, keylen, NULL, 0UL, rank, true);
}

Slice* Slice::Duplicate(Slice* slice) {
    return new Slice(slice->key(), slice->keylen(), slice->val(), slice->vallen(), slice->rank(), slice->tombstone());
}

} /* namespace papyruskv */

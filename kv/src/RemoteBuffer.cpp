#include <papyrus/kv.h>
#include "RemoteBuffer.h"
#include "DB.h"
#include "Debug.h"
#include <string.h>

namespace papyruskv {

RemoteBuffer::RemoteBuffer(DB* db, size_t unit, int nranks) {
    db_ = db;
    if (posix_memalign((void**) &buf_, 0x1000, unit * nranks) != 0) 
        _error("cannot alloc buf[%lu]", unit * nranks);
    unit_ = unit;
    off_ = new size_t[nranks];
    for (size_t i = 0; i < nranks; i++) off_[i] = 0UL;
}

RemoteBuffer::~RemoteBuffer() {
    if (buf_) free(buf_);
    delete[] off_;
}

bool RemoteBuffer::Available(size_t keylen, size_t vallen, int rank) {
    return off_[rank] + keylen + vallen + 1 <= unit_;
}

int RemoteBuffer::Put(const char* key, size_t keylen, const char* val, size_t vallen, bool tombstone, int rank) {
    size_t off = rank * unit_ + off_[rank];
    memcpy(buf_ + off, &keylen, sizeof(size_t));
    off += sizeof(size_t);
    memcpy(buf_ + off, &vallen, sizeof(size_t));
    off += sizeof(size_t);
    memcpy(buf_ + off, key, keylen);
    off += keylen;
    if (vallen > 0) {
        memcpy(buf_ + off, val, vallen);
        off += vallen;
    }
    buf_[off] = tombstone ? (char) 1 : (char) 0;
    off += 1;
    off_[rank] += 2 * sizeof(size_t) + keylen + vallen + 1;
    _trace("key[%s] keylen[%lu] val[%s] vallen[%lu] rank[%d] off[%lu]", key, keylen, val, vallen, rank, off_[rank]);
    return PAPYRUSKV_OK;
}

char* RemoteBuffer::Data(int rank) {
    return buf_ + unit_ * rank;
}

char* RemoteBuffer::Copy(int rank) {
    char* copy;
    if (posix_memalign((void**) &copy, 0x10, Size(rank)) != 0)
        _error("cannot alloc copy[%lu]", Size(rank));
    memcpy(copy, Data(rank), Size(rank));
    return copy;
}

size_t RemoteBuffer::Size(int rank) {
    return off_[rank];
}

void RemoteBuffer::Reset(int rank) {
    off_[rank] = 0UL;
}

} /* namespace papyruskv */

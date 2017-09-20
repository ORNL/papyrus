#ifndef PAPYRUS_KV_SRC_REMOTEBUFFER_H
#define PAPYRUS_KV_SRC_REMOTEBUFFER_H

namespace papyruskv {

class DB;

class RemoteBuffer {
public:
    RemoteBuffer(DB* db, size_t unit, int nranks);
    ~RemoteBuffer();

    bool Available(size_t keylen, size_t vallen, int rank);
    int Put(const char* key, size_t keylen, const char* val, size_t vallen, bool tombstone, int rank);
    char* Data(int rank);
    char* Copy(int rank);
    size_t Size(int rank);
    void Reset(int rank);

    DB* db() const { return db_; }

private:
    DB* db_;
    char* buf_;
    size_t* off_;
    size_t unit_;
};

} /* namespace papyruskv */

#endif /* PAPYRUS_KV_SRC_REMOTEBUFFER_H */


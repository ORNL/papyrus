#ifndef PAPYRUS_KV_SRC_CACHE_H
#define PAPYRUS_KV_SRC_CACHE_H

#include <pthread.h>
#include <unordered_map>
#include <list>
#include <string>
#include "Slice.h"

namespace papyruskv {

class DB;

class Cache {
public:

    typedef std::pair<std::string, Slice*> pair_t;
    typedef std::list<pair_t>::iterator iter_t;

    Cache(DB* db, size_t capacity, bool local);
    ~Cache();

    void Put(const char* key, size_t keylen, char* val, size_t vallen, int rank, bool tombstone);
    int Get(const char* key, size_t keylen, char** valp, size_t* vallenp);
    bool Invalidate(const char* key, size_t keylen, bool lock = true);
    void InvalidateAll();

    void Enable(bool enable) { enable_ = enable; }

private:
    DB* db_;
    size_t capacity_;
    size_t size_;
    bool local_;
    bool enable_;
    int rank_;

    size_t hit_;
    size_t miss_;

    std::list<pair_t> lru_;
    std::unordered_map<std::string, iter_t> table_;

    pthread_mutex_t mutex_;
};

} /* namespace papyruskv */

#endif /* PAPYRUS_KV_SRC_CACHE_H */

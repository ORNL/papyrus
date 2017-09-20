#ifndef PAPYRUS_KV_SRC_MEMTABLE_H
#define PAPYRUS_KV_SRC_MEMTABLE_H

#include <stddef.h>
#include <pthread.h>
#include <semaphore.h>
#include <map>
#include <string>
#include "Hasher.h"

namespace papyruskv {

class DB;
class Command;
class Slice;

class MemTable {
public:
    MemTable(DB* db, bool local_mt = false);
    ~MemTable();

    size_t Put(Slice* slice);
    int Get(const char* key, size_t keylen, char** valp, size_t* vallenp, papyruskv_pos_t* pos = NULL);
    int GetHash(const char* key, size_t keylen, char** valp, size_t* vallenp, papyruskv_pos_t* pos);
    int GetTable(const char* key, size_t keylen, char** valp, size_t* vallenp, papyruskv_pos_t* pos);

    Slice* SortByKey();
    void Hash();
    void ClearBucket();

    void Print();
    bool Empty();

    void Retain();

    unsigned long mid() const { return mid_; }
    void set_mid(unsigned long mid) { mid_ = mid; }
    size_t size() const { return size_; }
    size_t count() const { return table_.size(); }
    Slice* head() const { return head_; }
    DB* db() const { return db_; }
    MemTable* next() const { return next_; }
    void set_next(MemTable* mt) { next_ = mt; }
    Command* cmd() const { return cmd_; }
    void set_cmd(Command* cmd) { cmd_ = cmd; }

private:
    int BucketIdx(const char* key, size_t keylen);

private:
    unsigned long mid_;
    int ref_;

    std::map<std::string, Slice*> table_;

    size_t size_;
    Slice* head_;
    Slice* tail_;

    Slice** bucket_;
    size_t bucket_size_;

    Hasher* hasher_;
    DB* db_;

    MemTable* next_;

    Command* cmd_;

    pthread_mutex_t mutex_;

public:
    static MemTable* Duplicate(MemTable* mt);
    static void Release(MemTable* mt);
};

} /* namespace papyruskv */

#endif /* PAPYRUS_KV_SRC_MEMTABLE_H */

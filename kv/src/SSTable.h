#ifndef PAPYRUS_KV_SRC_SSTABLE_H
#define PAPYRUS_KV_SRC_SSTABLE_H

#include "Bloom.h"
#include "MemTable.h"
#include "Pool.h"
#include <stdint.h>
#include <pthread.h>

namespace papyruskv {

class DB;

typedef struct {
    uint64_t idx;
    uint64_t len;
    uint8_t tombstone;
} slice_idx_t;

class SSTable {
public:
    SSTable(DB* db, int mode);
    ~SSTable();

    uint64_t sid() const { return sid_; }
    uint64_t Flush(MemTable* mt);

    int Get(const char* key, size_t keylen, char** valp, size_t* vallenp);
    int Get(const char* key, size_t keylen, char** valp, size_t* vallenp, int rank, int sid);
    uint64_t SendFiles(uint64_t sid, const char* dst);
    uint64_t RecvFiles(uint64_t sid, const char* src);
    uint64_t DistributeFiles(uint64_t* sids, int size, const char* root);
    int WriteTOC(uint64_t* sids, int size, const char* root);
    int ReadTOC(uint64_t** sids, int* size, const char* root);
    int Load(MemTable* mt, uint64_t sid);

private:
    int GetSequential(const char* key, size_t keylen, char** valp, size_t* vallenp, int rank, slice_idx_t* idxes, size_t idx_cnt, int fd_sst, off_t fd_sst_size);
    int GetBinary(const char* key, size_t keylen, char** valp, size_t* vallenp, int rank, slice_idx_t* idxes, size_t idx_cnt, int fd_sst, off_t fd_sst_size);

    bool SendFile(uint64_t sid, const char* suffix, const char* dst);
    bool RecvFile(uint64_t sid, const char* suffix, const char* src);

    void GetPath(int level, int rank, uint64_t sid, char* root, const char* suffix, char* path);
    void GetPathNoRank(int level, int rank, uint64_t sid, char* root, const char* suffix, char* path);
    void GetSSTPath(int level, int rank, uint64_t sid, char* root, char* path);
    void GetIDXPath(int level, int rank, uint64_t sid, char* root, char* path);
    void GetBLMPath(int level, int rank, uint64_t sid, char* root, char* path);
    void GetTOCPath(const char* root, char* path);

private:
    DB* db_;
    int rank_;
    int nranks_;
    int group_;
    char root_[256];
    uint64_t sid_;
    int mode_;

    Pool* pool_;
    Bloom* bloom_;

    bool enable_bloom_;

    pthread_mutex_t mutex_;
};

} /* namespace papyruskv */

#endif /* PAPYRUS_KV_SRC_SSTABLE_H */

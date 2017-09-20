#ifndef PAPYRUS_KV_SRC_DB_H
#define PAPYRUS_KV_SRC_DB_H

#include <stddef.h>
#include <pthread.h>
#include "Define.h"
#include "Dispatcher.h"
#include "Compactor.h"
#include "Hasher.h"
#include "MemTable.h"
#include "RemoteBuffer.h"
#include "Cache.h"
#include "SSTable.h"
#include <unordered_map>
#include <list>

namespace papyruskv {

class Platform;

class DB {
public:
    DB(unsigned long dbid, const char* name, int flags, papyruskv_option_t* opt, Platform* platform);
    ~DB();

    int Put(const char* key, size_t keylen, const char* val, size_t vallen);
    int PutLocal(Slice* slice);
    int PutLocal(const char* key, size_t keylen, const char* val, size_t vallen, bool tombstone);
    int PutRemote(const char* key, size_t keylen, const char* val, size_t vallen, bool tombstone, int rank);

    int Get(const char* key, size_t keylen, char** valp, size_t* vallenp, papyruskv_pos_t* pos);
    int GetLocal(const char* key, size_t keylen, char** valp, size_t* vallenp, int mode, papyruskv_pos_t* pos);
    int GetRemote(const char* key, size_t keylen, char** valp, size_t* vallenp, int rank, papyruskv_pos_t* pos);
    int GetSST(const char* key, size_t keylen, char** valp, size_t* vallenp, int rank, uint64_t sid = 0);

    int Delete(const char* key, size_t keylen);

    int Fence(int level);
    int Barrier(int level);

    int Flush(bool sync, bool lock = true);

    int SetConsistency(int consistency);
    int SetProtection(int protection);

    int Checkpoint(const char* path, int* event);
    int Restart(const char* path, int* event);
    int Wait(int event);
    int WaitAll();

    int Hash();
    int IterLocal(papyruskv_iter_t* iter);
    int IterNext(papyruskv_iter_t* iter);

    int RegisterUpdate(int fnid, papyruskv_update_fn_t ufn);

    int Update(const char* key, size_t keylen, papyruskv_pos_t* pos, int fnid, void* userin, size_t userinlen, void* userout, size_t useroutlen);
    int UpdateLocal(const char* key, size_t keylen, papyruskv_pos_t* pos, int fnid, void* userin, size_t userinlen, void* userout, size_t useroutlen);
    int UpdateRemote(const char* key, size_t keylen, papyruskv_pos_t* pos, int fnid, void* userin, size_t userinlen, void* userout, size_t useroutlen, int rank);

    void RemoveLocalIMT(MemTable* mt);
    void RemoveRemoteIMT(MemTable* mt);

    unsigned long dbid() const { return dbid_; }
    const char* name() const { return name_; }
    int rank() const { return rank_; }
    int nranks() const { return nranks_; }
    int group() const { return group_; }
    Platform* platform() const { return platform_; }
    Hasher* hasher() const { return hasher_; }
    SSTable* sstable() const { return sstable_; }
    MPI_Comm mpi_comm() const { return mpi_comm_; }
    MPI_Comm mpi_comm_ext() const { return mpi_comm_ext_; }

    size_t keylen() const { return keylen_; }
    size_t vallen() const { return vallen_; }
    bool enable_remote_buffer() const { return enable_remote_buffer_; }

private:
    int Migrate(int rank, bool sync, int level);
    int Migrate(bool sync, int level);

private:
    unsigned long dbid_;
    int consistency_;
    int protection_;
    char name_[PAPYRUSKV_MAX_DB_NAME];
    int rank_;
    int nranks_;
    int group_;
    size_t memtable_size_;
    size_t remote_buf_size_;
    size_t cache_size_;
    bool enable_remote_buffer_;
    MPI_Comm mpi_comm_;
    MPI_Comm mpi_comm_ext_;

    size_t keylen_;
    size_t vallen_;

    Platform* platform_;
    Dispatcher* dispatcher_;
    Compactor* compactor_;
    Hasher* hasher_;
    MemTable* local_mt_;
    MemTable* remote_mt_;
    RemoteBuffer* remote_buf_;
    Cache* local_cache_;
    Cache* remote_cache_;
    SSTable* sstable_;

    char* big_buffer_;

    std::list<MemTable*> local_imts_;
    std::list<MemTable*> remote_imts_;

    std::unordered_map<int, Command*> events_;

    pthread_mutex_t mutex_local_mt_;
    pthread_mutex_t mutex_local_imts_;
    pthread_mutex_t mutex_remote_imts_;
    pthread_mutex_t mutex_atomic_update_;

    std::unordered_map<int, papyruskv_update_fn_t> user_ufns_;

public:
    static DB* Create(unsigned long dbid, const char* name, int flags, papyruskv_option_t* opt, Platform* platform);

};

} /* namespace papyruskv */

#endif /* PAPYRUS_KV_SRC_DB_H */

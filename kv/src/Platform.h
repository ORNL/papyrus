#ifndef PAPYRUS_KV_SRC_PLATFORM_H
#define PAPYRUS_KV_SRC_PLATFORM_H

#include <mpi.h>
#include "papyrus/kv.h"
#include "DB.h"
#include "Compactor.h"
#include "Dispatcher.h"
#include "Listener.h"
#include "Signal.h"
#include "Pool.h"
#include "Bloom.h"
#include "Hasher.h"

namespace papyruskv {

class Platform {
private:
    Platform();
    ~Platform();

public:
    int Init(int* argc, char*** argv, const char* repository);
    int Open(const char* name, int flags, papyruskv_option_t* opt, int* dbid);
    int Close(int dbid);
    int Put(int dbid, const char* key, size_t keylen, const char* val, size_t vallen);
    int Get(int dbid, const char* key, size_t keylen, char** val, size_t* vallen, papyruskv_pos_t* pos);
    int Delete(int dbid, const char* key, size_t keylen);
    int Free(char** val);
    int Fence(int dbid, int level);
    int Barrier(int dbid, int level);
    int SignalNotify(int signum, int* ranks, int count);
    int SignalWait(int signum, int* ranks, int count);
    int SetConsistency(int dbid, int consistency);
    int SetProtection(int dbid, int protection);
    int Checkpoint(int dbid, const char* path, int* event);
    int Restart(const char* path, const char* name, int prot, papyruskv_option_t* opt, int* dbid, int* event);
    int Destroy(int dbid, int* event);
    int Wait(int dbid, int event);

    int Hash(int dbid);
    int IterLocal(int dbid, papyruskv_iter_t* iter);
    int IterNext(int dbid, papyruskv_iter_t* iter);
    int RegisterUpdate(int dbid, int fnid, papyruskv_update_fn_t ufn);
    int Update(int dbid, const char* key, size_t keylen, papyruskv_pos_t* pos, int fnid, void* userin, size_t userinlen, void* userout, size_t useroutlen);

    int rank() const { return rank_; }
    int size() const { return size_; }
    int group() const { return group_; }
    int sstable_mode() const { return sstable_mode_; }
    MPI_Comm mpi_comm() const { return mpi_comm_; }
    MPI_Comm mpi_comm_ext() const { return mpi_comm_ext_; }
    const char* repository() const { return repository_; }

    DB* GetDB(int dbid);
    Compactor* compactor() { return compactor_; }
    Dispatcher* dispatcher() { return dispatcher_; }
    Pool* pool() { return pool_; }
    Hasher* hasher() { return hasher_; }
    Bloom* bloom() { return bloom_; }
    Signal* signal() { return signal_; }

    int consistency() const { return consistency_; }
    size_t memtable_size() const { return memtable_size_; }
    size_t remote_buf_size() const { return remote_buf_size_; }
    size_t remote_buf_entry_max() const { return remote_buf_entry_max_; }
    size_t cache_size() const { return cache_size_; }
    bool enable_cache_local() const { return enable_cache_local_; }
    bool enable_cache_remote() const { return enable_cache_remote_; }
    bool enable_bloom() const { return enable_bloom_; }
    bool force_redistribute() const { return force_redistribute_; }

    void set_umid(unsigned long mid) { umid_ = mid; }

private:
    unsigned long udbid_;
    unsigned long ucid_;
    unsigned long umid_;
    int ueid_;
    MPI_Comm mpi_comm_;
    MPI_Comm mpi_comm_ext_;
    int rank_;
    int size_;
    int group_;
    char name_[256];
    bool init_;
    char repository_[256];
    char repository_rank_[256];

    DB* db_[PAPYRUSKV_MAX_DB];
    Compactor* compactor_;
    Dispatcher* dispatcher_;
    Listener* listener_;
    Pool* pool_;
    Hasher* hasher_;
    Bloom* bloom_;
    Signal* signal_;

    size_t memtable_size_;
    size_t remote_buf_size_;
    size_t remote_buf_entry_max_;
    size_t cache_size_;
    int consistency_;
    int sstable_mode_;
    bool enable_cache_local_;
    bool enable_cache_remote_;
    bool enable_bloom_;
    bool force_redistribute_;
    bool destroy_repository_;

public:
    static Platform* GetPlatform();
    static int Finalize();
    static unsigned long NewDBID();
    static unsigned long NewCID();
    static unsigned long NewMID();
    static int NewEvtID();

private:
    static Platform* singleton_;
};

} /* namespace papyruskv */

#endif /* PAPYRUS_KV_SRC_PLATFORM_H */

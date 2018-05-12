#include "Platform.h"
#include "Command.h"
#include "Debug.h"
#include "Utils.h"
#include <string.h>
#include <stdio.h>

namespace papyruskv {

Platform::Platform() {
    init_ = false;
}

Platform::~Platform() {
    if (!init_) return;
    if (pool_) delete pool_;
    if (signal_) delete signal_;
    if (hasher_) delete hasher_;
    if (dispatcher_) delete dispatcher_;
    if (listener_) delete listener_;
    if (compactor_) delete compactor_;

    if (destroy_repository_) Utils::Rmdir(repository_);
}

int Platform::Init(int* argc, char*** argv, const char* repository) {
    if (init_) return PAPYRUSKV_ERR;

    int flag;
    MPI_Initialized(&flag);
    if (!flag) _error("FLAG[%d]", flag);

    MPI_Comm_dup(MPI_COMM_WORLD, &mpi_comm_);
    MPI_Comm_rank(mpi_comm_, &rank_);
    MPI_Comm_size(mpi_comm_, &size_);
    MPI_Get_processor_name(name_, &flag);
    sprintf(nick_, "[%5d:%s]", rank_, name_);

    MPI_Comm_dup(MPI_COMM_WORLD, &mpi_comm_ext_);

    memset(db_, 0, PAPYRUSKV_MAX_DB * sizeof(DB*));

    if (repository[0] == '$') {
        char* env = getenv(repository + 1);
        if (env) strcpy(repository_, env);
        else {
            _error("environment variable[%s] is not set", repository);
            return PAPYRUSKV_ERR;
        }
    } else strcpy(repository_, repository);

    int iret = Utils::Mkdir(repository_);
    if  (iret != 0) _trace("ret[%d] repository[%s]", iret, repository_);

    sprintf(repository_rank_, "%s/%d", repository_, rank_);
    iret = Utils::Mkdir(repository_rank_);
    if  (iret != 0) _trace("ret[%d] repository[%s]", iret, repository_rank_);

    char* env = getenv("PAPYRUSKV_GROUP_SIZE");
    int group_size = env ? atoi(env) : PAPYRUSKV_GROUP_SIZE;
    group_ = group_size == -1 ? rank_ : rank_ / group_size;

    env = getenv("PAPYRUSKV_CONSISTENCY");
    consistency_ = env ? atoi(env) : PAPYRUSKV_RELAXED;

    env = getenv("PAPYRUSKV_SSTABLE");
    sstable_mode_ = env ? atoi(env) : PAPYRUSKV_SSTABLE_BIN; 

    env = getenv("PAPYRUSKV_CACHE_LOCAL");
    enable_cache_local_ = env ? atoi(env) > 0 : PAPYRUSKV_CACHE_LOCAL;

    env = getenv("PAPYRUSKV_CACHE_REMOTE");
    enable_cache_remote_ = env ? atoi(env) > 0 : PAPYRUSKV_CACHE_REMOTE;

    env = getenv("PAPYRUSKV_BLOOM");
    enable_bloom_ = env ? atoi(env) > 0 : PAPYRUSKV_BLOOM;

    env = getenv("PAPYRUSKV_MEMTABLE_SIZE");
    memtable_size_ = env ? atol(env) : PAPYRUSKV_MEMTABLE_SIZE;

    env = getenv("PAPYRUSKV_REMOTE_BUFFER_SIZE");
    remote_buf_size_ = env ? atol(env) : PAPYRUSKV_REMOTE_BUFFER_SIZE;

    env = getenv("PAPYRUSKV_REMOTE_BUFFER_ENTRY_MAX");
    remote_buf_entry_max_ = env ? atol(env) : PAPYRUSKV_REMOTE_BUFFER_ENTRY_MAX;

    env = getenv("PAPYRUSKV_CACHE_SIZE");
    cache_size_ = env ? atol(env) : PAPYRUSKV_CACHE_SIZE;

    env = getenv("PAPYRUSKV_FORCE_REDISTRIBUTE");
    force_redistribute_ = env ? atoi(env) > 0 : PAPYRUSKV_FORCE_REDISTRIBUTE;

    env = getenv("PAPYRUSKV_DESTROY_REPOSITORY");
    destroy_repository_ = env ? atoi(env) > 0 : PAPYRUSKV_DESTROY_REPOSITORY;

    udbid_ = 0UL;
    ucid_ = PAPYRUSKV_MSG_TAG + 1 + rank_ * 7;
    umid_ = 0UL;
    ueid_ = 0;

    _trace("platform[%s] rank[%d/%d] group[%d]", name_, rank_, size_, group_);
    if (rank_ == 0)
        _info("PapyrusKV nranks[%d] repository[%s] storage_group[%d] consistency[%x] memtable[%lu] [%lu]MB remotebuf[%lu] [%lu]KB total_remotebuf[%lu] [%lu]MB remotebuf_entry_max[%lu] cache[%lu] [%lu]MB cache_local[%d] cache_remote[%d] sstable[%x] bloom[%d] force_redistribute[%d] destroy_repository[%d]", size_, repository_, group_size, consistency_, memtable_size_, memtable_size_ / 1024 / 1024, remote_buf_size_, remote_buf_size_ / 1024, remote_buf_size_ * size_, remote_buf_size_ * size_ / 1024 / 1024, remote_buf_entry_max_, cache_size_, cache_size_ / 1024 / 1024, enable_cache_local_, enable_cache_remote_, sstable_mode_, enable_bloom_, force_redistribute_, destroy_repository_);

    pool_ = new Pool(this);

    hasher_ = new Hasher(size_);
    bloom_ = new Bloom(hasher_, PAPYRUSKV_BLOOM_BITS);
    
    dispatcher_ = new Dispatcher(this);
    dispatcher_->Start();

    listener_ = new Listener(this);
    listener_->Start();

    compactor_ = new Compactor(this);
    compactor_->Start();

    signal_ = new Signal(this);

    init_ = true;

    MPI_Barrier(mpi_comm_);

    return PAPYRUSKV_OK;
}

int Platform::Open(const char* name, int flags, papyruskv_option_t* opt, int* dbid) {
    int id = NewDBID();
    if (id >= PAPYRUSKV_MAX_DB) {
        _error("dbid[%d]", id);
        if (dbid) *dbid = -1;
        return PAPYRUSKV_ERR;
    }
    db_[id] = DB::Create(id, name, flags & ~PAPYRUSKV_CREATE, opt, this);
    MPI_Barrier(mpi_comm_);
    if (dbid) *dbid = id;
    return PAPYRUSKV_OK;
}

int Platform::Close(int dbid) {
    int ret = Barrier(dbid, destroy_repository_ ? PAPYRUSKV_MEMTABLE : PAPYRUSKV_SSTABLE);
    delete db_[dbid];
    return ret;
}

int Platform::Put(int dbid, const char* key, size_t keylen, const char* val, size_t vallen) {
    return GetDB(dbid)->Put(key, keylen, val, vallen);
}

int Platform::Get(int dbid, const char* key, size_t keylen, char** val, size_t* vallen, papyruskv_pos_t* pos) {
    return GetDB(dbid)->Get(key, keylen, val, vallen, pos);
}

int Platform::Delete(int dbid, const char* key, size_t keylen) {
    return GetDB(dbid)->Delete(key, keylen);
}

int Platform::Free(char** val) {
    pool_->FreeVal(val);
    return PAPYRUSKV_OK;
}

int Platform::Fence(int dbid, int level) {
    return GetDB(dbid)->Fence(level);
}

int Platform::Barrier(int dbid, int level) {
    return GetDB(dbid)->Barrier(level);
}

int Platform::SignalNotify(int signum, int* ranks, int count) {
    return signal_->Notify(signum, ranks, count);
}

int Platform::SignalWait(int signum, int* ranks, int count) {
    return signal_->Wait(signum, ranks, count);
}

int Platform::SetConsistency(int dbid, int consistency) {
    return GetDB(dbid)->SetConsistency(consistency);
}

int Platform::SetProtection(int dbid, int protection) {
    return GetDB(dbid)->SetProtection(protection);
}

int Platform::Checkpoint(int dbid, const char* path, int* event) {
    return GetDB(dbid)->Checkpoint(path, event);
}

int Platform::Restart(const char* path, const char* name, int flags, papyruskv_option_t* opt, int* dbid, int* event) {
    int id = NewDBID();
    if (id >= PAPYRUSKV_MAX_DB) {
        _error("dbid[%d]", id);
        if (dbid) *dbid = -1;
        return PAPYRUSKV_ERR;
    }
    db_[id] = DB::Create(id, name, flags, opt, this);
//    db_[id]->RegisterHash(hfn);
    int ret = db_[id]->Restart(path, event);
    if (ret == PAPYRUSKV_OK) {
        if (dbid) *dbid = id;
    } else if (dbid) *dbid = -1;
    return ret;
}

int Platform::Destroy(int dbid, int* event) {
    MPI_Barrier(mpi_comm_);
    MPI_Barrier(mpi_comm_ext_);
    Utils::Rmdir(repository_);
    return PAPYRUSKV_OK;
}

int Platform::Wait(int dbid, int event) {
    return GetDB(dbid)->Wait(event);
}

int Platform::Hash(int dbid) {
    return GetDB(dbid)->Hash();
}

int Platform::IterLocal(int dbid, papyruskv_iter_t* iter) {
    return GetDB(dbid)->IterLocal(iter);
}

int Platform::IterNext(int dbid, papyruskv_iter_t* iter) {
    return GetDB(dbid)->IterNext(iter);
}

int Platform::RegisterUpdate(int dbid, int fnid, papyruskv_update_fn_t ufn) {
    return GetDB(dbid)->RegisterUpdate(fnid, ufn);
}

int Platform::Update(int dbid, const char* key, size_t keylen, papyruskv_pos_t* pos, int fnid, void* userin, size_t userinlen, void* userout, size_t useroutlen) {
    return GetDB(dbid)->Update(key, keylen, pos, fnid, userin, userinlen, userout, useroutlen);
}

DB* Platform::GetDB(int dbid) {
    if (db_[dbid] == NULL) _error("dbid[%d]", dbid);
    return db_[dbid];
}

char nick_[256];

Platform* Platform::singleton_ = NULL;

Platform* Platform::GetPlatform() {
    if (singleton_ == NULL) singleton_ = new Platform();
    return singleton_;
}

int Platform::Finalize() {
    if (singleton_ == NULL) return PAPYRUSKV_ERR;
    MPI_Barrier(singleton_->mpi_comm_);
    if (singleton_) delete singleton_;
    singleton_ = NULL;
    return PAPYRUSKV_OK;
}

unsigned long Platform::NewDBID() {
    unsigned long new_uid;
    do new_uid = singleton_->udbid_ + 1;
    while (!__sync_bool_compare_and_swap(&singleton_->udbid_, singleton_->udbid_, new_uid));
    return new_uid;
}

unsigned long Platform::NewCID() {
    unsigned long new_uid;
    do new_uid = singleton_->ucid_ + 1;
    while (!__sync_bool_compare_and_swap(&singleton_->ucid_, singleton_->ucid_, new_uid));
    return new_uid;
}

unsigned long Platform::NewMID() {
    unsigned long new_uid;
    do new_uid = singleton_->umid_ + 1;
    while (!__sync_bool_compare_and_swap(&singleton_->umid_, singleton_->umid_, new_uid));
    return new_uid;
}

int Platform::NewEvtID() {
    int new_uid;
    do new_uid = singleton_->ueid_ + 1;
    while (!__sync_bool_compare_and_swap(&singleton_->ueid_, singleton_->ueid_, new_uid));
    return new_uid;
}

} /* namespace papyruskv */

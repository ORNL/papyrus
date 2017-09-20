#include "DB.h"
#include "Debug.h"
#include "Platform.h"
#include <string.h>

namespace papyruskv {

DB::DB(unsigned long dbid, const char* name, int flags, papyruskv_option_t* opt, Platform* platform) {
    dbid_ = dbid;
    size_t len = strlen(name);
    if (len >= PAPYRUSKV_MAX_DB_NAME) _error("name[%s]", name);
    strncpy(name_, name, len);
    name_[len] = 0;
    consistency_ = flags & (PAPYRUSKV_SEQUENTIAL | PAPYRUSKV_RELAXED);
    if (consistency_ != PAPYRUSKV_SEQUENTIAL && consistency_ != PAPYRUSKV_RELAXED)
        consistency_ = platform->consistency();
    protection_ = flags & (PAPYRUSKV_RDWR | PAPYRUSKV_WRONLY | PAPYRUSKV_RDONLY | PAPYRUSKV_UDONLY);
    if (protection_ != PAPYRUSKV_RDWR && protection_ != PAPYRUSKV_WRONLY && protection_ != PAPYRUSKV_RDONLY && protection_ != PAPYRUSKV_UDONLY)
        protection_ = PAPYRUSKV_RDWR;

    platform_ = platform;
    rank_ = platform->rank();
    nranks_ = platform->size();
    group_ = platform->group();
    hasher_ = platform->hasher();
    dispatcher_ = platform->dispatcher();
    compactor_ = platform->compactor();
    mpi_comm_ = platform->mpi_comm();
    mpi_comm_ext_ = platform->mpi_comm_ext();
    memtable_size_ = platform->memtable_size();
    remote_buf_size_ = platform->remote_buf_size();
    cache_size_ = platform->cache_size();

    keylen_ = opt ? opt->keylen : 0UL;
    vallen_ = opt ? opt->vallen : 0UL;
    if (opt) hasher_->set_hash(opt->hash);
    enable_remote_buffer_ = vallen_ > 0UL && vallen_ <= platform->remote_buf_entry_max();

    local_mt_ = new MemTable(this, true);
    remote_mt_ = new MemTable(this);
    remote_buf_ = new RemoteBuffer(this, remote_buf_size_, nranks_);
    local_cache_ = new Cache(this, cache_size_, true);
    remote_cache_ = new Cache(this, cache_size_, false);
    sstable_ = new SSTable(this, platform->sstable_mode());

    if (posix_memalign((void**) &big_buffer_, 0x1000, PAPYRUSKV_BIG_BUFFER) != 0) _error("size[%lu]", PAPYRUSKV_BIG_BUFFER);

    local_cache_->Enable(platform->enable_cache_local());
    remote_cache_->Enable(platform->enable_cache_remote());

    pthread_mutex_init(&mutex_local_mt_, NULL);
    pthread_mutex_init(&mutex_local_imts_, NULL);
    pthread_mutex_init(&mutex_remote_imts_, NULL);
    pthread_mutex_init(&mutex_atomic_update_, NULL);

    _trace("dbid[%lu] name[%s] consistency[%x] protection[%x] keylen[%lu] vallen[%lu] hash[%p] remote_buffer[%d]", dbid_, name_, consistency_, protection_, keylen_, vallen_, hasher_->hash(), enable_remote_buffer_);

}

DB::~DB() {
    WaitAll();
    delete local_mt_;
    delete remote_mt_;
    delete remote_buf_;
    delete local_cache_;
    delete remote_cache_;
    delete sstable_;
    if (big_buffer_) free(big_buffer_);
    pthread_mutex_destroy(&mutex_local_mt_);
    pthread_mutex_destroy(&mutex_local_imts_);
    pthread_mutex_destroy(&mutex_remote_imts_);
    pthread_mutex_destroy(&mutex_atomic_update_);
}

int DB::Put(const char* key, size_t keylen, const char* val, size_t vallen) {
    if (protection_ == PAPYRUSKV_RDONLY) {
        _error("dbid[%lu] protection[0x%x]", dbid_, protection_);
        return PAPYRUSKV_ERR;
    }
    int rank = hasher_->KeyRank(key, keylen);
    _trace("key[%s] keylen[%lu] val[%s] vallen[%lu] rank[%d]", key, keylen, val, vallen, rank);
    return rank == rank_ ? PutLocal(key, keylen, val, vallen, false) : PutRemote(key, keylen, val, vallen, false, rank);
}

int DB::PutLocal(Slice* slice) {
    if (protection_ == PAPYRUSKV_RDWR || protection_ == PAPYRUSKV_UDONLY)
        local_cache_->Invalidate(slice->key(), slice->keylen());
    pthread_mutex_lock(&mutex_local_mt_);
    size_t size = local_mt_->Put(slice);
    if (size < memtable_size_) {
        pthread_mutex_unlock(&mutex_local_mt_);
        return PAPYRUSKV_OK;
    }
    int ret = Flush(false, false);
    if (ret != PAPYRUSKV_OK) _error("ret[%d]", ret);
    pthread_mutex_unlock(&mutex_local_mt_);
    return ret;
}

int DB::PutLocal(const char* key, size_t keylen, const char* val, size_t vallen, bool tombstone) {
    return PutLocal(new Slice(key, keylen, val, vallen, rank_, tombstone));
}

int DB::PutRemote(const char* key, size_t keylen, const char* val, size_t vallen, bool tombstone, int rank) {
    _trace("key[%s] keylen[%lu] val[%s] vallen[%lu] tombstone[%d] rank[%d]", key, keylen, val, vallen, tombstone, rank);
    if (protection_ == PAPYRUSKV_RDWR || protection_ == PAPYRUSKV_UDONLY)
        remote_cache_->Invalidate(key, keylen);
    if (consistency_ == PAPYRUSKV_SEQUENTIAL) {
        return dispatcher_->ExecutePut(this, key, keylen, val, vallen, tombstone, true, rank);
    }
    if (consistency_ == PAPYRUSKV_RELAXED) {
        if (enable_remote_buffer_) {
            bool available = remote_buf_->Available(keylen, vallen, rank);
            if (!available) {
                int ret = Migrate(rank, false, PAPYRUSKV_MEMTABLE);
                if (ret != PAPYRUSKV_OK) _error("ret[%d] rank[%d]", ret, rank);
            }
            return remote_buf_->Put(key, keylen, val, vallen, tombstone, rank);
        }
        size_t size = remote_mt_->Put(new Slice(key, keylen, val, vallen, rank, tombstone));
        if (size < memtable_size_) return PAPYRUSKV_OK;
        return Migrate(rank, false, PAPYRUSKV_MEMTABLE);
    }
    return PAPYRUSKV_ERR;
}

int DB::Get(const char* key, size_t keylen, char** valp, size_t* vallenp, papyruskv_pos_t* pos) {
    int rank = hasher_->KeyRank(key, keylen);
    int ret = rank == rank_ ? GetLocal(key, keylen, valp, vallenp, PAPYRUSKV_MEMTABLE | PAPYRUSKV_SSTABLE, pos) : GetRemote(key, keylen, valp, vallenp, rank, pos);
    _trace("rank[%d] key[%s] keylen[%lu] val[%s] vallen[%lu] ret[%x]", rank, key, keylen, *valp, *vallenp, ret);
    if (ret != PAPYRUSKV_SLICE_FOUND) _trace("rank[%d] key[%s] keylen[%lu] val[%s] vallen[%lu] ret[%x]", rank, key, keylen, *valp, *vallenp, ret);
    return ret == PAPYRUSKV_SLICE_FOUND ? PAPYRUSKV_OK : PAPYRUSKV_ERR;
}

int DB::GetLocal(const char* key, size_t keylen, char** valp, size_t* vallenp, int mode, papyruskv_pos_t* pos) {
    int ret = PAPYRUSKV_SLICE_NOT_FOUND;
    if (pos) pos->handle = NULL;
    if (mode & PAPYRUSKV_MEMTABLE) {
        pthread_mutex_lock(&mutex_local_mt_);
        ret = local_mt_->Get(key, keylen, valp, vallenp, pos);
        if (ret != PAPYRUSKV_SLICE_NOT_FOUND) {
            pthread_mutex_unlock(&mutex_local_mt_);
            return ret;
        }
        pthread_mutex_unlock(&mutex_local_mt_);

        pthread_mutex_lock(&mutex_local_imts_);
        for (auto it = local_imts_.begin(); it != local_imts_.end(); ++it) {
            ret = (*it)->Get(key, keylen, valp, vallenp);
            if (ret != PAPYRUSKV_SLICE_NOT_FOUND) {
                pthread_mutex_unlock(&mutex_local_imts_);
                return ret;
            }
        }
        pthread_mutex_unlock(&mutex_local_imts_);

        ret = local_cache_->Get(key, keylen, valp, vallenp);
        if (ret != PAPYRUSKV_SLICE_NOT_FOUND) return ret;
    }

    if (mode & PAPYRUSKV_SSTABLE) return GetSST(key, keylen, valp, vallenp, rank_);

    return PAPYRUSKV_SLICE_NOT_FOUND;
}

int DB::GetSST(const char* key, size_t keylen, char** valp, size_t* vallenp, int rank, uint64_t sid) {
    bool local = rank_ == rank;
    int ret = local ?
        sstable_->Get(key, keylen, valp, vallenp) :
        sstable_->Get(key, keylen, valp, vallenp, rank, sid);

    _trace("key[%s] keylen[%lu] val[%s] vallen[%lu] rank[%d] sid[%lu] local[%d]", key, keylen, *valp, *vallenp, rank, sid, local);
    
    if (local) {
        if (ret == PAPYRUSKV_SLICE_FOUND)
            local_cache_->Put(key, keylen, *valp, *vallenp, rank, false);
        else if (ret == PAPYRUSKV_SLICE_TOMBSTONE)
            local_cache_->Put(key, keylen, *valp, *vallenp, rank, true);
        else if (ret == PAPYRUSKV_SLICE_NOT_FOUND)
            local_cache_->Put(key, keylen, NULL, 0, rank, true);
        return ret;
    }
    if (protection_ == PAPYRUSKV_RDONLY) {
        if (ret == PAPYRUSKV_SLICE_FOUND)
            remote_cache_->Put(key, keylen, *valp, *vallenp, rank, false);
        else if (ret == PAPYRUSKV_SLICE_TOMBSTONE)
            remote_cache_->Put(key, keylen, *valp, *vallenp, rank, true);
        else if (ret == PAPYRUSKV_SLICE_NOT_FOUND)
            remote_cache_->Put(key, keylen, NULL, 0, rank, true);
    }
    return ret;
}

int DB::GetRemote(const char* key, size_t keylen, char** valp, size_t* vallenp, int rank, papyruskv_pos_t* pos) {
    int ret = PAPYRUSKV_SLICE_NOT_FOUND;
    if (!enable_remote_buffer_ && consistency_ == PAPYRUSKV_RELAXED) {
        pthread_mutex_lock(&mutex_remote_imts_);
        ret = remote_mt_->Get(key, keylen, valp, vallenp);
        if (ret != PAPYRUSKV_SLICE_NOT_FOUND) {
            pthread_mutex_unlock(&mutex_remote_imts_);
            return ret;
        }

        for (auto it = remote_imts_.begin(); it != remote_imts_.end(); ++it) {
            ret = (*it)->Get(key, keylen, valp, vallenp);
            if (ret != PAPYRUSKV_SLICE_NOT_FOUND) {
                pthread_mutex_unlock(&mutex_remote_imts_);
                return ret;
            }
        }
        pthread_mutex_unlock(&mutex_remote_imts_);
    }

    if (pos) pos->handle = NULL;

    if (protection_ == PAPYRUSKV_RDONLY) {
        ret = remote_cache_->Get(key, keylen, valp, vallenp);
        if (ret != PAPYRUSKV_SLICE_NOT_FOUND) return ret;
    }

    if (ret == PAPYRUSKV_SLICE_NOT_FOUND) {
        ret = dispatcher_->ExecuteGet(this, key, keylen, valp, vallenp, group_, rank, pos);
        _trace("key[%s] keylen[%lu] val[%s] vallen[%lu] ret[%x]", key, keylen, *valp, *vallenp, ret);
    }

    if (protection_ == PAPYRUSKV_RDONLY) {
        if (ret == PAPYRUSKV_SLICE_FOUND)
            remote_cache_->Put(key, keylen, *valp, *vallenp, rank, false);
        else if (ret == PAPYRUSKV_SLICE_TOMBSTONE)
            remote_cache_->Put(key, keylen, *valp, *vallenp, rank, true);
        else if (ret == PAPYRUSKV_SLICE_NOT_FOUND)
            remote_cache_->Put(key, keylen, NULL, 0, rank, true);
    }

    return ret;
}

int DB::Delete(const char* key, size_t keylen) {
    if (protection_ == PAPYRUSKV_RDONLY) {
        _error("dbid[%lu] protection[%x]", dbid_, protection_);
        return PAPYRUSKV_ERR;
    }
    int rank = hasher_->KeyRank(key, keylen);
    _trace("key[%s] keylen[%lu] rank[%d]", key, keylen, rank);
    return rank == rank_ ? PutLocal(key, keylen, NULL, 0, true) : PutRemote(key, keylen, NULL, 0, true, rank);
}

int DB::Fence(int level) {
    return Migrate(-1, true, level);
}

int DB::Barrier(int level) {
    if (consistency_ == PAPYRUSKV_SEQUENTIAL) {
        MPI_Barrier(mpi_comm_);
        if (level & PAPYRUSKV_SSTABLE) return Flush(true);
        return PAPYRUSKV_OK;
    }
    int ret = Fence(PAPYRUSKV_MEMTABLE);
    if (ret != PAPYRUSKV_OK) {
        _error("dbid[%lu] ret[%d]", dbid_, ret);
        return ret;
    }
    Command* cmd = Command::CreateBarrier(this, level);
    dispatcher_->EnqueueWaitRelease(cmd);

    MPI_Barrier(mpi_comm_);

    return PAPYRUSKV_OK;
}

int DB::Migrate(int rank, bool sync, int level) {
    Command* cmd;
    if (enable_remote_buffer_) {
        if (rank == -1) return dispatcher_->ExecuteMigrate(remote_buf_, sync, level, rank);
        char* block = remote_buf_->Copy(rank);
        size_t size = remote_buf_->Size(rank);
        remote_buf_->Reset(rank);
        cmd = Command::CreateMigrate(this, block, size, rank, sync, level);
    } else {
        cmd = Command::CreateMigrate(remote_mt_, sync, level);
        pthread_mutex_lock(&mutex_remote_imts_);
        remote_imts_.push_front(remote_mt_);
        remote_mt_ = new MemTable(this);
        pthread_mutex_unlock(&mutex_remote_imts_);
    }
    if (sync) dispatcher_->EnqueueWaitRelease(cmd);
    else dispatcher_->Enqueue(cmd);
    return PAPYRUSKV_OK;
}

int DB::Flush(bool sync, bool lock) {
    Command* cmd = Command::CreateFlush(local_mt_, sync);
    if (lock) pthread_mutex_lock(&mutex_local_mt_);
    if (!local_mt_->Empty()) {
        local_mt_->SortByKey();
        pthread_mutex_lock(&mutex_local_imts_);
        local_imts_.push_front(local_mt_);
        pthread_mutex_unlock(&mutex_local_imts_);
        local_mt_ = new MemTable(this, true);
    }
    if (lock) pthread_mutex_unlock(&mutex_local_mt_);
    if (sync) compactor_->EnqueueWaitRelease(cmd);
    else compactor_->Enqueue(cmd);
    return PAPYRUSKV_OK;
}

int DB::SetConsistency(int consistency) {
    if (consistency_ == consistency) return PAPYRUSKV_OK;
    if (consistency != PAPYRUSKV_SEQUENTIAL && consistency != PAPYRUSKV_RELAXED) {
        _error("consistency[0x%x]", consistency);
        return PAPYRUSKV_ERR;
    }
    if (consistency == PAPYRUSKV_SEQUENTIAL) {
        int ret = Barrier(PAPYRUSKV_MEMTABLE);
        consistency_ = consistency;
        return ret;
    }
    MPI_Barrier(mpi_comm_);
    return PAPYRUSKV_OK;
}

int DB::SetProtection(int protection) {
    int ret = Barrier(PAPYRUSKV_MEMTABLE);
    if (ret != PAPYRUSKV_OK) {
        _error("ret[%d]", ret);
        return ret;
    }
    if (protection_ == protection) return PAPYRUSKV_OK;
    if (protection_ == PAPYRUSKV_WRONLY) local_cache_->InvalidateAll();
    if (protection == PAPYRUSKV_RDONLY) remote_cache_->InvalidateAll();
    if (protection != PAPYRUSKV_RDONLY && protection != PAPYRUSKV_UDONLY) local_mt_->ClearBucket();

    protection_ = protection;
    return PAPYRUSKV_OK;
}

int DB::Checkpoint(const char* path, int* event) {
    int ret = Barrier(PAPYRUSKV_SSTABLE);
    if (ret != PAPYRUSKV_OK) {
        _error("ret[%d]", ret);
        return ret;
    }

    ret = Flush(true);
    if (ret != PAPYRUSKV_OK) {
        _error("ret[%d]", ret);
        return ret;
    }

    uint64_t sid = sstable_->sid();
    uint64_t* sids = new uint64_t[nranks_];
    MPI_Gather(&sid, 1, MPI_LONG_LONG_INT, sids, 1, MPI_LONG_LONG_INT, 0, mpi_comm_);
    if (rank_ == 0) sstable_->WriteTOC(sids, nranks_, path);
    delete[] sids;

    if (event) {
        Command* cmd = Command::CreateCheckpoint(sstable_, sid, path);
        dispatcher_->EnqueueWait(cmd);
        *event = (int) cmd->cid();
        events_[*event] = cmd;
    } else sstable_->SendFiles(sid, path);

    MPI_Barrier(mpi_comm_);

    return PAPYRUSKV_OK;
}

int DB::Restart(const char* path, int* event) {
    uint64_t* sids = NULL;
    int size = 0;
    int ret = sstable_->ReadTOC(&sids, &size, path);
    if (ret != PAPYRUSKV_OK) return PAPYRUSKV_ERR;
    uint64_t sid = sids[rank_];

    if (Platform::GetPlatform()->force_redistribute() || nranks_ != size) {
        if (event) {
            Command* cmd = Command::CreateDistribute(sstable_, sids, size, path);
            dispatcher_->EnqueueWait(cmd);
            *event = (int) cmd->cid();
            events_[*event] = cmd;
        } else sstable_->DistributeFiles(sids, size, path);
    } else {
        if (event) {
            Command* cmd = Command::CreateRestart(sstable_, sid, path);
            dispatcher_->EnqueueWait(cmd);
            *event = (int) cmd->cid();
            events_[*event] = cmd;
        } else sstable_->RecvFiles(sid, path);
        delete[] sids;
        platform_->set_umid(sid + 1);
        local_mt_->set_mid(sid + 1);
    }

    return Barrier(PAPYRUSKV_MEMTABLE);
}

int DB::Wait(int event) {
    auto it = events_.find(event);
    if (it == events_.end()) return PAPYRUSKV_OK;
    Command* cmd = it->second;
    cmd->Wait();
    if (cmd->type() == PAPYRUSKV_CMD_RESTART) MPI_Barrier(mpi_comm_);
    Command::Release(cmd);
    events_.erase(it);
    return PAPYRUSKV_OK;
}

int DB::WaitAll() {
    for (auto it = events_.begin(); it != events_.end(); ++it) {
        Command* cmd = it->second;
        if (cmd->type() == PAPYRUSKV_CMD_RESTART) continue;
        cmd->Wait();
        Command::Release(cmd);
    }
    events_.clear();
    return PAPYRUSKV_OK;
}

int DB::Hash() {
    if (protection_ != PAPYRUSKV_RDONLY && protection_ != PAPYRUSKV_UDONLY)
        return PAPYRUSKV_ERR;
    pthread_mutex_lock(&mutex_local_mt_);
    if (!local_mt_->Empty()) local_mt_->Hash();
    pthread_mutex_unlock(&mutex_local_mt_);
    return PAPYRUSKV_OK;
}

int DB::IterLocal(papyruskv_iter_t* iter) {
    MemTable* head = NULL;
    MemTable* tail = NULL;

    pthread_mutex_lock(&mutex_local_mt_);
    if (!local_mt_->Empty()) {
#if 1
        head = MemTable::Duplicate(local_mt_);
#else
        head = local_mt_;
        head->Retain();
#endif
        head->SortByKey();
        tail = head;
    }
    pthread_mutex_unlock(&mutex_local_mt_);

    pthread_mutex_lock(&mutex_local_imts_);
    if (!local_imts_.empty()) {
        for (auto it = local_imts_.begin(); it != local_imts_.end(); ++it) {
            MemTable* copy = *it;
            copy->Retain();
            if (head == NULL) {
                head = copy;
                tail = head;
            } else tail->set_next(copy);
            tail = copy;
        }
    }
    pthread_mutex_unlock(&mutex_local_imts_);

    uint64_t sid = sstable_->sid();
    if (head == NULL && sid != 0) {
        head = new MemTable(this);
        sstable_->Load(head, sid);
        if (sid > 1) {
            MemTable* next = new MemTable(this);
            Command* cmd = Command::CreateLoad(next, sid - 1);
            compactor_->Enqueue(cmd);
            next->set_cmd(cmd);
            head->set_next(next);
        }
    }

    if (head == NULL) {
        *iter = NULL;
        return PAPYRUSKV_OK;
    }

    *iter = new struct _papyruskv_iter_t;
    Slice* slice = head->head();
    slice->CopyIter(*iter);

    return PAPYRUSKV_OK;
}

int DB::IterNext(papyruskv_iter_t* iter) {
    Slice* slice = (Slice*) (*iter)->handle;
    Slice* next = slice->next();
    if (next != NULL) {
        next->CopyIter(*iter);
        return PAPYRUSKV_OK;
    }
    MemTable* mt = slice->mt();
    MemTable* next_mt = mt->next();
    MemTable::Release(mt);
    if (next_mt == NULL) {
        *iter = NULL;
        return PAPYRUSKV_OK;
    }

    if (next_mt->cmd()) {
        Command* cmd = next_mt->cmd();
        cmd->Wait();
        Command::Release(cmd);
    }

    slice = next_mt->head();
    slice->CopyIter(*iter);

    MemTable* next_next_mt = next_mt->next();
    if (next_next_mt == NULL && next_mt->mid() > 1) {
        next_next_mt = new MemTable(this);
        Command* cmd = Command::CreateLoad(next_next_mt, next_mt->mid() - 1);
        compactor_->Enqueue(cmd);
        next_next_mt->set_cmd(cmd);
        next_mt->set_next(next_next_mt);
    }

    return PAPYRUSKV_OK;
}

int DB::RegisterUpdate(int fnid, papyruskv_update_fn_t ufn) {
    if (user_ufns_.count(fnid)) {
        _error("fnid[%d]", fnid);
        return PAPYRUSKV_ERR;
    }
    user_ufns_[fnid] = ufn;
    MPI_Barrier(mpi_comm_);
    return PAPYRUSKV_OK;
}

int DB::Update(const char* key, size_t keylen, papyruskv_pos_t* pos, int fnid, void* userin, size_t userinlen, void* userout, size_t useroutlen) {
    if (!user_ufns_.count(fnid)) {
        _error("fnid[%d]", fnid);
        return PAPYRUSKV_ERR;
    }
    int rank = hasher_->KeyRank(key, keylen);
    int ret = rank == rank_ ?
        UpdateLocal(key, keylen, pos, fnid, userin, userinlen, userout, useroutlen) :
        UpdateRemote(key, keylen, pos, fnid, userin, userinlen, userout, useroutlen, rank);
    return ret;
}

int DB::UpdateLocal(const char* key, size_t keylen, papyruskv_pos_t* pos, int fnid, void* userin, size_t userinlen, void* userout, size_t useroutlen) {
    char* val = big_buffer_;
    size_t vallen = 0UL;
    int iret;
    papyruskv_pos_t new_pos;
    papyruskv_update_fn_t ufn = user_ufns_[fnid];

    pthread_mutex_lock(&mutex_atomic_update_);

    if (protection_ == PAPYRUSKV_UDONLY && pos && pos->handle) {
        Slice* slice = (Slice*) pos->handle;
        vallen = slice->vallen();
        memcpy(val, slice->val(), vallen);
        new_pos.handle = pos->handle;
    } else {
        iret = GetLocal(key, keylen, &val, &vallen, PAPYRUSKV_MEMTABLE | PAPYRUSKV_SSTABLE, &new_pos);
        if (iret != PAPYRUSKV_SLICE_FOUND) _error("ret[%d] key[%s] keylen[%lu]", iret, key, keylen);
    }

    int update = (ufn)(key, keylen, &val, &vallen, userin, userinlen, userout, useroutlen);
    _trace("update[%d] key[%s] keylen[%lu] val[%s] vallen[%lu] userin[%s] userinlen[%lu] userout[%s] useroutlen[%lu]", update, key, keylen, val, vallen, (char*) userin, userinlen, (char*) userout, useroutlen);
    if (update) {
        if (protection_ == PAPYRUSKV_UDONLY && new_pos.handle) {
            Slice* slice = (Slice*) new_pos.handle;
            memcpy(slice->val(), val, vallen);
        } else {
            iret = PutLocal(key, keylen, (const char*) val, vallen, false);
            if (iret != PAPYRUSKV_OK) _error("ret[%d] key[%s] keylen[%lu] val[%s] vallen[%lu]", iret, key, keylen, val, vallen);
        }
    }

    pthread_mutex_unlock(&mutex_atomic_update_);
    return PAPYRUSKV_OK;
}

int DB::UpdateRemote(const char* key, size_t keylen, papyruskv_pos_t* pos, int fnid, void* userin, size_t userinlen, void* userout, size_t useroutlen, int rank) {
    return dispatcher_->ExecuteUpdate(this, key, keylen, pos, fnid, userin, userinlen, userout, useroutlen, rank);
}

void DB::RemoveLocalIMT(MemTable* mt) {
    pthread_mutex_lock(&mutex_local_imts_);
    for (auto it = local_imts_.begin(); it != local_imts_.end(); ++it) {
        if (mt == *it) {
            local_imts_.erase(it);
            break;
        }
    }
    pthread_mutex_unlock(&mutex_local_imts_);
}

void DB::RemoveRemoteIMT(MemTable* mt) {
    pthread_mutex_lock(&mutex_remote_imts_);
    for (auto it = remote_imts_.begin(); it != remote_imts_.end(); ++it) {
        if (mt == *it) {
            remote_imts_.erase(it);
            break;
        }
    }
    pthread_mutex_unlock(&mutex_remote_imts_);
}

DB* DB::Create(unsigned long dbid, const char* name, int flags, papyruskv_option_t* opt, Platform* platform) {
    return new DB(dbid, name, flags, opt, platform);
}

} /* namespace papyruskv */

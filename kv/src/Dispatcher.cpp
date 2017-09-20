#include "Dispatcher.h"
#include "Debug.h"
#include "Command.h"
#include "Message.h"
#include "Platform.h"
#include "SSTable.h"
#include "RemoteBuffer.h"
#include <string.h>

namespace papyruskv {

Dispatcher::Dispatcher(Platform* platform) {
    platform_ = platform;
    mpi_comm_ = platform_->mpi_comm();
    mpi_comm_ext_ = platform_->mpi_comm_ext();
    pool_ = platform_->pool();
    rank_ = platform->rank();
    nranks_ = platform->size();
    queue_ = new LockFreeQueueMS<Command*>(1024);
    if (posix_memalign((void**) &big_buffer_, 0x1000, PAPYRUSKV_BIG_BUFFER) != 0) _error("%p", big_buffer_);
    if (posix_memalign((void**) &ret_buffer_, 0x1000, sizeof(int)) != 0) _error("%p", ret_buffer_);
}

Dispatcher::~Dispatcher() {
    if (big_buffer_) free(big_buffer_);
    if (ret_buffer_) free(ret_buffer_);
}

void Dispatcher::Enqueue(Command* cmd) {
    while (!queue_->Enqueue(cmd)) {}
    Invoke();
}

void Dispatcher::EnqueueWait(Command* cmd) {
    Enqueue(cmd);
    cmd->Wait();
}

void Dispatcher::EnqueueWaitRelease(Command* cmd) {
    EnqueueWait(cmd);
    Command::Release(cmd);
}

void Dispatcher::Run() {
    while (true) {
        sem_wait(&sem_);
        if (!running_) break;
        Command* cmd = NULL;
        while (queue_->Dequeue(&cmd)) Execute(cmd);
    }
}

void Dispatcher::Execute(Command* cmd) {
    _trace("cmd[%lu] type[%x]", cmd->cid(), cmd->type());
    switch (cmd->type()) {
        case PAPYRUSKV_CMD_MIGRATE:     ExecuteMigrate(cmd);    break;
        case PAPYRUSKV_CMD_BARRIER:     ExecuteBarrier(cmd);    break;
        case PAPYRUSKV_CMD_CHECKPOINT:  ExecuteCheckpoint(cmd); break;
        case PAPYRUSKV_CMD_RESTART:     ExecuteRestart(cmd);    break;
        case PAPYRUSKV_CMD_DISTRIBUTE:  ExecuteDistribute(cmd); break;
        default: _error("not supported command type[0x%x]", cmd->type());
    }
}

int Dispatcher::ExecutePut(DB* db, const char* key, size_t keylen, const char* val, size_t vallen, bool tombstone, bool sync, int rank) {
    unsigned long cid = Platform::NewCID();
    int tag = Tag(cid);

    Message msg(PAPYRUSKV_MSG_PUT);
    msg.WriteULong(db->dbid());
    msg.WriteInt(tag);
    msg.WriteULong(keylen);
    msg.WriteULong(vallen);
    msg.WriteBool(tombstone);
    msg.WriteBool(sync);
    msg.Send(rank, mpi_comm_);

    memcpy(big_buffer_, key, keylen);
    memcpy(big_buffer_ + keylen, val, vallen);

    int size = keylen + vallen;
    MPI_Send(big_buffer_, size, MPI_CHAR, rank, tag, mpi_comm_);

    if (!sync) return PAPYRUSKV_OK;

    MPI_Recv(ret_buffer_, 1, MPI_INT, rank, tag, mpi_comm_ext_, MPI_STATUS_IGNORE);
    return *ret_buffer_;
}

int Dispatcher::ExecuteGet(DB* db, const char* key, size_t keylen, char** valp, size_t* vallenp, int group, int rank, papyruskv_pos_t* pos) {
    unsigned long cid = Platform::NewCID();
    int tag = Tag(cid);

    _trace("cid[%lu] tag[%d] key[%s] keylen[%lu] valp[%p] vallenp[%p] group[%d] rank[%d]", cid, tag, key, keylen, valp, vallenp, group, rank);

    Message msg(PAPYRUSKV_MSG_GET);
    msg.WriteULong(db->dbid());
    msg.WriteInt(tag);
    msg.WriteULong(keylen);
    msg.Write(key, keylen);
    msg.WritePtr(valp);
    msg.WriteInt(group);
    msg.Send(rank, mpi_comm_);

    if (db->vallen()) MPI_Recv(big_buffer_, 5 * sizeof(size_t) + db->vallen(), MPI_CHAR, rank, tag, mpi_comm_ext_, MPI_STATUS_IGNORE);
    else MPI_Recv(big_buffer_, 5, MPI_LONG_LONG, rank, tag, mpi_comm_ext_, MPI_STATUS_IGNORE);

    size_t* packet = (size_t*) big_buffer_;
    int ret = (int) packet[0];
    int mode = (int) packet[1];
    size_t vallen = packet[2];
    uint64_t sid = packet[3];
    size_t pos_handle = packet[4];

    _trace("ret[%d] mode[%d] vallen[%lu] sid[%lu] pos_handle[0x%x]", ret, mode, vallen, sid, pos_handle);

    if (ret == PAPYRUSKV_SLICE_FOUND) {
        if (vallenp) *vallenp = vallen;
        if (valp) {
            if (*valp == NULL) *valp = pool_->AllocVal(vallen);
            if (db->keylen()) {
                memcpy(*valp, big_buffer_ + 5 * sizeof(size_t), vallen);
            }
            else MPI_Recv(*valp, (int) vallen, MPI_CHAR, rank, tag, mpi_comm_ext_, MPI_STATUS_IGNORE);
        }
        if (pos && pos_handle) pos->handle = (void*) pos_handle;
    } else if (ret == PAPYRUSKV_SLICE_NOT_FOUND && mode == PAPYRUSKV_MEMTABLE) {
        ret = db->GetSST(key, keylen, valp, vallenp, rank, sid);
    }
    return ret;
}

void Dispatcher::ExecuteMigrate(Command* cmd) {
    return cmd->db()->enable_remote_buffer() ? ExecuteMigrateRemoteBuffer(cmd) : ExecuteMigrateMemTable(cmd);
}

void Dispatcher::ExecuteMigrateRemoteBuffer(Command* cmd) {
    int tag = Tag(cmd->cid());
    bool sync = cmd->sync();

    Message msg(PAPYRUSKV_MSG_MIGRATE);
    msg.WriteULong(cmd->dbid());
    msg.WriteInt(tag);
    msg.WriteBool(sync);
    msg.WriteInt(cmd->level());
    msg.WriteULong(cmd->size());
    msg.Send(cmd->rank(), mpi_comm_);

    MPI_Send(cmd->block(), (int) cmd->size(), MPI_CHAR, cmd->rank(), tag, mpi_comm_);
    MPI_Recv(ret_buffer_, 1, MPI_INT, cmd->rank(), tag, mpi_comm_ext_, MPI_STATUS_IGNORE);

    free(cmd->block());

    cmd->Complete();
    if (!sync) Command::Release(cmd);
}

void Dispatcher::ExecuteMigrateMemTable(Command* cmd) {
    MemTable* mt = cmd->mt();
    int tag = Tag(cmd->cid());
    bool sync = cmd->sync();
    for (Slice* slice = mt->SortByKey(); slice; slice = slice->next()) {
        _trace("cmd[%lu] dbid[%d] key[%s] keylen[%lu] val[%s] vallen[%lu] tombstone[%d] sync[%d] level[0x%d]", cmd->cid(), cmd->dbid(), slice->key(), slice->keylen(), slice->val(), slice->vallen(), slice->tombstone(), sync, cmd->level());
        Message msg(PAPYRUSKV_MSG_PUT);
        msg.WriteULong(cmd->dbid());
        msg.WriteInt(tag);
        msg.WriteULong(slice->keylen());
        msg.WriteULong(slice->vallen());
        msg.WriteBool(slice->tombstone());
        msg.WriteBool(sync);
        msg.Send(slice->rank(), mpi_comm_);
        MPI_Send(slice->buf(), (int) slice->kvsize(), MPI_CHAR, slice->rank(), tag, mpi_comm_);
        if (sync) {
            MPI_Recv(ret_buffer_, 1, MPI_INT, slice->rank(), tag, mpi_comm_ext_, MPI_STATUS_IGNORE);
        }
    }

    mt->db()->RemoveRemoteIMT(mt);
    delete mt;

    cmd->Complete();
    if (!sync) Command::Release(cmd);
}

int Dispatcher::ExecuteMigrate(RemoteBuffer* rb, bool sync, int level, int rank) {
    unsigned long cid = Platform::NewCID();
    int tag = Tag(cid);
    unsigned long dbid = rb->db()->dbid();

    int begin = rank;
    int end = rank + 1;
    if (rank == -1) {
        begin = 0;
        end = nranks_;
    }

    for (int rank = begin; rank < end; rank++) {
        Message msg(PAPYRUSKV_MSG_MIGRATE);
        msg.WriteULong(dbid);
        msg.WriteInt(tag);
        msg.WriteBool(sync);
        msg.WriteInt(level);
        msg.WriteULong(rb->Size(rank));
        msg.Send(rank, mpi_comm_);

        //todo:isend & irecv
        MPI_Send(rb->Data(rank), (int) rb->Size(rank), MPI_CHAR, rank, tag, mpi_comm_);
        if (sync) MPI_Recv(ret_buffer_, 1, MPI_INT, rank, tag, mpi_comm_ext_, MPI_STATUS_IGNORE);
        rb->Reset(rank);
    }
    return PAPYRUSKV_OK;
}

int Dispatcher::ExecuteSignal(int signum, int* ranks, int count) {
    for (int i = 0; i < count; i++) {
        _trace("signum[0x%x] ranks[%d] count[%d]", signum, ranks[i], count);
    }

    Message msg(PAPYRUSKV_MSG_SIGNAL);
    msg.WriteInt(signum);
    for (int i = 0; i < count; i++) msg.Send(ranks[i], mpi_comm_);
    return PAPYRUSKV_OK;
}

void Dispatcher::ExecuteBarrier(Command* cmd) {
    Message msg(PAPYRUSKV_MSG_BARRIER);
    msg.WriteULong(cmd->dbid());
    msg.WriteInt(cmd->level());
    msg.WritePtr(cmd);
    msg.Send(rank_, mpi_comm_);
}

void Dispatcher::ExecuteCheckpoint(Command* cmd) {
    SSTable* sstable = cmd->sstable();
    uint64_t sid = cmd->sid();
    const char* path = cmd->path();
    sstable->SendFiles(sid, path);
    cmd->Complete();
}

void Dispatcher::ExecuteRestart(Command* cmd) {
    SSTable* sstable = cmd->sstable();
    uint64_t sid = cmd->sid();
    const char* path = cmd->path();
    sstable->RecvFiles(sid, path);
    cmd->Complete();
}

void Dispatcher::ExecuteDistribute(Command* cmd) {
    SSTable* sstable = cmd->sstable();
    uint64_t* sids = cmd->sids();
    int size = cmd->size();
    const char* path = cmd->path();
    sstable->DistributeFiles(sids, size, path);
    cmd->Complete();
}

int Dispatcher::ExecuteUpdate(DB* db, const char* key, size_t keylen, papyruskv_pos_t* pos, int fnid, void* userin, size_t userinlen, void* userout, size_t useroutlen, int rank) {
    unsigned long cid = Platform::NewCID();
    int tag = Tag(cid);

    Message msg(PAPYRUSKV_MSG_UPDATE);
    msg.WriteULong(db->dbid());
    msg.WriteInt(tag);
    msg.WriteULong(keylen);
    msg.Write(key, keylen);
    msg.WritePtr(pos ? pos->handle : (void*) 0);
    msg.WriteInt(fnid);
    msg.WriteULong(userinlen);
    if (userin && userinlen) msg.Write(userin, userinlen);
    msg.WriteULong(useroutlen);
    msg.Send(rank, mpi_comm_);
    if (userout && useroutlen) {
        MPI_Recv(big_buffer_, (int) useroutlen + sizeof(int), MPI_CHAR, rank, tag, mpi_comm_ext_, MPI_STATUS_IGNORE);
        memcpy(userout, big_buffer_ + sizeof(int), useroutlen);
    } else MPI_Recv(big_buffer_, 1, MPI_INT, rank, tag, mpi_comm_ext_, MPI_STATUS_IGNORE);
    return *((int*) big_buffer_);
}

} /* namespace papyruskv */

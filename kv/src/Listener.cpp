#include "Listener.h"
#include "Platform.h"
#include "Message.h"
#include "Debug.h"
#include <stdlib.h>
#include <unistd.h>

namespace papyruskv {

Listener::Listener(Platform* platform) {
    platform_ = platform;
    mpi_comm_ = platform->mpi_comm();
    mpi_comm_ext_ = platform->mpi_comm_ext();
    rank_ = platform->rank();
    group_ = platform->group();
    pool_ = platform->pool();
    if (posix_memalign((void**) &big_buffer_, 0x1000, PAPYRUSKV_BIG_BUFFER) != 0) _error("size[%lu]", PAPYRUSKV_BIG_BUFFER);
    if (posix_memalign((void**) &ret_buffer_, 0x1000, sizeof(int)) != 0) _error("size[%lu]", sizeof(int));
}

Listener::~Listener() {
    Stop();
    if (big_buffer_) free(big_buffer_);
    if (ret_buffer_) free(ret_buffer_);
}

void Listener::Stop() {
    if (!thread_) return;
    running_ = false;

    Message msg(PAPYRUSKV_MSG_EXIT);
    msg.Ssend(rank_, mpi_comm_);

    pthread_join(thread_, NULL);
    thread_ = (pthread_t) NULL;
}

void Listener::Run() {
    Message msg;
    while (running_) {
        int rank = msg.Recv(MPI_ANY_SOURCE, mpi_comm_);
        int header = msg.ReadHeader();
        _trace("headr[0x%x]", header);
        switch (header) {
            case PAPYRUSKV_MSG_PUT:         ExecutePut(msg, rank);      break;
            case PAPYRUSKV_MSG_GET:         ExecuteGet(msg, rank);      break;
            case PAPYRUSKV_MSG_MIGRATE:     ExecuteMigrate(msg, rank);  break;
            case PAPYRUSKV_MSG_SIGNAL:      ExecuteSignal(msg, rank);   break;
            case PAPYRUSKV_MSG_BARRIER:     ExecuteBarrier(msg, rank);  break;
            case PAPYRUSKV_MSG_UPDATE:      ExecuteUpdate(msg, rank);   break;
            case PAPYRUSKV_MSG_EXIT:        ExecuteExit(msg, rank);     break;
            default: _error("not supported message header[0x%x]", header);
        }
    }
}

void Listener::ExecuteExit(Message& msg, int rank) {
    running_ = false;
}

void Listener::ExecutePut(Message& msg, int rank) {
    unsigned long dbid = msg.ReadULong();
    int tag = msg.ReadInt();
    size_t keylen = msg.ReadULong();
    size_t vallen = msg.ReadULong();
    bool tombstone = msg.ReadBool();
    bool sync = msg.ReadBool();

    Slice *slice = new Slice(keylen, vallen, rank_, tombstone);
    MPI_Recv(slice->buf(), (int) (keylen + vallen), MPI_CHAR, rank, tag, mpi_comm_, MPI_STATUS_IGNORE);
    _trace("rank[%d] dbid[%lu] tag[%d] key[%s] keylen[%lu] val[%s] vallen[%lu] tombstone[%d] sync[%d]", rank, dbid, tag, slice->key(), slice->keylen(), slice->val(), slice->vallen(), slice->tombstone(), sync);

    DB* db = platform_->GetDB(dbid);
    int ret = db->PutLocal(slice);
    if (ret != PAPYRUSKV_OK) _error("ret[%d]", ret);

    if (sync) {
        *ret_buffer_ = ret;
        MPI_Send(ret_buffer_, 1, MPI_INT, rank, tag, mpi_comm_ext_);
    }
}

void Listener::ExecuteGet(Message& msg, int rank) {
    unsigned long dbid = msg.ReadULong();
    int tag = msg.ReadInt();
    size_t keylen = msg.ReadULong();
    char* key = msg.ReadString(keylen);
    char* valp = (char*) msg.ReadPtr();
    int group = msg.ReadInt();

    _trace("dbid[%lu] tag[%d] key[%s] keylen[%lu] group[%d]", dbid, tag, key, keylen, group);

    DB* db = platform_->GetDB(dbid);
    char* val = big_buffer_ + 5 * sizeof(size_t);
    size_t vallen = 0UL;
    int mode = group_ == group ? PAPYRUSKV_MEMTABLE : PAPYRUSKV_MEMTABLE | PAPYRUSKV_SSTABLE;
    papyruskv_pos_t pos;
    int ret = db->GetLocal(key, keylen, &val, &vallen, mode, &pos);
    _trace("ret[%d] key[%s]", ret, key);

    size_t* packet = (size_t*) big_buffer_;
    packet[0] = (size_t) ret;
    packet[1] = (size_t) mode;
    packet[2] = vallen;
    packet[3] = db->sstable()->sid();
    packet[4] = (size_t) pos.handle;

    if (db->keylen()) MPI_Send(big_buffer_, 5 * sizeof(size_t) + vallen, MPI_CHAR, rank, tag, mpi_comm_ext_);
    else {
        MPI_Send(packet, 5, MPI_LONG_LONG, rank, tag, mpi_comm_ext_);
        if (ret == PAPYRUSKV_SLICE_FOUND && valp)
            MPI_Send(val, (int) vallen, MPI_CHAR, rank, tag, mpi_comm_ext_);
    }
}

void Listener::ExecuteMigrate(Message& msg, int rank) {
    unsigned long dbid = msg.ReadULong();
    int tag = msg.ReadInt();
    bool sync = msg.ReadBool();
    int level = msg.ReadInt();
    size_t size = msg.ReadULong();

    DB* db = platform_->GetDB(dbid);
    size_t off = 0UL;

    MPI_Recv(big_buffer_, (int) size, MPI_CHAR, rank, tag, mpi_comm_, MPI_STATUS_IGNORE);
    int ret = PAPYRUSKV_OK;
    for (size_t off = 0UL; off < size; ) {
        size_t keylen = *((size_t*) (big_buffer_ + off));
        off += sizeof(size_t);
        size_t vallen = *((size_t*) (big_buffer_ + off));
        off += sizeof(size_t);
        char* key = big_buffer_ + off;
        off += keylen;
        char* val = big_buffer_ + off;
        off += vallen;
        bool tombstone = *(big_buffer_ + off) == 1;
        off += 1;
        ret = db->PutLocal(key, keylen, val, vallen, tombstone);
    }

    if (level & PAPYRUSKV_SSTABLE) {
        ret = db->Flush(sync);
        if (ret != PAPYRUSKV_OK) _error("ret[%d]", ret);
    }

    *ret_buffer_ = ret;
    MPI_Send(ret_buffer_, 1, MPI_INT, rank, tag, mpi_comm_ext_);
}

void Listener::ExecuteSignal(Message& msg, int rank) {
    int signum = msg.ReadInt();
    Signal* signal = platform_->signal();
    signal->Action(signum, rank);
}

void Listener::ExecuteBarrier(Message& msg, int rank) {
    unsigned long dbid = msg.ReadULong();
    int level = msg.ReadInt();
    _trace("dbid[%lu] level[0x%x]", dbid, level);
    Command* cmd = (Command*) msg.ReadPtr();
    if (level & PAPYRUSKV_SSTABLE) {
        DB* db = platform_->GetDB(dbid);
        int ret = db->Flush(true);
        if (ret != PAPYRUSKV_OK) _error("ret[%d]", ret);
    }
    cmd->Complete();
}

void Listener::ExecuteUpdate(Message& msg, int rank) {
    unsigned long dbid = msg.ReadULong();
    int tag = msg.ReadInt();
    size_t keylen = msg.ReadULong();
    char* key = msg.ReadString(keylen);
    void* pos_handle = msg.ReadPtr();
    int fnid = msg.ReadInt();
    size_t userinlen = msg.ReadULong();
    void* userin = userinlen == 0 ? NULL : msg.Read(userinlen);
    size_t useroutlen = msg.ReadULong();

    _trace("dbid[%lu] tag[%d] key[%s] keylen[%lu] fnid[%x] userinlen[%lu] useroutlen[%lu]", dbid, tag, key, keylen, fnid, userinlen, useroutlen);

    DB* db = platform_->GetDB(dbid);
    papyruskv_pos_t pos = { pos_handle };
    int ret = db->UpdateLocal(key, keylen, &pos, fnid, userin, userinlen, big_buffer_ + sizeof(int), useroutlen);
    if (ret != PAPYRUSKV_OK) _error("ret[%d] key[%s] keylen[%lu] fnid[%x]", ret, key, keylen, fnid);
    ((int*) big_buffer_)[0] = ret;
    if (useroutlen) MPI_Send(big_buffer_, (int) useroutlen + sizeof(int), MPI_CHAR, rank, tag, mpi_comm_ext_);
    else MPI_Send(big_buffer_, 1, MPI_INT, rank, tag, mpi_comm_ext_);
}

} /* namespace papyruskv */

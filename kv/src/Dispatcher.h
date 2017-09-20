#ifndef PAPYRUS_KV_SRC_DISPATCHER_H
#define PAPYRUS_KV_SRC_DISPATCHER_H

#include <papyrus/kv.h>
#include "Command.h"
#include "Message.h"
#include "Queue.h"
#include "Pool.h"
#include "Thread.h"

namespace papyruskv {

class Platform;

class Dispatcher : public Thread {
public:
    Dispatcher(Platform* platform);
    virtual ~Dispatcher();

    void Enqueue(Command* cmd);
    void EnqueueWait(Command* cmd);
    void EnqueueWaitRelease(Command* cmd);

    int ExecutePut(DB* db, const char* key, size_t keylen, const char* val, size_t vallen, bool tombstone, bool sync, int rank);
    int ExecuteGet(DB *db, const char* key, size_t keylen, char** valp, size_t* vallenp, int group, int rank, papyruskv_pos_t* pos);
    int ExecuteUpdate(DB *db, const char* key, size_t keylen, papyruskv_pos_t* pos, int fnid, void* userin, size_t userinlen, void* userout, size_t useroutlen, int rank);
    int ExecuteMigrate(RemoteBuffer* rb, bool sync, int level, int rank);
    int ExecuteSignal(int signum, int* ranks, int count);

private:
    void Execute(Command* cmd);
    void ExecuteBarrier(Command* cmd);
    void ExecuteMigrate(Command* cmd);
    void ExecuteMigrateRemoteBuffer(Command* cmd);
    void ExecuteMigrateMemTable(Command* cmd);
    void ExecuteCheckpoint(Command* cmd);
    void ExecuteRestart(Command* cmd);
    void ExecuteDistribute(Command* cmd);

    int Tag(unsigned long cid) { return cid % PAPYRUSKV_MPI_TAG_LIMIT; }

private:
    virtual void Run();

private:
    LockFreeQueue<Command*>* queue_;
    Platform* platform_;
    Pool* pool_;
    MPI_Comm mpi_comm_;
    MPI_Comm mpi_comm_ext_;

    int rank_;
    int nranks_;

    char* big_buffer_;
    int* ret_buffer_;
};

} /* namespace papyruskv */

#endif /* PAPYRUS_KV_SRC_DISPATCHER_H */

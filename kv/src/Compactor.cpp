#include "Compactor.h"
#include "Debug.h"
#include "Message.h"
#include "Platform.h"
#include <string.h>

namespace papyruskv {

Compactor::Compactor(Platform* platform) {
    platform_ = platform;
    rank_ = platform->rank();
    queue_ = new LockFreeQueueMS<Command*>(4);
}

Compactor::~Compactor() {
}

void Compactor::Enqueue(Command* cmd) {
    while (!queue_->Enqueue(cmd)) {}
    Invoke();
}

void Compactor::EnqueueWait(Command* cmd) {
    Enqueue(cmd);
    cmd->Wait();
}

void Compactor::EnqueueWaitRelease(Command* cmd) {
    EnqueueWait(cmd);
    Command::Release(cmd);
}

void Compactor::Run() {
    while (true) {
        sem_wait(&sem_);
        if (!running_) break;
        Command* cmd = NULL;
        while (queue_->Dequeue(&cmd)) Execute(cmd);
    }
}

void Compactor::Execute(Command* cmd) {
    _trace("cmd[%lu] type[%x]", cmd->cid(), cmd->type());
    switch (cmd->type()) {
        case PAPYRUSKV_CMD_FLUSH:       ExecuteFlush(cmd);      break;
        case PAPYRUSKV_CMD_LOAD:        ExecuteLoad(cmd);       break;
        default: _error("not supported command type[0x%x]", cmd->type());
    }
}

void Compactor::ExecuteFlush(Command* cmd) {
    MemTable* mt = cmd->mt();
    DB* db = mt->db();

    if (mt->Empty()) {
        cmd->Complete();
        return;
    }

    SSTable* sstable = db->sstable();
    uint64_t old_sid = sstable->sid();
    uint64_t new_sid = sstable->Flush(mt);

    _trace("old_sid[%llu] new_sid[%llu]", old_sid, new_sid);

    db->RemoveLocalIMT(mt);
    MemTable::Release(mt);

    cmd->Complete();
    if (!cmd->sync()) Command::Release(cmd);
}

void Compactor::ExecuteLoad(Command* cmd) {
    MemTable* mt = cmd->mt();
    uint64_t sid = cmd->sid();

    SSTable* sstable = mt->db()->sstable();
    sstable->Load(mt, sid);

    cmd->Complete();
}

} /* namespace papyruskv */

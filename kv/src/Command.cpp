#include "Command.h"
#include "Platform.h"
#include "Debug.h"
#include <string.h>

namespace papyruskv {

Command::Command(int type) {
    cid_ = Platform::NewCID();
    type_ = type;
    status_ = PAPYRUSKV_NONE;

    pthread_mutex_init(&mutex_complete_, NULL);
    pthread_cond_init(&cond_complete_, NULL);
}

Command::~Command() {
    pthread_mutex_destroy(&mutex_complete_);
    pthread_cond_destroy(&cond_complete_);
}

void Command::SetStatus(int status) {
    if (status == PAPYRUSKV_COMPLETE) Complete();
    else status_ = status;
}

void Command::Complete() {
    pthread_mutex_lock(&mutex_complete_);
    status_ = PAPYRUSKV_COMPLETE;
    pthread_cond_broadcast(&cond_complete_);
    pthread_mutex_unlock(&mutex_complete_);
}

void Command::Complete(int ret) {
    ret_ = ret;
    Complete();
}

void Command::Wait() {
    pthread_mutex_lock(&mutex_complete_);
    if (status_ != PAPYRUSKV_COMPLETE) {
        pthread_cond_wait(&cond_complete_, &mutex_complete_);
    }
    pthread_mutex_unlock(&mutex_complete_);
}

Command* Command::Create(int type) {
    return new Command(type);
}

Command* Command::CreatePut(DB* db, const char* key, size_t keylen, const char* val, size_t vallen, bool tombstone, int rank) {
    Command* cmd = Create(PAPYRUSKV_CMD_PUT);
    cmd->db_ = db;
    cmd->dbid_ = db->dbid();
    cmd->key_ = key;
    cmd->keylen_ = keylen;
    cmd->val_ = val;
    cmd->vallen_ = vallen;
    cmd->tombstone_ = tombstone;
    cmd->rank_ = rank;
    return cmd;
}


Command* Command::CreateGet(DB* db, const char* key, size_t keylen, char** valp, size_t* vallenp, int group, int rank) {
    Command* cmd = Create(PAPYRUSKV_CMD_GET);
    cmd->db_ = db;
    cmd->dbid_ = db->dbid();
    cmd->key_ = key;
    cmd->keylen_ = keylen;
    cmd->valp_ = valp;
    cmd->vallenp_ = vallenp;
    cmd->group_ = group;
    cmd->rank_ = rank;
    return cmd;
}

Command* Command::CreateMigrate(MemTable* mt, bool sync, int level) {
    Command* cmd = Create(PAPYRUSKV_CMD_MIGRATE);
    cmd->db_ = mt->db();
    cmd->dbid_ = mt->db()->dbid();
    cmd->mt_ = mt;
    cmd->sync_ = sync;
    cmd->level_ = level;
    return cmd;
}

Command* Command::CreateMigrate(DB* db, char* block, size_t size, int rank, bool sync, int level) {
    Command* cmd = Create(PAPYRUSKV_CMD_MIGRATE);
    cmd->db_ = db;
    cmd->dbid_ = db->dbid();
    cmd->block_ = block;
    cmd->size_ = size;
    cmd->rank_ = rank;
    cmd->sync_ = sync;
    cmd->level_ = level;
    return cmd;
}

Command* Command::CreateFlush(MemTable* mt, bool sync) {
    Command* cmd = Create(PAPYRUSKV_CMD_FLUSH);
    cmd->mt_ = mt;
    cmd->sync_ = sync;
    return cmd;
}

Command* Command::CreateSignal(int signum, int* ranks, int count) {
    Command* cmd = Create(PAPYRUSKV_CMD_SIGNAL);
    cmd->signum_ = signum;
    cmd->ranks_ = ranks;
    cmd->count_ = count;
    return cmd;
}

Command* Command::CreateBarrier(DB* db, int level) {
    Command* cmd = Create(PAPYRUSKV_CMD_BARRIER);
    cmd->db_ = db;
    cmd->dbid_ = db->dbid();
    cmd->level_ = level;
    return cmd;
}

Command* Command::CreateCheckpoint(SSTable* sstable, uint64_t sid, const char* path) {
    Command* cmd = Create(PAPYRUSKV_CMD_CHECKPOINT);
    cmd->sstable_ = sstable;
    cmd->sid_ = sid;
    cmd->path_ = path;
    return cmd;
}

Command* Command::CreateRestart(SSTable* sstable, uint64_t sid, const char* path) {
    Command* cmd = Create(PAPYRUSKV_CMD_RESTART);
    cmd->sstable_ = sstable;
    cmd->sid_ = sid;
    cmd->path_ = path;
    return cmd;
}

Command* Command::CreateDistribute(SSTable* sstable, uint64_t* sids, int size, const char* path) {
    Command* cmd = Create(PAPYRUSKV_CMD_DISTRIBUTE);
    cmd->sstable_ = sstable;
    cmd->sids_ = sids;
    cmd->size_ = size;
    cmd->path_ = path;
    return cmd;
}

Command* Command::CreateLoad(MemTable* mt, uint64_t sid) {
    Command* cmd = Create(PAPYRUSKV_CMD_LOAD);
    cmd->mt_ = mt;
    cmd->sid_ = sid;
    return cmd;
}

Command* Command::CreateUpdate(DB* db, const char* key, size_t keylen, int fnid, void* userin, size_t userinlen, void* userout, size_t useroutlen, int rank) {
    Command* cmd = Create(PAPYRUSKV_CMD_UPDATE);
    cmd->db_ = db;
    cmd->dbid_ = db->dbid();
    cmd->key_ = key;
    cmd->keylen_ = keylen;
    cmd->fnid_ = fnid;
    cmd->userin_ = userin;
    cmd->userinlen_ = userinlen;
    cmd->userout_ = userout;
    cmd->useroutlen_ = useroutlen;
    cmd->rank_ = rank;
    return cmd;
}

void Command::Release(Command *cmd) {
    delete cmd;
}

} /* namespace papyruskv */

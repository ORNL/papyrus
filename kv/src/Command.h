#ifndef PAPYRUS_KV_SRC_COMMAND_H
#define PAPYRUS_KV_SRC_COMMAND_H

#define PAPYRUSKV_CMD_NOP               0x1100
#define PAPYRUSKV_CMD_PUT               0x1101
#define PAPYRUSKV_CMD_GET               0x1102
#define PAPYRUSKV_CMD_DELETE            0x1103
#define PAPYRUSKV_CMD_MIGRATE           0x1104
#define PAPYRUSKV_CMD_FLUSH             0x1105
#define PAPYRUSKV_CMD_SIGNAL            0x1106
#define PAPYRUSKV_CMD_BARRIER           0x1107
#define PAPYRUSKV_CMD_CHECKPOINT        0x1108
#define PAPYRUSKV_CMD_RESTART           0x1109
#define PAPYRUSKV_CMD_DISTRIBUTE        0x110a
#define PAPYRUSKV_CMD_LOAD              0x110b
#define PAPYRUSKV_CMD_UPDATE            0x110c
#define PAPYRUSKV_CMD_EXIT              0x11ff

#define PAPYRUSKV_COMPLETE              0x0
#define PAPYRUSKV_RUNNING               0x1
#define PAPYRUSKV_SUBMITTED             0x2
#define PAPYRUSKV_QUEUED                0x3
#define PAPYRUSKV_NONE                  0x4

#define PAPYRUSKV_MPI_TAG_LIMIT         0x200000

#include <stdint.h>
#include <pthread.h>
#include <sys/types.h>

namespace papyruskv {

class DB;
class MemTable;
class RemoteBuffer;
class SSTable;
class Slice;

class Command {
public:
    Command(int type);
    ~Command();
    void SetStatus(int status);
    void Complete();
    void Complete(int ret);
    void Wait();

    unsigned long cid() const { return cid_; }
    int type() const { return type_; }
    int ret() const { return ret_; }
    int rank() const { return rank_; }
    int group() const { return group_; }
    bool sync() const { return sync_; }
    int level() const { return level_; }
    int tag() const { return cid_ % PAPYRUSKV_MPI_TAG_LIMIT; }

    DB* db() const { return db_; }
    int dbid() const { return dbid_; }
    const char* key() const { return key_; }
    size_t keylen() const { return keylen_; }
    const char* val() const { return val_; }
    size_t vallen() const { return vallen_; }
    char** valp() const { return valp_; }
    size_t* vallenp() const { return vallenp_; }
    bool tombstone() const { return tombstone_; }

    int signum() const { return signum_; }
    int* ranks() const { return ranks_; }
    int count() const { return count_; }

    Slice* slice() const { return slice_; }
    MemTable* mt() const { return mt_; }
    RemoteBuffer* rb() const { return rb_; }
    char* block() const { return block_; }

    SSTable* sstable() const { return sstable_; }
    uint64_t* sids() const { return sids_; }
    uint64_t sid() const { return sid_; }
    int size() const { return size_; }
    const char* path() const { return path_; }

    int fnid() const { return fnid_; }
    void* userin() { return userin_; }
    size_t userinlen() const { return userinlen_; }
    void* userout() { return userout_; }
    size_t useroutlen() const { return useroutlen_; }

private:
    unsigned long cid_;
    int type_;
    int status_;
    int ret_;
    int rank_;
    int group_;
    bool sync_;
    int level_;

    DB* db_;
    int dbid_;
    const char* key_;
    size_t keylen_;
    const char* val_;
    size_t vallen_;
    char** valp_;
    size_t* vallenp_;
    bool tombstone_;

    int signum_;
    int* ranks_;
    int count_;

    Slice* slice_;

    MemTable* mt_;
    RemoteBuffer* rb_;
    char* block_;

    SSTable* sstable_;
    uint64_t sid_;
    const char* path_;
    uint64_t* sids_;
    int size_;

    int fnid_;
    void* userin_;
    size_t userinlen_;
    void* userout_;
    size_t useroutlen_;

    pthread_mutex_t mutex_complete_;
    pthread_cond_t cond_complete_;

public:
    static Command* Create(int type);
    static Command* CreatePut(DB* db, const char* key, size_t keylen, const char* val, size_t vallen, bool tombstone, int rank);
    static Command* CreateGet(DB* db, const char* key, size_t keylen, char** valp, size_t* vallenp, int group, int rank);
    static Command* CreateMigrate(MemTable* mt, bool sync, int level);
    static Command* CreateMigrate(DB* db, char* block, size_t size, int rank, bool sync, int level);
    static Command* CreateFlush(MemTable* mt, bool sync);
    static Command* CreateSignal(int signum, int* ranks, int count);
    static Command* CreateBarrier(DB* db, int level);
    static Command* CreateCheckpoint(SSTable* sstable, uint64_t sid, const char* path);
    static Command* CreateRestart(SSTable* sstable, uint64_t sid, const char* path);
    static Command* CreateDistribute(SSTable* sstable, uint64_t* sids, int size, const char* path);
    static Command* CreateLoad(MemTable* mt, uint64_t sid);
    static Command* CreateUpdate(DB* db, const char* key, size_t keylen, int fnid, void* userin, size_t userinlen, void* userout, size_t useroutlen, int rank);
    static void Release(Command *cmd);
};

} /* namespace papyruskv */

#endif /* PAPYRUS_KV_SRC_COMMAND_H */

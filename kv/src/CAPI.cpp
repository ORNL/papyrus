#include <papyrus/kv.h>
#include "Debug.h"
#include "Platform.h"

using namespace papyruskv;

int papyruskv_init(int* argc, char*** argv, const char* repository) {
    return Platform::GetPlatform()->Init(argc, argv, repository);
}

int papyruskv_finalize() {
    return Platform::Finalize();
}

int papyruskv_open(const char* name, int flags, papyruskv_option_t* opt, int* db) {
    return Platform::GetPlatform()->Open(name, flags, opt, db);
}

int papyruskv_close(int db) {
    return Platform::GetPlatform()->Close(db);
}

int papyruskv_put(int db, const char* key, size_t keylen, const char* val, size_t vallen) {
    return Platform::GetPlatform()->Put(db, key, keylen, val, vallen);
}

int papyruskv_get(int db, const char* key, size_t keylen, char** val, size_t* vallen) {
    return papyruskv_get_pos(db, key, keylen, val, vallen, NULL);
}

int papyruskv_get_pos(int db, const char* key, size_t keylen, char** val, size_t* vallen, papyruskv_pos_t* pos) {
    return Platform::GetPlatform()->Get(db, key, keylen, val, vallen, pos);
}

int papyruskv_delete(int db, const char* key, size_t keylen) {
    return Platform::GetPlatform()->Delete(db, key, keylen);
}

int papyruskv_free(char** val) {
    return Platform::GetPlatform()->Free(val);
}

int papyruskv_fence(int db, int level) {
    return Platform::GetPlatform()->Fence(db, level);
}

int papyruskv_barrier(int db, int level) {
    return Platform::GetPlatform()->Barrier(db, level);
}

int papyruskv_signal_notify(int signum, int* ranks, int count) {
    return Platform::GetPlatform()->SignalNotify(signum, ranks, count);
}

int papyruskv_signal_wait(int signum, int* ranks, int count) {
    return Platform::GetPlatform()->SignalWait(signum, ranks, count);
}

int papyruskv_consistency(int db, int consistency) {
    return Platform::GetPlatform()->SetConsistency(db, consistency);
}

int papyruskv_protect(int db, int prot) {
    return Platform::GetPlatform()->SetProtection(db, prot);
}

int papyruskv_destroy(int db, int* event) {
    return Platform::GetPlatform()->Destroy(db, event);
}

int papyruskv_checkpoint(int db, const char* path, int* event) {
    return Platform::GetPlatform()->Checkpoint(db, path, event);
}

int papyruskv_restart(const char* path, const char* name, int flags, papyruskv_option_t* opt, int* db, int* event) {
    return Platform::GetPlatform()->Restart(path, name, flags, opt, db, event);
}

int papyruskv_wait(int db, int event) {
    return Platform::GetPlatform()->Wait(db, event);
}

int papyruskv_hash(int db, papyruskv_hash_fn_t hfn) {
    return Platform::GetPlatform()->Hash(db);
}

int papyruskv_iter_local(int db, papyruskv_iter_t* iter) {
    return Platform::GetPlatform()->IterLocal(db, iter);
}

int papyruskv_iter_next(int db, papyruskv_iter_t* iter) {
    return Platform::GetPlatform()->IterNext(db, iter);
}

int papyruskv_register_update(int db, int fnid, papyruskv_update_fn_t ufn) {
    return Platform::GetPlatform()->RegisterUpdate(db, fnid, ufn);
}

int papyruskv_update(int db, const char* key, size_t keylen, papyruskv_pos_t* pos, int fnid, void* userin, size_t userinlen, void* userout, size_t useroutlen) {
    return Platform::GetPlatform()->Update(db, key, keylen, pos, fnid, userin, userinlen, userout, useroutlen);
}


#ifndef PAPYRUS_KV_INC_PAPYRUS_KV_H
#define PAPYRUS_KV_INC_PAPYRUS_KV_H

#define PAPYRUSKV_OK                        0
#define PAPYRUSKV_ERR                       -1

#define PAPYRUSKV_MEMTABLE                  (1 << 0)
#define PAPYRUSKV_SSTABLE                   (1 << 1)

#define PAPYRUSKV_SEQUENTIAL                (1 << 0)
#define PAPYRUSKV_RELAXED                   (1 << 1)

#define PAPYRUSKV_CREATE                    (1 << 2)
#define PAPYRUSKV_RDWR                      (1 << 3)
#define PAPYRUSKV_WRONLY                    (1 << 4)
#define PAPYRUSKV_RDONLY                    (1 << 5)
#define PAPYRUSKV_UDONLY                    (1 << 6)

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

struct _papyruskv_iter_t {
    char* key;
    size_t keylen;
    char* val;
    size_t vallen;
    void* handle;
};
typedef struct _papyruskv_iter_t* papyruskv_iter_t;

struct _papyruskv_pos_t {
    void* handle;
};
typedef struct _papyruskv_pos_t papyruskv_pos_t;

typedef int (*papyruskv_hash_fn_t)(const char* key, size_t keylen, size_t nranks);
typedef int (*papyruskv_update_fn_t)(const char* key, size_t keylen, char** val, size_t* vallen, void* userin, size_t userinlen, void* userout, size_t useroutlen);

typedef struct {
    size_t keylen;
    size_t vallen;
    papyruskv_hash_fn_t hash;
} papyruskv_option_t;

extern int papyruskv_init(int* argc, char*** argv, const char* repository);
extern int papyruskv_finalize();
extern int papyruskv_open(const char* name, int flags, papyruskv_option_t* opt, int* db);
extern int papyruskv_close(int db);
extern int papyruskv_put(int db, const char* key, size_t keylen, const char* val, size_t vallen);
extern int papyruskv_get(int db, const char* key, size_t keylen, char** val, size_t* vallen);
extern int papyruskv_get_pos(int db, const char* key, size_t keylen, char** val, size_t* vallen, papyruskv_pos_t* pos);
extern int papyruskv_delete(int db, const char* key, size_t keylen);
extern int papyruskv_free(void* val);
extern int papyruskv_fence(int db, int level);
extern int papyruskv_barrier(int db, int level);
extern int papyruskv_signal_notify(int signum, int* ranks, int count);
extern int papyruskv_signal_wait(int signum, int* ranks, int count);
extern int papyruskv_consistency(int db, int consistency);
extern int papyruskv_protect(int db, int prot);
extern int papyruskv_destroy(int db, int* event);
extern int papyruskv_checkpoint(int db, const char* path, int* event);
extern int papyruskv_restart(const char* path, const char* name, int flags, papyruskv_option_t* opt, int* db, int* event);
extern int papyruskv_wait(int db, int event);

extern int papyruskv_hash(int db, papyruskv_hash_fn_t hfn);
extern int papyruskv_iter_local(int db, papyruskv_iter_t* iter);
extern int papyruskv_iter_next(int db, papyruskv_iter_t* iter);
extern int papyruskv_register_update(int db, int fnid, papyruskv_update_fn_t ufn);
extern int papyruskv_update(int db, const char* key, size_t keylen, papyruskv_pos_t* pos, int fnid, void* userin, size_t userinlen, void* userout, size_t useroutlen);

#ifdef __cplusplus
} /* end extern "C" */
#endif

#endif /* PAPYRUS_KV_INC_PAPYRUS_KV_H */

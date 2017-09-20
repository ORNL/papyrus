#ifndef PAPYRUS_KV_SRC_DEFINE_H
#define PAPYRUS_KV_SRC_DEFINE_H

#define PAPYRUSKV_MAX_DB                    256
#define PAPYRUSKV_MAX_DB_NAME               256

#define PAPYRUSKV_GROUP_SIZE                1

#define PAPYRUSKV_MEMTABLE_SIZE             (1UL   * 1024 * 1024 * 1024)
#define PAPYRUSKV_REMOTE_BUFFER_SIZE        (128UL * 1024)
#define PAPYRUSKV_REMOTE_BUFFER_ENTRY_MAX   (4UL   * 1024)
#define PAPYRUSKV_CACHE_SIZE                (128UL * 1024 * 1024)
#define PAPYRUSKV_MAX_KEYLEN                (16UL  * 1024)
#define PAPYRUSKV_MAX_VALLEN                (16UL  * 1024 * 1024)
#define PAPYRUSKV_BIG_BUFFER                (32UL  * 1024 * 1024)

#define PAPYRUSKV_SLICE_FOUND               0x1
#define PAPYRUSKV_SLICE_TOMBSTONE           0x2
#define PAPYRUSKV_SLICE_NOT_FOUND           0x3

#define PAPYRUSKV_SSTABLE_SEQ               0x1
#define PAPYRUSKV_SSTABLE_BIN               0x2

#define PAPYRUSKV_REMOTE_BUFFER             false
#define PAPYRUSKV_CACHE_LOCAL               false
#define PAPYRUSKV_CACHE_REMOTE              false

#define PAPYRUSKV_BLOOM                     true
#define PAPYRUSKV_BLOOM_BITS                (64 * 1024 * 4)

#define PAPYRUSKV_DESTROY_REPOSITORY        true
#define PAPYRUSKV_FORCE_REDISTRIBUTE        false

#endif /* PAPYRUS_KV_SRC_DEFINE_H */

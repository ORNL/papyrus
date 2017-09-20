#ifndef PAPYRUS_KV_SRC_HASHER_H
#define PAPYRUS_KV_SRC_HASHER_H

#include <papyrus/kv.h>
#include <stddef.h>
#include <stdint.h>

namespace papyruskv {

class Hasher {
public:
    Hasher(int nranks);
    ~Hasher();

    int KeyRank(const char* key, size_t keylen);
    int KeyBucket(const char* key, size_t keylen, size_t bucket_size);
    unsigned long KeyHash(const char* key, size_t keylen);
    size_t BucketSize(size_t size);

    uint64_t djb2(const char* key, size_t len);
    uint64_t MurmurHash2(const char* key, size_t len, uint64_t seed = 0x1f0d3804);

    papyruskv_hash_fn_t hash() const { return hash_; }
    void set_hash(papyruskv_hash_fn_t hash) { hash_ = hash; }

private:
    int nranks_;
    papyruskv_hash_fn_t hash_;
};

} /* namespace papyruskv */

#endif /* PAPYRUS_KV_SRC_HASHER_H */

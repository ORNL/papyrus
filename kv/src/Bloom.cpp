#include <papyrus/kv.h>
#include "Bloom.h"
#include "Debug.h"

namespace papyruskv {

Bloom::Bloom(Hasher* hasher, size_t bitlen) {
    hasher_ = hasher;
    bitlen_ = bitlen;
    len_ = bitlen / 64;
}

Bloom::~Bloom() {

}

uint64_t* Bloom::Bits(Slice* slices) {
    uint64_t* bits = new uint64_t[len_];
    for (size_t i = 0; i < len_; i++) bits[i] = 0ULL;

    for (Slice* slice = slices; slice; slice = slice->next()) {
        uint64_t hash1 = hasher_->djb2(slice->key(), slice->keylen());
        uint64_t hash2 = hasher_->MurmurHash2(slice->key(), slice->keylen());
        uint64_t sha1 = hash1 % bitlen_;
        uint64_t sha2 = hash2 % bitlen_;
        uint64_t idx1 = sha1 / 64;
        uint64_t idx2 = sha2 / 64;
        uint64_t off1 = sha1 % 64;
        uint64_t off2 = sha2 % 64;
        uint64_t bit1 = 1ULL << off1;
        uint64_t bit2 = 1ULL << off2;
        bits[idx1] |= bit1;
        bits[idx2] |= bit2;
        _trace("hash1[0x%lx] hash2[0x%lx] sha1[%lu] sha2[%lu] idx1[%lu] idx2[%lu] off1[%lu] off2[%lu] bits1[%lx] bits2[%lx]", hash1, hash2, sha1, sha2, idx1, idx2, off1, off2, bits[idx1], bits[idx2]);
    }
    return bits;
}

bool Bloom::Maybe(const char* key, size_t keylen, uint64_t* bits, size_t bitslen) {
    uint64_t hash1 = hasher_->djb2(key, keylen);
    uint64_t hash2 = hasher_->MurmurHash2(key, keylen);
    uint64_t sha1 = hash1 % bitlen_;
    uint64_t sha2 = hash2 % bitlen_;
    uint64_t idx1 = sha1 / 64;
    uint64_t idx2 = sha2 / 64;
    uint64_t off1 = sha1 % 64;
    uint64_t off2 = sha2 % 64;
    uint64_t bit1 = 1ULL << off1;
    uint64_t bit2 = 1ULL << off2;
    uint64_t bits1 = bits[idx1];
    uint64_t bits2 = bits[idx2];
    _trace("key[%s] hash1[0x%lx] hash2[0x%lx] sha1[%lu] sha2[%lu] idx1[%lu] idx2[%lu] off1[%lu] off2[%lu] bits1[%lx] bits2[%lx] bit1[%lx] bit2[%lx]", key, hash1, hash2, sha1, sha2, idx1, idx2, off1, off2, bits1, bits2, bit1, bit2);
    if (idx1 >= bitslen || idx2 >= bitslen) {
        _error("bitslen[%lu] idx1[%lu] idx2[%lu]", bitslen, idx1, idx2);
        return false;
    }
    return (bits1 & bit1) == bit1 && (bits2 & bit2) == bit2;
}

} /* namespace papyruskv */

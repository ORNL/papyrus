#ifndef PAPYRUS_KV_SRC_BLOOM_H
#define PAPYRUS_KV_SRC_BLOOM_H

#include "Hasher.h"
#include "Slice.h"

namespace papyruskv {

class Bloom {
public:
    Bloom(Hasher* hasher, size_t bitlen);
    ~Bloom();

    size_t len() const { return len_; }

    uint64_t* Bits(Slice* slices);
    bool Maybe(const char* key, size_t keylen, uint64_t* bits, size_t bitslen);

private:
    Hasher* hasher_;
    size_t bitlen_;
    size_t len_;
};

} /* namespace papyruskv */

#endif /*PAPYRUS_KV_SRC_BLOOM_H */

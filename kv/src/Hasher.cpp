#include "Hasher.h"
#include "Utils.h"
#include "Debug.h"

namespace papyruskv {

Hasher::Hasher(int nranks) {
    nranks_ = nranks;
    hash_ = NULL;
}

Hasher::~Hasher() {
}

int Hasher::KeyRank(const char* key, size_t keylen) {
    return hash_ ? (hash_)(key, keylen, nranks_) : KeyHash(key, keylen) % nranks_;
}

int Hasher::KeyBucket(const char* key, size_t keylen, size_t bucket_size) {
    return KeyHash(key, keylen) & (bucket_size - 1);
    //return KeyHash(key, keylen) % bucket_size;
}

size_t Hasher::BucketSize(size_t size) {
    return Utils::P2(size) << 2;
    //return size * 3;
}

unsigned long Hasher::KeyHash(const char* key, size_t keylen) {
    return djb2(key, keylen);
}

uint64_t Hasher::djb2(const char* key, size_t len) {
    uint64_t hash = 5381;
    for (size_t i = 0UL; i < len; i++) hash = ((hash << 5) + hash) + key[i];
    return hash;
}

uint64_t Hasher::MurmurHash2(const char* key, size_t len, uint64_t seed) {
  const uint64_t m = 0xc6a4a7935bd1e995ULL;
  const int r = 47;

  uint64_t h = seed ^ (len * m);

  const uint64_t* data = (const uint64_t*) key;
  const uint64_t* end = data + (len / 8);

  while (data != end) {
    uint64_t k = *data++;
    k *= m; 
    k ^= k >> r; 
    k *= m; 
    h ^= k;
    h *= m; 
  }

  const unsigned char* data2 = (const unsigned char*) data;

  switch (len & 7) {
  case 7: h ^= uint64_t(data2[6]) << 48;
  case 6: h ^= uint64_t(data2[5]) << 40;
  case 5: h ^= uint64_t(data2[4]) << 32;
  case 4: h ^= uint64_t(data2[3]) << 24;
  case 3: h ^= uint64_t(data2[2]) << 16;
  case 2: h ^= uint64_t(data2[1]) << 8;
  case 1: h ^= uint64_t(data2[0]);
          h *= m;
  };
 
  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}

} /* namespace papyruskv */

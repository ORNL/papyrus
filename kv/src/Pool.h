#ifndef PAPYRUS_KV_SRC_POOL_H
#define PAPYRUS_KV_SRC_POOL_H

#include <stddef.h>

namespace papyruskv {

class Platform;

class Pool {
public:
    Pool(Platform* platform);
    ~Pool();

    char* AllocVal(size_t vallen);
    void FreeVal(void* val);

private:
    Platform* platform_;
};

} /* namespace papyruskv */

#endif /* PAPYRUS_KV_SRC_POOL_H */

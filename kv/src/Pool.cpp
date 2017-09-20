#include "Pool.h"
#include "Debug.h"

namespace papyruskv {

Pool::Pool(Platform* platform) {
    platform_ = platform;
}

Pool::~Pool() {
}

char* Pool::AllocVal(size_t vallen) {
    return new char[vallen];
}

void Pool::FreeVal(void* val) {
    delete[] reinterpret_cast<char *>(val);
}

} /* namespace papyruskv */

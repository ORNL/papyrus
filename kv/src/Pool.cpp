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

void Pool::FreeVal(char** val) {
    delete[] *val;
    *val = NULL;
}

} /* namespace papyruskv */

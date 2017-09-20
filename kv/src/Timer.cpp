#include "Timer.h"
#include <sys/time.h>
#include <stdlib.h>

#define PAPYRUSKV_TIMER

namespace papyruskv {

Timer* Timer::singleton_ = NULL;

Timer::Timer() {
    base_sec_ = 0;
    for (int i = 0; i < PAPYRUSKV_TIMER_MAX; i++) {
        total_[i] = 0.0;
    }
}

Timer::~Timer() {
}

double Timer::Now() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + 1.e-6 * tv.tv_usec - base_sec_;
}

void Timer::Start(int i) {
#ifdef PAPYRUSKV_TIMER
    start_[i] = Now();
#endif
}

void Timer::Stop(int i) {
#ifdef PAPYRUSKV_TIMER
    total_[i] += Now() - start_[i];
#endif
}

double Timer::Total(int i) {
    return total_[i];
}

Timer* Timer::GetTimer() {
    if (singleton_ == NULL) singleton_ = new Timer();
    return singleton_;
}

} /* namespace papyruskv */

#ifndef PAPYRUS_KV_SRC_SIGNAL_H
#define PAPYRUS_KV_SRC_SIGNAL_H

#include <pthread.h>
#include <semaphore.h>
#include <unordered_map>
#include "Dispatcher.h"

namespace papyruskv {

class Platform;

class Signal {
public:
    Signal(Platform* platform);
    ~Signal();

    int Notify(int signum, int* ranks, int count);
    int Wait(int signum, int* ranks, int count);
    int Action(int signum, int rank);

private:
    sem_t* Semaphore(int signum, int rank);

private:
    pthread_mutex_t mutex_;
    Dispatcher* dispatcher_;

    std::unordered_map<unsigned long, sem_t*> sems_;
};

} /* namespace papyruskv */
#endif /* PAPYRUS_KV_SRC_SIGNAL_H */

#include <papyrus/kv.h>
#include "Signal.h"
#include "Debug.h"
#include "Platform.h"
#include "Command.h"

namespace papyruskv {

Signal::Signal(Platform* platform) {
    dispatcher_ = platform->dispatcher();
    pthread_mutex_init(&mutex_, NULL);
}

Signal::~Signal() {
    pthread_mutex_destroy(&mutex_);
    for (auto it = sems_.begin(); it != sems_.end(); ++it) {
        sem_destroy(it->second);
        delete it->second;
    }
}

int Signal::Notify(int signum, int* ranks, int count) {
    return dispatcher_->ExecuteSignal(signum, ranks, count);
}

int Signal::Wait(int signum, int* ranks, int count) {
    for (int i = 0; i < count; i++) {
        sem_t* sem = Semaphore(signum, ranks[i]);
        int ret = sem_wait(sem);
        if (ret != 0) {
            _error("ret[%d]", ret);
            return PAPYRUSKV_ERR;
        }
    }
    return PAPYRUSKV_OK;
}

int Signal::Action(int signum, int rank) {
    sem_t* sem = Semaphore(signum, rank);
    int ret = sem_post(sem);
    if (ret != 0) {
        _error("ret[%d]", ret);
        return PAPYRUSKV_ERR;
    }
    return PAPYRUSKV_OK;
}

sem_t* Signal::Semaphore(int signum, int rank) {
    sem_t* sem = NULL;
    unsigned long semid = (unsigned long) signum << 32 | rank;
    pthread_mutex_lock(&mutex_);
    if (sems_.count(semid) == 0) {
        sem = new sem_t;
        sem_init(sem, 0, 0);
        sems_[semid] = sem;
    } else sem = sems_[semid];
    pthread_mutex_unlock(&mutex_);
    return sem;
}

} /* namespace papyruskv */

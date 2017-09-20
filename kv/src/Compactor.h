#ifndef PAPYRUS_KV_SRC_COMPACTOR_H
#define PAPYRUS_KV_SRC_COMPACTOR_H

#include "Queue.h"
#include "Thread.h"
#include "Command.h"

namespace papyruskv {

class Platform;

class Compactor : public Thread {
public:
    Compactor(Platform* platform);
    virtual ~Compactor();

    void Enqueue(Command* cmd);
    void EnqueueWait(Command* cmd);
    void EnqueueWaitRelease(Command* cmd);

private:
    void Execute(Command* cmd);
    void ExecuteFlush(Command* cmd);
    void ExecuteLoad(Command* cmd);

private:
    virtual void Run();

private:
    LockFreeQueue<Command*>* queue_;
    Platform* platform_;

    int rank_;
};

} /* namespace papyruskv */

#endif /* PAPYRUS_KV_SRC_COMPACTOR_H */

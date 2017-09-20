#ifndef PAPYRUS_KV_SRC_LISTENER_H
#define PAPYRUS_KV_SRC_LISTENER_H

#include <mpi.h>
#include "Thread.h"
#include "Message.h"
#include "Pool.h"

namespace papyruskv {

class Platform;

class Listener : public Thread {
public:
    Listener(Platform* platform);
    virtual ~Listener();

    virtual void Stop();

private:
    void ExecutePut(Message& msg, int rank);
    void ExecuteGet(Message& msg, int rank);
    void ExecuteMigrate(Message& msg, int rank);
    void ExecuteSignal(Message& msg, int rank);
    void ExecuteBarrier(Message& msg, int rank);
    void ExecuteUpdate(Message& msg, int rank);
    void ExecuteExit(Message& msg, int rank);

private:
    virtual void Run();

private:
    Platform* platform_;
    MPI_Comm mpi_comm_;
    MPI_Comm mpi_comm_ext_;
    Pool* pool_;
    char* big_buffer_;
    int* ret_buffer_;

    int rank_;
    int group_;

    int sleep_usec_;
};

} /* namespace papyruskv */

#endif /* PAPYRUS_KV_SRC_LISTENER_H */

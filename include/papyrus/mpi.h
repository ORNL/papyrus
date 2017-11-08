#ifndef PAPYRUS_INCLUDE_PAPYRUS_MPI_H
#define PAPYRUS_INCLUDE_PAPYRUS_MPI_H

#include <mpi.h>
#include <stdio.h>

#define MPI_Init(ARGC, ARGV)  {                                                         \
    int provided;                                                                       \
    MPI_Init_thread(ARGC, ARGV, MPI_THREAD_MULTIPLE, &provided);                        \
    if (provided != MPI_THREAD_MULTIPLE) {                                              \
        fprintf(stderr, "FAILED:MPI_THREAD_MULTIPLE not supported[%d]\n", provided);    \
        MPI_Abort(MPI_COMM_WORLD, 1);                                                   \
    }                                                                                   \
}

#endif /* PAPYRUS_INCLUDE_PAPYRUS_MPI_H */

#ifndef PAPYRUS_INCLUDE_PAPYRUS_BUPC_H
#define PAPYRUS_INCLUDE_PAPYRUS_BUPC_H

#include <bupc_extern.h>
#include <stdio.h>

extern int MPI_Init_thread(int*, char***, int, int*);

int __real_MPI_Init(int* argc, char*** argv);

int __wrap_MPI_Init(int* argc, char*** argv) {
    int provided;
    MPI_Init_thread(argc, argv, 3, &provided);
    if (provided != 3) fprintf(stderr, "[%s:%d] provided[%d]\n", __FILE__, __LINE__, provided);
    return 0;
}

#endif /* PAPYRUS_INCLUDE_PAPYRUS_BUPC_H */

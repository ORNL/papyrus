# Papyrus
Papyrus is a programming system that provides features for scalable, aggregate, persistent memory in an extreme-scale system for typical HPC usage scenarios. Papyrus provides a portable and scalable programming interface to access and manage parallel data structures on the distributed NVM storage.

## Requirements

- C++11 compiler
- MPI library supporting MPI\_THREAD\_MULTIPLE
- CMake (>=3.8)

## Installation

You can download Papyrus from code.ornl.gov:

    $ git clone https://code.ornl.gov/eck/papyrus.git
    $ cd papyrus

You can build Papyrus with CMake and Make:

    $ mkdir build
    $ cd build
    $ cmake .. -DCMAKE_INSTALL_PREFIX=<install_dir>
      or if you want to run tests after build
    $ cmake .. -DCMAKE_INSTALL_PREFIX=<install_dir> -DMPIEXEC=<mpiexec_path> -DMPIEXEC_NUMPROC_FLAG=<np_flag>
    $ make
    $ make install

### Running tests

For the Cray MPI Library, an environment variable MPICH\_MAX\_THREAD\_SAFETY has to be set to multiple:

    $ export MPICH_MAX_THREAD_SAFETY=multiple

The project's test suite can be run by executing:

    $ make test (or use 'ctest -V' for verbose test output)

## Repository contents

- The public interface is in include/papyrus/\*.h.
- The Key-Value Store is in kv/.

## References

To cite Papyrus, please use the following papers:

- Jungwon Kim, Kittisak Sajjapongse, Seyong Lee, and Jeffrey S. Vetter. "Design and Implementation of Papyrus: Parallel Aggregate Persistent Storage". IPDPS 2017, DOI: [10.1109/IPDPS.2017.72](https://doi.org/10.1109/IPDPS.2017.72)
- Jungwon Kim, Seyong Lee, and Jeffrey S. Vetter. "PapyrusKV: A High-Performance Parallel Key-Value Store for Distributed NVM Architectures". SC 2017. DOI: [10.1145/3126908.3126943](https://doi.org/10.1145/3126908.3126943)

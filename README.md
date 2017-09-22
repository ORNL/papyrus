# Papyrus
Papyrus is a programming system written at Oak Ridge National Laboratory that provides features for scalable, aggregate, persistent memory in an extreme-scale system for typical HPC usage scenarios.

## Requirements

- C++11 compiler
- MPI library support MPI\_THREAD\_MULTIPLE
- CMake (>=2.8)

## Installation

You can build Papyrus with CMake and Make:

    $ git clone https://code.ornl.gov/eck/papyrus.git
    $ cd papyrus
    $ mkdir build; cd build
    $ cmake ..
    $ make
    $ make test (or use 'ctest -V' for verbose test output)

## Repository contents

- The public interface is in include/papyrus/*.h.
- The Key-Value Store in is kv/.

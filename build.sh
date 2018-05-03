# This is a sample build script for CSCS's Grand Tave
rm -rf build install
mkdir build
cd build
cmake .. -DCMAKE_INSTALL_PREFIX=../install -DMPIEXEC="/opt/intel/compilers_and_libraries_2017.4.196/linux/mpi/intel64/bin/mpirun" -DMPIEXEC_NUMPROC_FLAG="-n"
make -j install

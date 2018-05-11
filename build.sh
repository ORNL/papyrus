# This is a sample build script for CSCS's Grand Tave
rm -rf build install
mkdir build
cd build
cmake .. -DCMAKE_INSTALL_PREFIX=../install -DMPIEXEC="/opt/intel/compilers_and_libraries_2017.4.196/linux/mpi/intel64/bin/mpirun" -DMPIEXEC_NUMPROC_FLAG="-n" -DPAPYRUS_USE_FORTRAN=ON # Stampede2
#cmake .. -DCMAKE_INSTALL_PREFIX=../install -DMPIEXEC="/opt/slurm/default/bin/srun" -DMPIEXEC_NUMPROC_FLAG="-n" -DPAPYRUS_USE_FORTRAN=ON #Grand Tave
make -j install

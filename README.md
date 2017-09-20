**Papyrus is a programming system written at Oak Ridge National Laboratory that provides features for scalable, aggregate, persistent memory in an extreme-scale system for typical HPC usage scenarios.**

Authors: Jungwon Kim (kimj@ornl.gov), Kittisak Sajjapongse (kittisaks@computer.org), Seyong Lee (slee2@ornl.gov), and Jeffrey S. Vetter (vetter@ornl.gov)

# How to build
You can build Papyrus with CMake and Make:
$ cmake <papyrus_source_directory> -DCMAKE_INSTALL_PREFIX=<papyrus_install_directory>
$ make install

# Repository contents
The public interface is in include/papyrus/*.h.

The Key-Value Store in is kv/.

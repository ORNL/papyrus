#!/bin/bash -l

#SBATCH -J workload
#SBATCH -o output.o%j
#SBATCH -e output.e%j
#SBATCH -p normal
#SBATCH -N 64
#SBATCH -n 4352
#SBATCH -t 02:00:00
#SBATCH -A ##-#########

KEYLEN=16
VALLEN=(1024 4096 16384)
COUNT=1000
RANKS=(68 136 272 544 1088 2176 4352)

export PAPYRUSKV_REPOSITORY=/tmp/pkv_workload
export PAPYRUSKV_DESTROY_REPOSITORY=1

export PAPYRUSKV_GROUP_SIZE=1
export PAPYRUSKV_CONSISTENCY=1
export PAPYRUSKV_SSTABLE=2
export PAPYRUSKV_CACHE_LOCAL=0
export PAPYRUSKV_CACHE_REMOTE=0
export PAPYRUSKV_BLOOM=1
for i in "${RANKS[@]}"; do
    for j in "${VALLEN[@]}"; do
        ibrun -np $i ./workload $KEYLEN $j $COUNT 50
    done
done

export PAPYRUSKV_GROUP_SIZE=1
export PAPYRUSKV_CONSISTENCY=1
export PAPYRUSKV_SSTABLE=2
export PAPYRUSKV_CACHE_LOCAL=0
export PAPYRUSKV_CACHE_REMOTE=0
export PAPYRUSKV_BLOOM=1
for i in "${RANKS[@]}"; do
    for j in "${VALLEN[@]}"; do
        ibrun -np $i ./workload $KEYLEN $j $COUNT 5
    done
done

export PAPYRUSKV_GROUP_SIZE=1
export PAPYRUSKV_CONSISTENCY=1
export PAPYRUSKV_SSTABLE=2
export PAPYRUSKV_CACHE_LOCAL=0
export PAPYRUSKV_CACHE_REMOTE=0
export PAPYRUSKV_BLOOM=1
for i in "${RANKS[@]}"; do
    for j in "${VALLEN[@]}"; do
        ibrun -np $i ./workload $KEYLEN $j $COUNT 0
    done
done

export PAPYRUSKV_GROUP_SIZE=1
export PAPYRUSKV_CONSISTENCY=1
export PAPYRUSKV_SSTABLE=2
export PAPYRUSKV_CACHE_LOCAL=0
export PAPYRUSKV_CACHE_REMOTE=1
export PAPYRUSKV_BLOOM=1
for i in "${RANKS[@]}"; do
    for j in "${VALLEN[@]}"; do
        ibrun -np $i ./workload $KEYLEN $j $COUNT 0
    done
done

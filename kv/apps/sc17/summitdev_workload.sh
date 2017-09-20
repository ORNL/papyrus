#!/bin/bash

#BSUB -P ######
#BSUB -J workload
#BSUB -o output.o%J
#BSUB -W 1:00
#BSUB -n 320
#BSUB -env "all,JOB_FEATURE=NVME"

KEYLEN=16
VALLEN=(8)
COUNT=10000
RANKS=(1 2 4 8 16 20 40 80 160 320)

export PAPYRUSKV_REPOSITORY=/xfs/scratch/kimj1/pkv_workload
export PAPYRUSKV_DESTROY_REPOSITORY=1

export PAPYRUSKV_GROUP_SIZE=20
export PAPYRUSKV_CONSISTENCY=1
export PAPYRUSKV_SSTABLE=2
export PAPYRUSKV_CACHE_LOCAL=0
export PAPYRUSKV_CACHE_REMOTE=1
export PAPYRUSKV_BLOOM=1
for i in "${RANKS[@]}"; do
    for j in "${VALLEN[@]}"; do
        mpirun -np $i --map-by ppr:10:socket ./workload $KEYLEN $j $COUNT 50
    done
done

export PAPYRUSKV_REPOSITORY=/lustre/atlas/scratch/kimj1/csc103/pkv_workload
export PAPYRUSKV_DESTROY_REPOSITORY=1

export PAPYRUSKV_GROUP_SIZE=20
export PAPYRUSKV_CONSISTENCY=1
export PAPYRUSKV_SSTABLE=2
export PAPYRUSKV_CACHE_LOCAL=0
export PAPYRUSKV_CACHE_REMOTE=1
export PAPYRUSKV_BLOOM=1
for i in "${RANKS[@]}"; do
    for j in "${VALLEN[@]}"; do
        mpirun -np $i --map-by ppr:10:socket ./workload $KEYLEN $j $COUNT 50
    done
done

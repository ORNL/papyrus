#!/bin/bash

#BSUB -P ######
#BSUB -J 1node
#BSUB -o output.o%J
#BSUB -W 4:00
#BSUB -n 20
#BSUB -env "all,JOB_FEATURE=NVME"

KEYLEN=16
VALLEN=(8 32 128 512 2048 8192 32768 131072 524288)
COUNT=10000
FLUSH=2

export PAPYRUSKV_REPOSITORY=/xfs/scratch/kimj1/pkv_1node
export PAPYRUSKV_DESTROY_REPOSITORY=1

export PAPYRUSKV_GROUP_SIZE=1
export PAPYRUSKV_CONSISTENCY=2
export PAPYRUSKV_SSTABLE=2
export PAPYRUSKV_CACHE_LOCAL=0
export PAPYRUSKV_CACHE_REMOTE=0
export PAPYRUSKV_BLOOM=1
for i in "${VALLEN[@]}"; do
    mpirun -np 20 --map-by ppr:10:socket ./basic $KEYLEN $i $COUNT $FLUSH
done

export PAPYRUSKV_REPOSITORY=/lustre/atlas/scratch/kimj1/csc103/pkv_1node
export PAPYRUSKV_DESTROY_REPOSITORY=1

export PAPYRUSKV_GROUP_SIZE=1
export PAPYRUSKV_CONSISTENCY=2
export PAPYRUSKV_SSTABLE=2
export PAPYRUSKV_CACHE_LOCAL=0
export PAPYRUSKV_CACHE_REMOTE=0
export PAPYRUSKV_BLOOM=1
for i in "${VALLEN[@]}"; do
    mpirun -np 20 --map-by ppr:10:socket ./basic $KEYLEN $i $COUNT $FLUSH
done


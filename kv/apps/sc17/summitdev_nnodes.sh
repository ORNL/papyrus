#!/bin/bash

#BSUB -P ######
#BSUB -J nnodes
#BSUB -o output.o%J
#BSUB -W 1:00
#BSUB -n 320
#BSUB -env "all,JOB_FEATURE=NVME"

KEYLEN=16
VALLEN=131072
COUNT=10000
RANKS=(320)

export PAPYRUSKV_REPOSITORY=/xfs/scratch/kimj1/pkv_nnodes
export PAPYRUSKV_DESTROY_REPOSITORY=1

rm -rf $PAPYRUSKV_REPOSITORY

export PAPYRUSKV_GROUP_SIZE=1
export PAPYRUSKV_CONSISTENCY=1
export PAPYRUSKV_SSTABLE=1
export PAPYRUSKV_CACHE_LOCAL=0
export PAPYRUSKV_CACHE_REMOTE=0
export PAPYRUSKV_BLOOM=1
for i in "${RANKS[@]}"; do
    mpirun -np $i --map-by ppr:10:socket ./basic $KEYLEN $VALLEN $COUNT put
done

export PAPYRUSKV_GROUP_SIZE=1
export PAPYRUSKV_CONSISTENCY=2
export PAPYRUSKV_SSTABLE=1
export PAPYRUSKV_CACHE_LOCAL=0
export PAPYRUSKV_CACHE_REMOTE=0
export PAPYRUSKV_BLOOM=1
for i in "${RANKS[@]}"; do
    mpirun -np $i --map-by ppr:10:socket ./basic $KEYLEN $VALLEN $COUNT
done

export PAPYRUSKV_GROUP_SIZE=1
export PAPYRUSKV_CONSISTENCY=2
export PAPYRUSKV_SSTABLE=2
export PAPYRUSKV_CACHE_LOCAL=0
export PAPYRUSKV_CACHE_REMOTE=0
export PAPYRUSKV_BLOOM=1
for i in "${RANKS[@]}"; do
    mpirun -np $i --map-by ppr:10:socket ./basic $KEYLEN $VALLEN $COUNT
done

export PAPYRUSKV_GROUP_SIZE=20
export PAPYRUSKV_CONSISTENCY=2
export PAPYRUSKV_SSTABLE=1
export PAPYRUSKV_CACHE_LOCAL=0
export PAPYRUSKV_CACHE_REMOTE=0
export PAPYRUSKV_BLOOM=1
for i in "${RANKS[@]}"; do
    mpirun -np $i --map-by ppr:10:socket ./basic $KEYLEN $VALLEN $COUNT
done

export PAPYRUSKV_GROUP_SIZE=20
export PAPYRUSKV_CONSISTENCY=2
export PAPYRUSKV_SSTABLE=2
export PAPYRUSKV_CACHE_LOCAL=0
export PAPYRUSKV_CACHE_REMOTE=0
export PAPYRUSKV_BLOOM=1
for i in "${RANKS[@]}"; do
    mpirun -np $i --map-by ppr:10:socket ./basic $KEYLEN $VALLEN $COUNT
done

export PAPYRUSKV_REPOSITORY=/lustre/atlas/scratch/kimj1/csc103/pkv_nranks
export PAPYRUSKV_DESTROY_REPOSITORY=1

export PAPYRUSKV_GROUP_SIZE=1
export PAPYRUSKV_CONSISTENCY=1
export PAPYRUSKV_SSTABLE=1
export PAPYRUSKV_CACHE_LOCAL=0
export PAPYRUSKV_CACHE_REMOTE=0
export PAPYRUSKV_BLOOM=1
for i in "${RANKS[@]}"; do
    mpirun -np $i --map-by ppr:10:socket ./basic $KEYLEN $VALLEN $COUNT put
done

export PAPYRUSKV_GROUP_SIZE=1
export PAPYRUSKV_CONSISTENCY=2
export PAPYRUSKV_SSTABLE=1
export PAPYRUSKV_CACHE_LOCAL=0
export PAPYRUSKV_CACHE_REMOTE=0
export PAPYRUSKV_BLOOM=1
for i in "${RANKS[@]}"; do
    mpirun -np $i --map-by ppr:10:socket ./basic $KEYLEN $VALLEN $COUNT put
done

export PAPYRUSKV_GROUP_SIZE=1
export PAPYRUSKV_CONSISTENCY=2
export PAPYRUSKV_SSTABLE=2
export PAPYRUSKV_CACHE_LOCAL=0
export PAPYRUSKV_CACHE_REMOTE=0
export PAPYRUSKV_BLOOM=1
for i in "${RANKS[@]}"; do
    mpirun -np $i --map-by ppr:10:socket ./basic $KEYLEN $VALLEN $COUNT
done

export PAPYRUSKV_GROUP_SIZE=20
export PAPYRUSKV_CONSISTENCY=2
export PAPYRUSKV_SSTABLE=2
export PAPYRUSKV_CACHE_LOCAL=0
export PAPYRUSKV_CACHE_REMOTE=0
export PAPYRUSKV_BLOOM=1
for i in "${RANKS[@]}"; do
    mpirun -np $i --map-by ppr:10:socket ./basic $KEYLEN $VALLEN $COUNT
done

export PAPYRUSKV_GROUP_SIZE=640
export PAPYRUSKV_CONSISTENCY=2
export PAPYRUSKV_SSTABLE=2
export PAPYRUSKV_CACHE_LOCAL=0
export PAPYRUSKV_CACHE_REMOTE=0
export PAPYRUSKV_BLOOM=1
for i in "${RANKS[@]}"; do
    mpirun -np $i --map-by ppr:10:socket ./basic $KEYLEN $VALLEN $COUNT
done


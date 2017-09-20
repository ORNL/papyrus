#!/bin/bash

#BSUB -P ######
#BSUB -J cr
#BSUB -o output.o%J
#BSUB -W 1:00
#BSUB -n 320
#BSUB -env "all,JOB_FEATURE=NVME"

KEYLEN=16
VALLEN=131072
COUNT=10000
RANKS=(20 40 80 160 320)

export PAPYRUSKV_REPOSITORY=/xfs/scratch/kimj1/pkv_cr
export PAPYRUSKV_LUSTRE=/lustre/atlas/scratch/kimj1/csc103/pkv_cr
export PAPYRUSKV_DESTROY_REPOSITORY=1

export PAPYRUSKV_GROUP_SIZE=1
export PAPYRUSKV_CONSISTENCY=2
export PAPYRUSKV_SSTABLE=1
export PAPYRUSKV_CACHE_LOCAL=0
export PAPYRUSKV_CACHE_REMOTE=0
export PAPYRUSKV_BLOOM=1

for i in "${RANKS[@]}"; do
    mpirun -np $i --map-by ppr:10:socket ./cr $KEYLEN $VALLEN $COUNT $PAPYRUSKV_LUSTRE c
export PAPYRUSKV_FORCE_REDISTRIBUTE=0
    mpirun -np $i --map-by ppr:10:socket ./cr $KEYLEN $VALLEN $COUNT $PAPYRUSKV_LUSTRE r
export PAPYRUSKV_FORCE_REDISTRIBUTE=1
    mpirun -np $i --map-by ppr:10:socket ./cr $KEYLEN $VALLEN $COUNT $PAPYRUSKV_LUSTRE r
    rm -rf $PAPYRUSKV_LUSTRE
done


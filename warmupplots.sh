#!/bin/bash

WARMUP=25

if [ ! -d tmp-warmup/data ]; then
    mkdir -p tmp-warmup/data;
fi;

if [ ! -d tmp-warmup/plots ]; then
    mkdir tmp-warmup/plots;
fi;

for q in {1..22}
do
    echo "========== Query $q =========="
    ./warmup.sh $WARMUP $q | tee tmp-warmup/data/tmp-$q.csv
    sed -E 's/$/,/' tmp-warmup/data/tmp-$q.csv | tr -d '\n' | sed 's/,$//' > tmp-warmup/data/$q.csv
    rm tmp-warmup/data/tmp-$q.csv
    python genplots.py -w $WARMUP tmp-warmup/data/$q.csv tmp-warmup/plots/$q.pdf
done

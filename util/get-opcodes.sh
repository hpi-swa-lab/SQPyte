#!/bin/bash

mkdir processed
for i in {1..22}
do
    sed '/>>>/!d' $i.out | sed 's/^>>> OP_//' | sed 's/ <<<$//' | sort | uniq > processed/$i.txt
done

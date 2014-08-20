#!/bin/bash

./benchrun.sh | tee tmp.txt
sed -E '/^[0-9]|^=/!d' tmp.txt | sed 's/$/,/' | sed 's/Query /aquery-/' | tr -d ' ' | tr -d '\n' | tr -d '=' | tr 'a' '\n' | sed '/^$/d' | sed 's/,$//' > benchres.csv
rm tmp.txt

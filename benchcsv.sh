#!/bin/bash

./benchrun.sh | tee tmp-bench-sqpyte.txt
sed -E '/^[0-9]|^=/!d' tmp-bench-sqpyte.txt | sed 's/$/,/' | sed 's/Query /aquery-/' | tr -d ' ' | tr -d '\n' | tr -d '=' | tr 'a' '\n' | sed '/^$/d' | sed 's/,$//' > bench-res.csv
rm tmp-bench-sqpyte.txt
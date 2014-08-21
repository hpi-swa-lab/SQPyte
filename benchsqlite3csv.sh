#!/bin/bash

./benchsqlite3.sh | tee tmp-bench-sqlite3.txt
sed 's/^R.*real //' tmp-bench-sqlite3.txt | sed 's/ user.*$//' | sed -E '/^[0-9]|^=/!d' | sed 's/$/,/' | sed 's/Query /aquery-/' | tr -d ' ' | tr -d '\n' | tr -d '=' | tr 'a' '\n' | sed '/^$/d' | sed 's/,$//' > bench-sqlite3-res.csv
rm tmp-bench-sqlite3.txt
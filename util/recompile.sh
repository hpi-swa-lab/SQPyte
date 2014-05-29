#!/bin/bash
set -e
make distclean
./configure
make
sudo make install
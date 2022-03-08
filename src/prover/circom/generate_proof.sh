#!/usr/bin/env bash

cd build/pyth_cpp

echo 'Pyth Circuit: Generate Witness'
time ./pyth $1 witness.wtns


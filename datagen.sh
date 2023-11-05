#!/bin/bash

for i in {100..100}
do
    dd if=/dev/urandom of=randomfile$i.txt bs="$((1024 * 1024))" count=$i
done

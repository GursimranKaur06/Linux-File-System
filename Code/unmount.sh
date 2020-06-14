#!/bin/sh

sudo umount -f -l ../master

if [ $# -lt 1 ]; then
    slaves=1
else
    slaves=$1
fi

i=0
while [ $i -lt $slaves ]; do
    sudo umount -f -l "../slave_$i"
    i=$(($i+1))
done

#!/bin/bash

uuid=$(uuidgen)
froot=$1; shift

dir=$froot$uuid

#create unique dir for current job
mkdir $dir
file_base=$dir/out_

#-m flag deletes source files, should retain by default
zip -qq - $@ | split -b 1m -d - $file_base

if [ $? -eq 0 ]
then
    echo -n "$uuid " && ls $dir | head -c -1 | tr "\n" " "
else
    rm -r $dir
    exit 1
fi
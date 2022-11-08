#!/bin/bash

uuid=$(uuidgen)
froot=$1; shift
ziproot=$1; shift
out_name=$1; shift

dir=$froot$uuid

#create unique dir for current job
mkdir $dir
file=$dir/$out_name

#if no files provided just create an empty zip file
if [ $# -eq 0 ]
then
    touch $file
else
    cd ziproot
    #-m flag deletes source files, should retain by default
    zip -qq -r $file $@
fi

if [ $? -eq 0 ] && [ -f "$file" ]
then
    echo -n $file
else
    rm -r $dir
    exit 1
fi
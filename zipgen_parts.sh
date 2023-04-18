#!/bin/bash

uuid=$(uuidgen)
froot=$1; shift
ziproot=$1; shift

dir=$froot$uuid

#create unique dir for current job
mkdir $dir
file_base=$dir/out_

#if no files provided just create an empty single file
if [ $# -eq 0 ]
then
    file=${file_base}00
    touch $file
else
    cd $ziproot
    #-m flag deletes source files, should retain by default
    zip -qq -r - $@ | split -b 4m -d - $file_base
fi

if [ $? -eq 0 ]
then
    echo -n "$uuid " && ls $dir | head -c -1 | tr "\n" " "
else
    rm -r $dir
    exit 1
fi
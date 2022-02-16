#!/bin/bash

sleeptime=$1
beg=$2
end=$3

for i in `seq ${beg} ${end}`;
do
  echo "Sleeping ${sleeptime} secs."
  sleep ${sleeptime}
  cmd="cp -r ./ptycho/fly$i ./ptycho-test/"
  echo ${cmd}
  cp -r "./ptycho/fly$i" "./ptycho-test/"
done    

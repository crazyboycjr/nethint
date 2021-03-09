#!/bin/bash
if [ "$#" -lt 1 ]; then echo "$(tput setaf 1)[ERROR]$(tput sgr 0) number of tests required"; exit -1; fi
if [ "$#" -gt 1 ]; then echo "$(tput setaf 1)[ERROR]$(tput sgr 0) too many arguments: $#"; exit -1; fi

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM SIGHUP EXIT

log_dir=log
mkdir -p $log_dir

for ((i=0;i<$1;i++))
do
  ./rplaunch --controller-ssh 192.168.211.35 --controller-uri 192.168.211.35:9000 --hostfile hostfiles/$i --jobname mapreduce 2>&1 | tee $log_dir/$i &
  sleep 10
done
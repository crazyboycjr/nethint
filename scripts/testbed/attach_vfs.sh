#!/bin/bash

if [ $UID -ne 0 ]; then
	echo "Please run $0 as root"
	exit 3
fi

source `dirname $0`/utils.sh

names=(`lsnames 8`);
for i in {0..7}; do
	name=${names[$i]};
	virsh attach-device --domain $name --file /nfs/cjr/Developing/nethint-rs/scripts/testbed/vfconfig/vf${i}.xml
done

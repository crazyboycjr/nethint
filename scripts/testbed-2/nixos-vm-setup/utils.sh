#!/bin/bash

function lsnames {
	host_id=`hostname | cut -d'-' -f2`
	[[ -n $1 ]] && num_vms=$1 || num_vms=8
	# base: host_id, len: num_vms, offset: i, id: j
	for ((i=0;i<$num_vms;i++)); do
		j=`expr $host_id \* $num_vms + $i - $num_vms`;
		name=nixos${j}
		echo $name
	done
}

#!/bin/bash

function lsnames {
	host_id=`hostname | cut -d'-' -f2`
	[[ -n $1 ]] && num_vms=$1 || num_vms=8
	# base: host_id, len: num_vms, offset: i, id: j
	for ((i=0;i<$num_vms;i++)); do
		j=`expr $host_id \* $num_vms + $i - $num_vms`;
		name=cpu${j}
		echo $name
	done
}

function get_rack_agent_ip {
	# my_ip=`ip a show rdma0 | grep 'inet ' | awk '{print $2}' | cut -d'/' -f1`
	my_id=`hostname | cut -d'-' -f2`
	if [ $my_id -le 3 ]; then
		echo "192.168.211.2"
	else
		echo "192.168.211.130"
	fi
}

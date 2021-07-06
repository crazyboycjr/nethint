#!/bin/bash

set -e

if [ $UID -ne 0 ]; then
	echo "Please run $0 as root"
	exit 3
fi

if [ $# -ne 1 ]; then
	echo "Usage: $0 <pf_intf>"
	exit 1
fi

pf=$1

# Create an OVS bridge (here it's named ovs-sriov).
ovs-vsctl add-br ovs0

# check the result
ovs-vsctl get Open_vSwitch . other_config
ovs-vsctl get Open_vSwitch . dpdk_initialized

# Restart the openvswitch service. This step is required for HW offload changes to take effect.
systemctl restart openvswitch-switch.service

# Make sure to bring up the PF and representor netdevices.
ovs-vsctl add-port ovs0 $pf

# show something
ovs-vsctl list-ports ovs0
ovs-dpctl show

# add sflow configuration
DIR=$(dirname `realpath $0`)
source "$DIR"/utils.sh
rack_agent_ip=`get_rack_agent_ip`
echo 'rack agent IP: ' $rack_agent_ip
./setup_sflow.sh $rack_agent_ip

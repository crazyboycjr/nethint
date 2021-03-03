#!/bin/bash

set -e

if [ $UID -ne 0 ]; then
	echo "Please run $0 as root"
	exit 3
fi

if [ $# -ne 2 ]; then
	echo "Usage: $0 <pf_intf> <num_vfs>"
	exit 1
fi

pf=$1
num_vfs=$2

# Create an OVS bridge (here it's named ovs-sriov).
ovs-vsctl add-br ovs-sriov

# Enable hardware offload (disabled by default).
ovs-vsctl set Open_vSwitch . other_config:hw-offload=true

# The aging timeout of OVS is given is ms and can be controlled with this command:
ovs-vsctl set Open_vSwitch . other_config:max-idle=30000

# check the result
ovs-vsctl get Open_vSwitch . other_config

# Restart the openvswitch service. This step is required for HW offload changes to take effect.
systemctl restart openvswitch-switch.service


# Make sure to bring up the PF and representor netdevices.
ovs-vsctl add-port ovs-sriov $pf
for ((i=0;i<$num_vfs;i++)); do
	ovs-vsctl add-port ovs-sriov ${pf}_${i}
done

# show something
ovs-vsctl list-ports ovs-sriov
ovs-dpctl show


# sudo ovs-appctl dpctl/dump-flows type=all -m
# This will give results like below. All type of flows are displayed!
# recirc_id(0),in_port(2),eth(src=02:49:61:d4:70:e8,dst=02:bc:b6:ff:bf:97),eth_type(0x0800),ipv4(frag=no), packets:584316, bytes:36666200, used:4.940s, actions:3
# recirc_id(0),in_port(2),eth(src=02:49:61:d4:70:e8,dst=02:bc:b6:ff:bf:97),eth_type(0x0806), packets:0, bytes:0, used:3.110s, actions:3
# recirc_id(0),in_port(3),eth(src=02:bc:b6:ff:bf:97,dst=02:49:61:d4:70:e8),eth_type(0x0800),ipv4(frag=no), packets:29461402, bytes:34366302886, used:4.940s, actions:2
# recirc_id(0),in_port(3),eth(src=02:bc:b6:ff:bf:97,dst=02:49:61:d4:70:e8),eth_type(0x0806), packets:2, bytes:120, used:2.090s, actions:2
# recirc_id(0),in_port(3),eth(src=1c:34:da:a5:55:94,dst=01:80:c2:00:00:0e),eth_type(0x88cc), packets:0, bytes:0, used:4.340s, actions:drop
# recirc_id(0),in_port(3),eth(src=0c:42:a1:ef:1b:06,dst=ff:ff:ff:ff:ff:ff),eth_type(0x0806),arp(sip=192.168.211.162,tip=192.168.211.2,op=1/0xff), packets:2120, bytes:127200, used:0.141s, actions:1,2
# recirc_id(0),in_port(3),eth(src=0c:42:a1:ef:1a:4e,dst=ff:ff:ff:ff:ff:ff),eth_type(0x0806),arp(sip=192.168.211.130,tip=192.168.211.2,op=1/0xff), packets:2121, bytes:127260, used:0.633s, actions:1,2
# recirc_id(0),in_port(3),eth(src=0c:42:a1:ef:1b:5a,dst=ff:ff:ff:ff:ff:ff),eth_type(0x0806),arp(sip=192.168.211.194,tip=192.168.211.2,op=1/0xff), packets:2120, bytes:127200, used:0.401s, actions:1,2
# recirc_id(0),in_port(3),eth(src=0c:42:a1:ef:1b:26,dst=ff:ff:ff:ff:ff:ff),eth_type(0x0806),arp(sip=192.168.211.34,tip=192.168.211.2,op=1/0xff), packets:3120, bytes:187200, used:0.721s, actions:1,2
# recirc_id(0),in_port(3),eth(src=0c:42:a1:ef:1a:4a,dst=ff:ff:ff:ff:ff:ff),eth_type(0x0806),arp(sip=192.168.211.66,tip=192.168.211.2,op=1/0xff), packets:2118, bytes:127080, used:0.845s, actions:1,2

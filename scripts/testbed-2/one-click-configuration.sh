#!/bin/bash

DIR=`dirname $(realpath $0)`

sudo "$DIR"/enable_sriov.sh rdma0 1
sudo "$DIR"/setup_ovs.sh rdma0
sudo "$DIR"/migrate_pf.sh up

sudo ip link set rdma0 mtu 9000
sudo ip link set ovs-system mtu 9000
sudo mlnx_qos -i rdma0 --prio_tc=0,1,2,3,4,5,6,7 -r 0,0,0,0,0,0,0,0

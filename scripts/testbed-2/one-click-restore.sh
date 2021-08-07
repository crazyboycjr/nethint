#!/usr/bin/env bash

DIR=`dirname $(realpath $0)`

sudo "$DIR"/migrate_pf.sh down
sudo ovs-vsctl del-br ovs0
sudo "$DIR"/enable_sriov.sh rdma0 0

sudo ip link set ovs-system mtu 1500
sudo ip link set rdma0 mtu 1500

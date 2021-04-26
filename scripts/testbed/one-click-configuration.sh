#!/bin/bash



sudo ./enable_sriov.sh rdma0 5
sudo ./enable_eswitch.sh rdma0
sudo ./setup_ovs.sh rdma0 4
sudo ./migrate_pf.sh up
sudo mlnx_qos -i rdma0 --prio_tc=0,1,2,3,4,5,6,7 -r 0,0,0,0,0,0,0,0

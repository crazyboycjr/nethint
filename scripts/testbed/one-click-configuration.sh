#!/bin/bash



sudo ./enable_sriov.sh rdma0 5
sudo ./enable_eswitch.sh rdma0
sudo ./setup_ovs.sh rdma0 4
sudo ./migrate_pf.sh up

#!/bin/bash

set -e

if [ $# -ne 1 ]; then
	echo "Usage: $0 up/down"
	exit 1
fi


if [ $1 = "up" ]; then

	echo 0000:18:00.5 | sudo tee /sys/bus/pci/drivers/vfio-pci/unbind
	echo 0000:18:00.5 | sudo tee /sys/bus/pci/drivers/mlx5_core/bind
	echo "sleep for 5 seconds to wait for the interface ready"
	sleep 5
	sudo ip link set enp24s0v4 up
	cidr=`ip a show rdma0 | grep 'inet ' | awk '{print $2}'`
	sudo ip addr del ${cidr} dev rdma0
	sudo ip addr add ${cidr} dev enp24s0v4
	sudo ovs-vsctl add-port ovs-sriov rdma0_4

elif [ $1 = "down" ]; then

	sudo ovs-vsctl del-port ovs-sriov rdma0_4
	cidr=`ip a show enp24s0v4 | grep 'inet ' | awk '{print $2}'`
	sudo ip addr del ${cidr} dev enp24s0v4
	sudo ip addr add ${cidr} dev rdma0
	sudo ip link set enp24s0v4 down
	echo 0000:18:00.5 | sudo tee /sys/bus/pci/drivers/mlx5_core/unbind
	echo 0000:18:00.5 | sudo tee /sys/bus/pci/drivers/vfio-pci/bind

else
	echo "Usage: $0 up/down"
	exit 1
fi

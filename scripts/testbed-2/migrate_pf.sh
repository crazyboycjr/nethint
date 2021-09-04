#!/bin/bash

set -e

if [ $# -ne 1 ]; then
	echo "Usage: $0 up/down"
	exit 1
fi


if [ $1 = "up" ]; then

	echo 0000:18:00.1 | sudo tee /sys/bus/pci/drivers/vfio-pci/unbind
	echo 0000:18:00.1 | sudo tee /sys/bus/pci/drivers/mlx5_core/bind
	echo "sleep for 5 seconds to wait for the interface ready"
	sleep 5
	sudo ip link set enp24s0v0 up
	cidr=`ip a show rdma0 | grep 'inet ' | awk '{print $2}'`
	sudo ip addr del ${cidr} dev rdma0
	sudo ip addr add ${cidr} dev enp24s0v0
	sudo ip addr add ${cidr} dev rdma0

elif [ $1 = "down" ]; then

	cidr=`ip a show enp24s0v0 | grep 'inet ' | awk '{print $2}'`
	sudo ip addr del ${cidr} dev enp24s0v0
	sudo ip link set enp24s0v0 down
	echo 0000:18:00.1 | sudo tee /sys/bus/pci/drivers/mlx5_core/unbind
	echo 0000:18:00.1 | sudo tee /sys/bus/pci/drivers/vfio-pci/bind

else
	echo "Usage: $0 up/down"
	exit 1
fi

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

pci_addr=$(basename `readlink /sys/class/net/$pf/device`)
echo "PCI address of $pf is $pci_addr"
# domain:bus:slot.function
# pci_slot=`echo $pci_addr | cut -d "." -f1`

# check if sriov has already been enabled
echo 0 > /sys/class/net/$pf/device/sriov_numvfs
num=`cat /sys/class/net/$pf/device/sriov_numvfs`
if [ $num -ne 0 ]; then
	echo "$pf SR-IOV has already been enabled, to change the number of VFs, please disable it first, then execute this script"
	exit 2
fi

echo $num_vfs > /sys/class/net/$pf/device/sriov_numvfs

# set mac address of VFs
for ((i=0;i<$num_vfs;i++)); do
	macaddr=`tr -dc A-F0-9 < /dev/urandom | head -c 10 | sed -r 's/(..)/\1:/g;s/:$//;s/^/02:/'`
	ip link set $pf vf $i mac $macaddr;
done

ip link show $pf

# bind VF's driver to vfio-pci
modprobe vfio-pci

for ((i=0;i<$num_vfs;i++)); do
	vf_pci_addr=$(basename `readlink /sys/class/net/$pf/device/virtfn$i`)
	echo $vf_pci_addr > /sys/bus/pci/devices/$vf_pci_addr/driver/unbind
	numeric_id=`lspci -s $vf_pci_addr -n | cut -d " " -f3 | tr ':' ' '`
	echo $numeric_id > /sys/bus/pci/drivers/vfio-pci/new_id
	# show results
	lspci -k -s $vf_pci_addr
done

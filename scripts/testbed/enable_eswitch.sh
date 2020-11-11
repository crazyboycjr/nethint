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

pci_addr=$(basename `readlink /sys/class/net/$pf/device`)
echo "PCI address of $pf is $pci_addr"

pci_bus=$(printf "%d" "0x`echo $pci_addr | cut -d':' -f2`")
pci_slot=$(printf "%d" "0x`echo $pci_addr | cut -d':' -f3 | cut -d'.' -f1 `")
pf_altname="enp${pci_bus}s${pci_slot}"
echo "altname of PF: $pf_altname"

# check if device has already been set to eswitch mode
mode=`cat /sys/class/net/$pf/compat/devlink/mode`
if [ $mode = "switchmode" ]; then
	echo "$pf has already been set to eswitch mode"
	exit 2
fi

# unbind the VFs
num_vfs=`cat /sys/class/net/$pf/device/sriov_numvfs`
drivers=(`seq 1 8`)

for ((i=0;i<$num_vfs;i++)); do
	vf_pci_addr=$(basename `readlink /sys/class/net/$pf/device/virtfn$i`)
	driver=$(basename `readlink /sys/bus/pci/devices/$vf_pci_addr/driver`)
	drivers[$i]=$driver
	echo $vf_pci_addr > /sys/bus/pci/devices/$vf_pci_addr/driver/unbind
	# show results
	lspci -k -s $vf_pci_addr
done

echo switchdev > "/sys/class/net/$pf/compat/devlink/mode"

echo "sleeping for 20 seconds..."
sleep 20

# check if the VF representors has been renamed
ip link

# It is necessary to first set the network VF representor device names
# to be in the form of $PF_$VFID where $PF is the PF netdev name,
# and $VFID is the VF ID=0,1,[..], bring up these VF representors
for ((i=0;i<$num_vfs;i++)); do
	ip link set "${pf_altname}_${i}" name "${pf}_${i}"
	ip link set "${pf}_${i}" up
done


# re-bind the VFs' drivers
for ((i=0;i<$num_vfs;i++)); do
	vf_pci_addr=$(basename `readlink /sys/class/net/$pf/device/virtfn$i`)
	echo $vf_pci_addr > /sys/bus/pci/drivers/${drivers[$i]}/bind
	# show results
	lspci -k -s $vf_pci_addr
done

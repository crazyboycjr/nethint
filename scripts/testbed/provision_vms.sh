#!/bin/bash

if [ $UID -ne 0 ]; then
	echo "Please run $0 as root"
	exit 3
fi

source `dirname $0`/utils.sh

# cpubase m4.xlarge
virt-install --virt-type kvm --vcpus 8 --name cpubase --ram 16384 --boot hd --disk /var/lib/libvirt/images/cpu_vm_base.img,format=raw --network network=default --nographic --os-type=linux --os-variant=ubuntu20.04 --noreboot --import


for name in `lsnames 4`; do
	virsh vol-delete ${name}.img --pool images
	virt-clone --replace --original cpubase --name $name --file /var/lib/libvirt/images/${name}.img
	virt-sysprep --domain $name --enable customize,dhcp-client-state,machine-id --hostname $name
done

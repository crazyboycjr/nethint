#!/bin/bash

DIR=`dirname $(realpath $0)`
source "$DIR./utils.sh"

for name in `lsnames 8`; do
	sudo virsh vol-delete ${name}.img --pool images
	sudo virsh undefine $name
done

sudo virsh undefine nixosbase
sudo rm /var/lib/libvirt/images/nixos_vm_base.img

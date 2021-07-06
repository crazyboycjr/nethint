#!/bin/bash


[[ $UID -ne 0 ]] && echo "Please run $0 as root" && exit 1

# this will create a disk image with nixos filesystem at /tmp/nixos_vm_base.img
./prepare_disk.sh

DISK_FILE="/var/lib/libvirt/images/nixos_vm_base.img" 

[[ -e "$DISK_FILE" ]] && echo "$(tput setaf 1)[ERROR]$(tput sgr 0) image file already exists" && exit 2

# copy the image to image pool so virt-install can see it

echo "copying /tmp/nixos_vm_base.img to libvirt image pool"
cp /tmp/nixos_vm_base.img $DISK_FILE

# cpubase m4.xlarge
virt-install --virt-type kvm --name nixosbase --vcpus 8 --ram 16384 --boot hd --disk $DISK_FILE,format=raw --network network=default --network bridge=ovs0,virtualport_type=openvswitch,model=virtio --nographic --os-type=linux --os-variant=generic --noreboot --import

# provision 8 NixOS VMs
source `dirname $0`/utils.sh

function customize()
{
	name=$1
	# customize the ip address on ens3 interface
	# sed -i "s/192.168.211.3/192.168.211.3/" ./mnt/etc/nixos/configuration.nix
	# customize hostname
	sed -i "s/networking.hostName = \"nixos\"/networking.hostName = \"$name\"/" ./mnt/etc/nixos/configuration.nix
}

for name in `lsnames 8`; do
	TARGET="/var/lib/libvirt/images/${name}.img"
	virsh vol-delete ${name}.img --pool images
	virt-clone --replace --original nixosbase --name $name --file "$TARGET"

	# an alternative to virt-sysprep
	mkdir -p ./mnt; umount ./mnt; losetup -D
	LOOP_DEV=`losetup -f`
	losetup -P $LOOP_DEV $TARGET
	mount ${LOOP_DEV}p1 ./mnt
	customize $name
	export NIX_PATH=nixpkgs=/nix/var/nix/profiles/per-user/cjr/channels/nixos/nixpkgs
	nixos-install --root $(realpath ./mnt) --no-root-passwd
	nixos-install --root $(realpath ./mnt) --no-root-passwd --no-bootloader
	sync; umount -R ./mnt; losetup -D
done

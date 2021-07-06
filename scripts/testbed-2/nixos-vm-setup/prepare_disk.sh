#!/bin/bash

# This script will create cpu_vm_base.img in the current directory,
# the cpu_vm_base.img can be used as a disk and boot by qemu

FSLABEL="NixOS" MNT_DIR=./mnt
mkdir -p $MNT_DIR

DISK_IMG=/tmp/nixos_vm_base.img

umount $MNT_DIR
losetup -D

# create raw disk, the 5GB is not enought for the latest OFED packages, so give it 10GB
dd if=/dev/zero of=$DISK_IMG bs=1G count=10 status=progress && sync

# create only 1 partition, mark it bootable
# an example command
echo -en 'n\np\n1\n\n\na\nw\n\n' | fdisk $DISK_IMG

# in the below example output, I only create 100MB file disk
# cjr@cpu22 /tmp % fdisk -l raw_disk.img
# Disk raw.bin: 100 MiB, 104857600 bytes, 204800 sectors
# Units: sectors of 1 * 512 = 512 bytes
# Sector size (logical/physical): 512 bytes / 512 bytes
# I/O size (minimum/optimal): 512 bytes / 512 bytes
# Disklabel type: dos
# Disk identifier: 0x7bcdb498
# 
# Device     Boot Start    End Sectors Size Id Type
# raw.bin1   *     2048 204799  202752  99M 83 Linux 

LOOP_DEV=`losetup -f`
if [ $? -ne 0 ]; then
	echo "losetup -f cannot find free loop device"
	exit 1
fi

losetup $LOOP_DEV $DISK_IMG
partprobe $LOOP_DEV
# root@cpu21 /tmp # lsblk
# NAME      MAJ:MIN RM  SIZE RO TYPE MOUNTPOINT
# loop0       7:0    0  4.7G  0 loop
# ├─loop0p1 259:0    0  4.7G  0 loop
# sda         8:0    0  1.8T  0 disk
# └─sda1      8:1    0  1.8T  0 part /

LOOP_PART=${LOOP_DEV}p1
# format the filesystems
mkfs.ext4 -L "$FSLABEL" $LOOP_PART

# mount the partitions
mount $LOOP_PART $MNT_DIR

# nixos bootstrap, the typical procedure
# nixos-generate-config --root $MNT_DIR --no-filesystems
# edit ${MNT_DIR}/etc/nixos/configuration.nix
# nixos-install --root $MNT_DIR
mkdir -p $MNT_DIR/etc/nixos/
cp pubkeys.nix configuration.nix hardware-configuration.nix $MNT_DIR/etc/nixos/
export NIX_PATH=nixpkgs=/nix/var/nix/profiles/per-user/cjr/channels/nixos/nixpkgs
# this command will fail at installing bootloader, but it will successfully create grub.cfg
nixos-install --root $(realpath $MNT_DIR) --no-root-passwd
# so re-execute it to finish the remaining stages
nixos-install --root $(realpath $MNT_DIR) --no-bootloader --no-root-passwd

# install bootloader manually. Somehow the nixos-enter cannot work with grub-install on distros other than NixOS
# so I have to use this workaround. TODO(cjr): Also make sure the two grub2 are the same version.
nix-shell -p grub2 --run "grub-install --boot-directory=$MNT_DIR/boot --recheck --target=i386-pc $LOOP_DEV"

# some impure staff
echo "# Created automatically" > $MNT_DIR/home/tenant/.zshrc

sync
umount $MNT_DIR
losetup -D

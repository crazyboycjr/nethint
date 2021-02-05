#!/bin/bash

# This script will create cpu_vm_base.img in the current directory,
# the cpu_vm_base.img can be used as a disk and boot by qemu

DISK_IMG=/tmp/cpu_vm_base.img

umount /mnt
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
mkfs.ext4 $LOOP_PART

# mount the partitions
mount $LOOP_PART /mnt

# debootstrap
apt install debootstrap -y
debootstrap --merged-usr --keyring=/usr/share/keyrings/ubuntu-archive-keyring.gpg --verbose focal /mnt http://archive.ubuntu.com/ubuntu/

# generate fstab
apt install arch-install-scripts -y
genfstab -U /mnt | grep -v swap >> /mnt/etc/fstab

# after command finish, chroot to that directory
cp /etc/apt/sources.list /mnt/etc/apt/sources.list

#mount -t proc /proc /mnt/proc
#mount --rbind /sys /mnt/sys
#mount --rbind /dev /mnt/dev
#mount --rbind /run /mnt/run
#cp /etc/resolv.conf /mnt/etc/resolv.conf
#chroot /mnt /bin/bash

cp ./cpu_vm_stage2.sh /mnt/root
arch-chroot /mnt /bin/bash /root/cpu_vm_stage2.sh $LOOP_DEV

sync
umount /mnt
losetup -D

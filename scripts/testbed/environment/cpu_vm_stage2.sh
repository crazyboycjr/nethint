#!/bin/bash

# copy this to the guest vm filesystem, e.g. /root, and execute it

# =================Second Stage======================
# after above command, we are able to enter the disk image
cd /root

[[ $# -ge 1 ]] && LOOP_DEV=$1 || LOOP_DEV=/dev/loop0

# check network connection
ping -c 1 8.8.8.8
ping -c 1 google.com

# no need to set keyboard because we use the default

# set hostname
echo cpuvm > /etc/hostname

# set timezone
ln -sf /usr/share/zoneinfo/America/New_York /etc/localtime

## set hardware clock sync
#hwclock --systohc
#
## set timedatectl
#timedatectl set-ntp true
#timedatectl set-timezone America/New_York

# generate locales
cat >> /etc/locale.gen <<EOF
en_US.UTF-8 UTF-8
zh_CN GB2312
zh_CN.GB18030 GB18030
zh_CN.GBK GBK
zh_CN.UTF-8 UTF-8
zh_HK BIG5-HKSCS
zh_HK.UTF-8 UTF-8
zh_SG GB2312
zh_SG.GBK GBK
zh_SG.UTF-8 UTF-8
zh_TW BIG5
zh_TW.EUC-TW EUC-TW
EOF

locale-gen

# update softwares
apt update
DEBIAN_FRONTEND=noninteractive apt dist-upgrade -y

# set root passwd
# stop here and set your password
echo -ne 'root\nroot\n' | passwd root

# setup kernel and bootloader, this should automatically install bootloader (grub-pc)
# but need user to choose the disk, so we install it manually later
KERN_VERSION="5.8.0-36-generic"
DEBIAN_FRONTEND=noninteractive apt install linux-image-$KERN_VERSION linux-headers-$KERN_VERSION -y

# change grub settings
mkdir -p /etc/default/grub.d/
cat > /etc/default/grub.d/50-grub-console.cfg <<EOF
# Set the recordfail timeout
GRUB_RECORDFAIL_TIMEOUT=0

# Set the default commandline
GRUB_CMDLINE_LINUX_DEFAULT="console=tty1 console=ttyS0,115200n8 nosplash"

# Set the grub console type
GRUB_TERMINAL=serial
GRUB_SERIAL_COMMAND="serial --speed=115200 --unit=0 --word=8 --parity=no --stop=1"
EOF
cat > /etc/default/grub.d/51-no-os-prober.cfg <<EOF
GRUB_DISABLE_OS_PROBER=true
EOF

update-grub
grub-install --boot-directory=/boot --recheck --target=i386-pc $LOOP_DEV

# create user and install tools
apt install wget -y
wget https://cjr.host/download/config/deploy_maas_master.tar
mkdir deploy_maas
tar xf deploy_maas_master.tar -C deploy_maas
cd deploy_maas
sed -i 's/NEW_USER=cjr/NEW_USER=tenant/g' deploy.sh
echo 'tenant' > passfile

# let's start install
./deploy.sh
# MLNX_OFED for Ubuntu should be installed with the following flags in chroot environment:
# ./install_ofed.sh --without-dkms --add-kernel-support --kernel $KERN_VERSION --without-fw-update --force
# ./install_ofed.sh --without-dkms --kernel $KERN_VERSION --without-fw-update --force
# they are not working, so install later in the VM, with only --force option.

# remember to enable systemd-networkd.service
mkdir -p /etc/systemd/system
mkdir -p /etc/systemd/system/network-online.target.wants
mkdir -p /etc/systemd/system/sockets.target.wants
ln -sf /lib/systemd/system/systemd-networkd.service /etc/systemd/system/dbus-org.freedesktop.network1.service
ln -sf /lib/systemd/system/systemd-networkd.service /etc/systemd/system/multi-user.target.wants/systemd-networkd.service
ln -sf /lib/systemd/system/systemd-networkd.socket /etc/systemd/system/sockets.target.wants/systemd-networkd.socket
ln -sf /lib/systemd/system/systemd-networkd-wait-online.service /etc/systemd/system/network-online.target.wants/systemd-networkd-wait-online.service

# also enable tmpfs at /tmp
mkdir -p /etc/systemd/system/local-fs.target.wants
cp /usr/share/systemd/tmp.mount /etc/systemd/system/
ln -sf /etc/systemd/system/tmp.mount /etc/systemd/system/local-fs.target.wants/tmp.mount

cat > /etc/systemd/network/20-mgmt.network <<EOF
[Match]
Name=enp1s0

[Network]
DHCP=ipv4
EOF


# sync the disk
sync

exit

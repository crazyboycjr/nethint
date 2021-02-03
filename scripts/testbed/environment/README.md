# Base Image Preparation

Scripts to prepare a CPU base image file disk.

```bash
sudo ./cpu_vm_stage1.sh
#   at the end of stage1, the script will mount the guest filesystem at
# /mnt, and copy the stage2 script to /mnt/root, and chroot to that and
# execute the stage2.sh.
```

To install and use the VM, use libvirt. First copy the `cpu_vm_base.img` to 
`/var/lib/libvirt/images/cpu_vm_base.img`. Then run virt-install. The
example below create a AWS `m4.xlarge` like CPU machine, except that the
network uses RDMA SR-IOV. Remove the `--print-xml` to actually install
the profile to libvirt.
```bash
# you may finish the install without graphics
virt-install --virt-type kvm --name cpubase --vcpus 4 --ram 16384 --boot hd --disk /var/lib/libvirt/images/cpu_vm_base.img,format=raw --network network=default --hostdev=pci_0000_18_00_1 --nographic --os-type=linux --os-variant=ubuntu20.04 --print-xml

# or with graphics
virt-install --virt-type kvm --name cpubase --vcpus 4 --ram 16384 --boot hd --disk /var/lib/libvirt/images/cpu_vm_base.img,format=raw --network network=default --hostdev=pci_0000_18_00_1 --graphic vnc,listen=0.0.0.0 --os-type=linux --os-variant=ubuntu20.04 --print-xml
```


To simply the configuration, I use default network created by libvirt to
allow internet access (SNAT). But this would now allow the VMs to reach
each other. To allow this, configure correct IP addresses for the rdma
interface and use that for interconnection. The switch has already been
configuration to support this.


A bunch of things I decide to setup later after all VMs have been booted are that
1. add many sshkeys to these VMs
2. generate a script for each VM to bring up and set different IP 
   address for the rdma interface.
3. On every boot, attach VFs to corresponding VMs by using virsh 
   attach-device, this gives more flexibility.
4. configure the name address resolution in both guests and hosts to
   allow easy access by sth like rdma0.cpu5


Then we are done!


### Notes
A couple of things that has to be check for each reboot of physical
server (aka what is not persistent).
1. `enable_sriov.sh`
2. `enable_eswitch.sh`
3. `setup_ovs.sh`

The order is important.

To disable eswtich and recovery the configuation.
1. `echo legacy | sudo tee /sys/class/net/rdma0/compat/devlink/mode`
2. `enable_sriov.sh`
3. `echo 0 | sudo tee /sys/class/net/rdma0/device/sriov_numvfs` (optionally)

#!/bin/bash


for i in {0..6}; do
	ssh danyang-0$i 'sudo mlnx_qos -i rdma0 -r 0,0,0,0,0,0,0,0'
done



read -d '' cmds <<EOF
"enable"
"config terminal"

"interface ethernet 1/4 no bandwidth shape  "
"interface ethernet 1/6 no bandwidth shape  "
"interface ethernet 1/8 no bandwidth shape  "
"interface ethernet 1/12 no bandwidth shape "
"interface ethernet 1/14 no bandwidth shape "
"interface ethernet 1/16 no bandwidth shape "

"interface ethernet 1/2 no bandwidth shape  "
"interface ethernet 1/10 no bandwidth shape "
EOF

ssh -oKexAlgorithms=+diffie-hellman-group14-sha1 danyang@danyang-mellanox-switch.cs.duke.edu cli -h $cmds

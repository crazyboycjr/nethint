#!/bin/bash

check_args() {
	[[ $# -eq 1 ]] && [[ $1 = "PerFlowMaxMin" || $1 = "TenantFlowMaxMin" ]]
}

usage_exit() {
	echo "Usage: $0 [fairness]    fairness in [PerFlowMaxMin, TenantFlowMaxMin]" && exit 1
}

check_args $* || usage_exit

fairness=$1

echo switching to $fairness

sed -i 's/^fairness = "\(.*\)"/Fairness = "\1"/' *.toml
sed -i "s/^# fairness = \"${fairness}\"/fairness = \"${fairness}\"/" *.toml
sed -i 's/^Fairness = "\(.*\)"/# fairness = "\1"/' *.toml

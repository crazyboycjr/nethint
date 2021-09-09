import Text.Printf (printf)

numRacks = 1000
numMachinesEachRack = 20
numVMsEachMachine = 10

rackBandwidth = 600 * 1e9 -- Gbps
-- machineBandwidth = 100 -- Gbps

nethintPeriodInSec = 0.1 -- 100ms

numVirtualLinksEachRack :: Float
numVirtualLinksEachRack = 2 * (1 + numMachinesEachRack * numVMsEachMachine)
-- 2 * (1 ToR switch + a bunch of VMs)
-- 2 because of there are a upstream link and a downstream link

numVirtualLinks :: Float
numVirtualLinks = numVirtualLinksEachRack * numRacks

-- in bytes
nBrPairSize :: Float
nBrPairSize = 2 * 8

-- in bytes
virtualLinkIDSize :: Float
virtualLinkIDSize = 8

-- one virtualLinkID, 8 bytes
-- two (n, Br) pairs, one for traffic within the rack, and the other for traffic contributes to the cross rack link
virtualLinkBytes :: Float
virtualLinkBytes = virtualLinkIDSize + 2 * nBrPairSize

-- for each rack, it has to send and receive so much information
-- note that the `numVirtualLinks` contains all the virtual links in the data center
crossRackTrafficBytes :: Float -- bytes
crossRackTrafficBytes = numVirtualLinks * virtualLinkBytes

crossRackTrafficPerSecond :: Float -- bits/second
crossRackTrafficPerSecond = 8 * crossRackTrafficBytes / nethintPeriodInSec

computeBandwidthOverhead :: Float
computeBandwidthOverhead = crossRackTrafficPerSecond / rackBandwidth


showMB :: Float -> String
showMB bytes = printf "%fMB" (bytes / 1e6)

main = do
    let percentage = computeBandwidthOverhead
    printf "%.5f%%\n" (percentage * 100)

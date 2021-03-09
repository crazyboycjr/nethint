import sys
import random

filename = sys.argv[1]
num = int(sys.argv[2])
size = int(sys.argv[3])
print ("original hostfile: ", filename)
print ("num of hostfiles to generate: ", num)
print ("num of vms per job: ", size)

ips = []
f = open(filename,'r')
for line in f.readlines():
  ips.append(line)
f.close()

folder = "hostfiles"
if folder not in os.listdir():
  os.mkdir(folder)

for i in range(0, num):
  f = open(os.path.join(folder, str(i)),'w')
  a = random.sample(ips, size)
  for line in a:
    f.write(line)
  f.close()

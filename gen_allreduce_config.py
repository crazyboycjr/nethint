import os, sys
import random

def create_toml(folder, job_id):
  f = open(os.path.join(folder, "%d.toml" % job_id), 'w')
  f.write("job_id = %d\n" % job_id)
  f.write("job_size_distribution = [[40, 4], [80, 8], [90, 16]]\n")
  f.write("buffer_size = 1000_000_000\n")
  f.write("num_iterations = 1000\n")
  f.write("poisson_lambda = 240_000_000_000.0\n")

  f.write("seed_base = 1\n")
  f.write("traffic_scale = 1\n")

  f.write("allreduce_policy = \"Random\"\n")
  f.write("probe = { enable = false }\n")
  f.write("nethint_level = 0\n")
  f.write("auto_tune = 10\n")

  f.close()

folder = "allreduce_tomls"
if folder not in os.listdir():
  os.mkdir(folder)

for job_id in range(0, 10):
  create_toml(folder, job_id)
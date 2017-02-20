#!/usr/bin/python

import numpy as np
import sys

def findcdf(data):
  cdf = []
  for i in xrange(0,100):
    cdf.append(np.percentile(data,int(i)))
  return cdf

data_file = sys.argv[1]
data = np.array([float(line.rstrip('\n')) for line in open(data_file)])
cdf = findcdf(data)

out_file = data_file + '.cdf'
out = open(out_file, 'w')
for xy in zip(cdf, xrange(0,100)):
	out.write('%lf\t%lf\n' % xy)
out.close()



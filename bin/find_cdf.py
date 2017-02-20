#!/usr/bin/python

import numpy as np
import sys

data_file = sys.argv[1]

xvals = np.array([float(line.rstrip('\n')) for line in open(data_file)])
yvals = np.arange(len(xvals))/float(len(xvals))

out_file = data_file + '.cdf'
out = open(out_file, 'w')
for (x, y) in zip(xvals, yvals):
	out.write(x + '\t' + y + '\n')
out.close()



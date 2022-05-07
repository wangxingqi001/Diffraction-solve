import sys
import os
import json
from ConfigParser import ConfigParser
import re
import time
import numpy as np


def findnumber(string):
	return re.findall(r"\d+\.?\d*", string)


# ../utils.py has a copy
def compile_h5loc(loc, run_name):
	# %r means run name
	tmp = re.sub(r"%r", run_name, loc)
	# ...
	# tmp = ...
	return tmp

def avg_pooling(myarr, factor, ignoredim=0):
	# 3D data avg pooling, along the first dimension
	# crop edges if the shape is not a multiple of factor
	if ignoredim > 0: myarr = myarr.swapaxes(0,ignoredim)
	zs,ys,xs = myarr.shape
	crarr = myarr[:,:ys-(ys % int(factor)),:xs-(xs % int(factor))]
	dsarr = np.mean(np.concatenate([[crarr[:,i::factor,j::factor] 
		for i in range(factor)] 
		for j in range(factor)]), axis=0)
	if ignoredim > 0: dsarr = dsarr.swapaxes(0,ignoredim)
	return dsarr


def write_status(file, infos, values):
	# write status index defined in app_namespace.ini
	minlen = min(len(infos), len(values))
	with open(file, "w") as fp:
		for i in range(minlen):
			fp.write( "%s : %s\n" % (str(infos[i]), str(values[i])) )


def read_status(file):
	values = {}
	with open(file, "r") as fp:
		for line in fp.readlines():
			info, value = line.split(":")
			values[info.strip()] = value.strip()
	return values
from spipy.image import classify
from mpi4py import MPI
import sys
import os
import json
import h5py
from ConfigParser import ConfigParser
import glob
import time
import numpy as np

import scripts_utils as su

comm = MPI.COMM_WORLD
mpi_rank = comm.Get_rank()
mpi_size = comm.Get_size()

if __name__ == '__main__':
	"""
	python SpecFilter.py [runtime.json] [config.ini]
	* runtime.json should contain : 
		dataset : paths of input data files, list
		savepath : path (dir) for saving results, str
		run_name  : name of this run, str
		benchmark : True / False
		inh5     : path inside hdf5
		#mask    : mask file, .npy, .bin, .byt
	* output status format :
		(status = 1 : processing or 2 : finished)
		-> summary.txt
		status : xxx
		processed : xxx
		time : xxx
	* output h5 file format :
		'output.h5'
		|- features
		|- labels
	"""

	runtime_json = sys.argv[1]
	config_ini   = sys.argv[2]

	# start time
	if mpi_rank == 0:
		start_time = time.time()

	with open(runtime_json, 'r') as fp:
		runtime = json.load(fp)
	config = ConfigParser()
	config.read(config_ini)
	sec = config.sections()[0]

	# read runtime param
	data_files  = runtime['dataset']
	savepath    = runtime['savepath']
	run_name    = runtime['run_name']
	benchmark   = runtime['benchmark']
	inh5        = runtime['inh5']
	if runtime.has_key('mask'):
		maskfile = runtime['mask']
	else:
		maskfile = ""

	if mpi_rank == 0:
		print("- Submit %d jobs for manifold / spectral clustering of %s." % (mpi_size, run_name))
		print("- Read config file %s." % os.path.split(config_ini)[-1])

	# read config
	method = config.get(sec, "method").split(',')[1].strip()
	low_cut_percent = config.getfloat(sec, "low_cut_percent")
	components = config.getint(sec, "components")
	group_size = config.getint(sec, "group_size")
	njobs = config.getint(sec, "njobs")
	if mpi_rank == 0:
		print("- Using method %s" % method)

	# read data
	try:
		fp = h5py.File(data_files[0], 'r')
		num_patterns = fp[inh5].shape[0]
		databin = np.linspace(0, num_patterns, mpi_size+1, dtype=int)
		data = fp[inh5][databin[mpi_rank]:databin[mpi_rank+1], :, :]
		fp.close()
	except Exception as err:
		MPI.Finalize()
		raise RuntimeError(str(err))

	# read mask
	if maskfile is None or len(maskfile) == 0:
		mask = None
	elif os.path.splitext(maskfile)[-1] == ".npy":
		mask = np.load(maskfile)
	elif os.path.splitext(maskfile)[-1] == ".bin" or os.path.splitext(maskfile)[-1] == ".byt":
		mask = np.fromfile(maskfile, dtype="uint8")
	else:
		MPI.Finalize()
		raise RuntimeError("Error in loading mask.")

	if method.lower() == "lle":
		LLE_method = config.get(sec, "LLE_method").split(',')[1].strip()
		LLE_neighbors = config.getint(sec, "LLE_neighbors")
		d_comp, predict = classify.cluster_fSpec(data, mask=mask, low_filter=low_cut_percent, \
			decomposition='LLE', ncomponent=components, nneighbors=LLE_neighbors, LLEmethod=LLE_method, \
			clustering=2, njobs=njobs, verbose=False)
	else:
		d_comp, predict = classify.cluster_fSpec(data, mask=mask, low_filter=low_cut_percent, \
			decomposition=method, ncomponent=components, clustering=2, njobs=njobs, verbose=False)

	if mpi_rank == 0:
		print("- Writing results.")
		fp = h5py.File(os.path.join(savepath, "output.h5"), 'w')
		fp.create_dataset("features", data=np.zeros([num_patterns, components]), chunks=True, compression="gzip")
		fp.create_dataset("labels", data=np.zeros(num_patterns), chunks=True, compression="gzip")
		fp.close()

	comm.Barrier()

	# save
	fp = None
	while fp is None:
		try:
			fp = h5py.File(os.path.join(savepath, "output.h5"), 'a')
		except:
			fp = None
			
	fp['features'][databin[mpi_rank]:databin[mpi_rank+1],:] = d_comp
	fp['labels'][databin[mpi_rank]:databin[mpi_rank+1]] = predict
	fp.close()

	if mpi_rank == 0:
		# write summary
		end_time = time.time()
		status_file = os.path.join(savepath, "status/summary.txt")
		su.write_status(status_file, ["status", "processed", "time"], \
					[2, num_patterns, end_time - start_time])
		print("- Finished.")


	MPI.Finalize()









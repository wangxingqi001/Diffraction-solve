import spipy
from mpi4py import MPI
import sys
import os
import json
import h5py
from ConfigParser import ConfigParser
import re
import time
import numpy as np

import scripts_utils as su

comm = MPI.COMM_WORLD
mpi_rank = comm.Get_rank()
mpi_size = comm.Get_size()


if __name__ == '__main__':
	"""
	python Adu2photon.py [runtime.json] [config.ini]
	* runtime.json should contain : 
		dataset : paths of input data files, list
		savepath : path (dir) for saving results, str
		run_name  : name of this run, str
	* output status format :
		(status = 1 : processing or 2 : finished)
		-> summary.txt
		status : xxx
		processed : xxx
		time : xxx
		-> status_%{rank}.txt
		status : xxx
		processed : xxx
	* output h5 file format :
		|- PhotonCount
			|- data
			|- adu_per_photon
			|- rawpath
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
	data_files = runtime['dataset']
	savepath   = runtime['savepath']
	run_name   = runtime['run_name']

	if mpi_rank == 0:
		print("- Submit %d jobs for adu2photon of %s." % (mpi_size, run_name))
		print("- Read config file %s." % os.path.split(config_ini)[-1])

	inh5          = su.compile_h5loc(config.get(sec, 'data-path in cxi/h5'), run_name)
	force_poisson = config.getint(sec, 'force poisson')
	appending     = config.getint(sec, 'append to input h5')
	aduperphoton  = float(config.get(sec, 'adu-per-photon'))
	photon_percent= config.getfloat(sec, 'photon percent')
	mask_file     = config.get(sec, 'mask (.npy)')

	if os.path.exists(mask_file):
		mask = np.load(mask_file)
	else:
		mask = None

	if mpi_rank == 0:
		if mask is None:
			print("- Mask file is not given (correctly).")
		else:
			print("- Mask file %s is loaded." % mask_file)
		if aduperphoton > 1:
			print("- Adu-per-photon is set as %f, ignore photon percent." % aduperphoton)
		else:
			print("- Photon percent is set as %f." % photon_percent)

	# status
	status_file = os.path.join(savepath, "status/status_%d.txt" % mpi_rank)
	su.write_status(status_file, ["status", "processed"], [1, 0])

	# processing
	num_processed = 0

	for df in data_files:

		if mpi_rank == 0:
			print("- Processing %s ..." % df)

		# load data
		fp = h5py.File(df, 'r')
		num_patterns, sx, sy = fp[inh5].shape
		adus = {}
		newpat = {}

		for i in range(num_patterns):

			if i % mpi_size != mpi_rank:
					continue

			if mask is not None:
				pat = fp[inh5][i] * (1 - mask)
			else:
				pat = fp[inh5][i]
			pat[pat<0] = 0

			if aduperphoton > 1:
				adus[i] = aduperphoton
				newpat[i] = spipy.image.preprocess._transfer(np.array([pat]), 0, aduperphoton, force_poisson)[0]
			else:
				no_photon_percent = 1 - photon_percent
				countp = np.bincount(np.round(pat.ravel()).astype(int))
				if mask is not None:
					countp[0] = countp[0] - len(np.where(mask==1))
				sumc = np.cumsum(countp)
				percentc = sumc/sumc[-1].astype(float)
				try:
					adu = np.where(np.abs(percentc - no_photon_percent)<0.1)[0][0]
				except:
					adu = np.where((percentc - no_photon_percent)>=0)[0][0]
					
				if adu < 1.0:
					adu = 1.0
				adus[i] = adu
				newpat[i] = spipy.image.preprocess._transfer(np.array([pat]), 0, adu, force_poisson)[0]

			num_processed += 1
			su.write_status(status_file, ["status", "processed"], [1, num_processed])

		fp.close()

		# barrier
		comm.Barrier()
		adus_gather = comm.gather(adus, root=0)
		newpat_gather = comm.gather(newpat, root=0)

		# write to file
		if mpi_rank == 0:

			# print status
			print("- Write results of %s." % os.path.split(df)[-1])

			adus = np.zeros(num_patterns)
			newpats = np.zeros((num_patterns, sx, sy), dtype=int)
			for tmp in adus_gather:
				adus[tmp.keys()] = tmp.values()
			for tmp in newpat_gather:
				newpats[tmp.keys()] = tmp.values()

			# save
			if not appending:
				this_rank_file = os.path.splitext(os.path.split(df)[-1])[0]
				save_file = os.path.join(savepath, this_rank_file + ".ptc.h5")
				sfp = h5py.File(save_file, 'w')
				sfp.create_dataset("PhotonCount/data", data=newpats, chunks=True, compression="gzip")
				sfp.create_dataset("PhotonCount/adu_per_photon", data=adus, chunks=True, compression="gzip")
				sfp.create_dataset("PhotonCount/rawpath", data=df)
				sfp.close()
				print("- Save results to %s." % save_file)
			else:
				sfp = h5py.File(df, 'a')
				if "PhotonCount/data" in sfp:
					del sfp["PhotonCount/data"]
				if "PhotonCount/adu_per_photon" in sfp:
					del sfp["PhotonCount/adu_per_photon"]
				sfp.create_dataset("PhotonCount/data", data=newpats, chunks=True, compression="gzip")
				sfp.create_dataset("PhotonCount/adu_per_photon", data=adus, chunks=True, compression="gzip")
				sfp.close()
				os.symlink(df, os.path.join(savepath, "photonCount.link.h5"))
				print("- Append results to %s." % df)


	num_processed_gather = comm.gather(num_processed, root = 0)

	if mpi_rank == 0:
			# end time
			end_time = time.time()
			status_file = os.path.join(savepath, "status/summary.txt")
			su.write_status(status_file, ["status", "processed", "time"], \
				[2, sum(num_processed_gather), end_time - start_time])

			print("- Finish.")



	MPI.Finalize()




































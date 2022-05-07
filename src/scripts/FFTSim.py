from spipy.simulate import sim
from mpi4py import MPI
import sys
import os
import json
import h5py
from ConfigParser import ConfigParser
import glob
import time
import numpy as np
import subprocess

import scripts_utils as su

comm = MPI.COMM_WORLD
mpi_rank = comm.Get_rank()
mpi_size = comm.Get_size()


if __name__ == '__main__':
	"""
	python FFTSim.py [runtime.json] [config.ini]
	* runtime.json should contain : 
		dataset : paths of input data files, list
		savepath : path (dir) for saving results, str
		run_name  : name of this run, str
		num_pattern : number of simulated patterns
	* output status format :
		(status = 1 : processing or 2 : finished)
		-> summary.txt
		status : xxx
		processed : xxx
		time : xxx
	* project structure :
		|- runname.tag.remarks (dir)
			|- status (dir)
				|- summary.txt
			|- runtime.json
			|- config.ini
			|- runname (dir)
				|- input
					|- xxx.pdb
				|- output
					|- xxx.h5
				|- xxx.h5 --> output/xxx.h5
				|- ... (config & log)
	* simulation output (h5) structure
		|- detector mapping
		|- electron density
		|- patterns
		|- quaternions
		|- scattering intensity
	"""

	runtime_json = sys.argv[1]
	config_ini   = sys.argv[2]

	# don't support multi-processing
	if mpi_size > 1:
		MPI.Finalize()
		raise RuntimeError("FFT simulation only support 1 mpi rank !")

	if mpi_rank == 0:
		start_time = time.time()

		with open(runtime_json, 'r') as fp:
			runtime = json.load(fp)
		savepath = runtime['savepath']
		config = ConfigParser()
		config.read(config_ini)
		sec = config.sections()[0]

		# read runtime param
		data_files  = runtime['dataset']
		savepath    = runtime['savepath']
		run_name    = runtime['run_name']
		num_pattern = runtime['num_pattern']

		print("- Submit %d jobs for FFT simulation of %s." % (mpi_size, run_name))
		print("- Read config file %s." % os.path.split(config_ini)[-1])

		# read config
		pdbfile = data_files[0]
		config_param = {'parameters|detd' : config.getfloat(sec, "detd"), \
						'parameters|lambda' : config.getfloat(sec, "lambda"), \
						'parameters|detsize' : config.getint(sec, "det_size"), \
						'parameters|pixsize' : config.getfloat(sec, "pix_size"), \
						'parameters|stoprad' : config.getint(sec, "beam_stop"), \
						'parameters|polarization' : 'x', \
						'make_data|num_data' : num_pattern, \
						'make_data|fluence' : config.getfloat(sec, "fluence") * 1e14 }

		# mask
		if os.path.isfile(config.get(sec, "mask")):
			mask = np.load(config.get(sec, "mask"))
			print("- Load mask file.")
			if mask.shape[0] != mask.shape[1] or mask.shape[0] != config_param['parameters|detsize']:
				MPI.Finalize()
				raise RuntimeError("Mask shape is not consistent with input detector size !")
		else:
			print("- No mask provided.")
			mask = None


		# construct project
		sim.generate_config_files(pdb_file=pdbfile, workpath=savepath, name=run_name, params=config_param)

		# run simulaion
		sim.run_simulation(skip_check = True)

		# apply mask
		if mask is not None:
			print("- Applying mask ...")
			outputdata = glob.glob(os.path.join(savepath, run_name, "output", "*.h5"))[0]
			fp = h5py.File(outputdata, 'a')
			for i in range(fp['patterns'].shape[0]):
				fp['patterns'][i] = fp['patterns'][i] * (1-mask)
			fp.close()

		# make soft link
		output_datah5 = glob.glob(os.path.join(savepath, run_name, "output", "*.h5"))[0]
		cmd = "cd %s;ln -fs ./%s/output/*.h5 spipy_fft_simulation.h5" % (savepath, run_name)
		subprocess.check_call(cmd, shell=True)

		# write summary
		end_time = time.time()
		status_file = os.path.join(savepath, "status/summary.txt")
		su.write_status(status_file, ["status", "processed", "time"], \
					[2, num_pattern, end_time - start_time])
		print("- Simulation done.")





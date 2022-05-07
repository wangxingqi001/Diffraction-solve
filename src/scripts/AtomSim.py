from spipy.simulate import sim_adu
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
	python AtomSim.py [runtime.json] [config.ini]
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
	* output h5 file format :
		|- simu_parameters
			|- ...
		|- rotation_order
		|- patterns
		|- oversampling_rate
		|- euler_angles
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
	num_pattern = runtime['num_pattern']

	if mpi_rank == 0:
		print("- Submit %d jobs for atom-scattering simulation of %s." % (mpi_size, run_name))
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
					'make_data|fluence' : config.getfloat(sec, "fluence") * 1e14, \
					'make_data|scatter_factor' : False, \
					'make_data|ram_first' : False, \
					'make_data|poisson' : False }
	if config.getint(sec, "scatter_f") > 0:
		config_param['make_data|scatter_factor'] = True
	if config.getint(sec, "ram_first") > 0:
		config_param['make_data|ram_first'] = True
	if config.getint(sec, "poisson") > 0:
		config_param['make_data|poisson'] = True

	# mask
	if os.path.isfile(config.get(sec, "mask")):
		mask = np.load(config.get(sec, "mask"))
		print("- Load mask file.")
		if mask.shape[0] != mask.shape[1] or mask.shape[0] != config_param['parameters|detsize']:
			raise RuntimeError("Mask shape is not consistent with input detector size !")
	else:
		print("- No mask provided.")
		mask = None
		
	rot_order = config.get(sec, "rot_order")
	euler_type = config.get(sec, "euler_type")
	if euler_type.lower() == "predefined":
		euler = np.loadtxt(config.get(sec, "euler_pred_file"))
	else:
		euler = None

	# prepare euler angles
	euler_range = np.array([[0, np.pi * 2.0], [0, np.pi], [0, np.pi * 2.0]])

	if mpi_rank == 0:
		print("- Configuration done.")
	comm.Barrier()

	# simulation
	if mpi_size > 1:
		sim_adu.multi_process(save_dir=savepath, pdb_file=pdbfile, param=config_param, \
			euler_mode=euler_type, euler_order=rot_order, euler_range=euler_range, \
			predefined=euler, verbose=True)
	else:
		sim_adu.single_process(pdb_file=pdbfile, param=config_param, euler_mode=euler_type, \
        	euler_order=rot_order, euler_range=euler_range, predefined=euler, \
        	save_dir=savepath, verbose=True)

	if mpi_rank == 0:
		# apply mask
		print("\n")
		print("- Applying mask ...")
		outputdata = glob.glob(os.path.join(savepath, "spipy_adu_*.h5"))[0]
		fp = h5py.File(outputdata, 'a')
		for i in range(fp['patterns'].shape[0]):
			fp['patterns'][i] = fp['patterns'][i] * (1-mask)
		fp.close()

		# write summary
		end_time = time.time()
		status_file = os.path.join(savepath, "status/summary.txt")
		su.write_status(status_file, ["status", "processed", "time"], \
					[2, num_pattern, end_time - start_time])
		print("- Simulation done.")


	MPI.Finalize()








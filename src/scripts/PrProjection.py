import spipy.phase as phr
import sys
import os
import json
from ConfigParser import ConfigParser
import time
import numpy as np
import subprocess

import scripts_utils as su
from mpi4py import MPI

comm = MPI.COMM_WORLD
mpi_rank = comm.Get_rank()
mpi_size = comm.Get_size()

if __name__ == '__main__':
	'''
	python PrProjection.py [runtime.json] [config.ini]
	* runtime.json should contain : 
		dataset  : path of input data file, list with 1 element
		savepath : path (dir) for saving results, str
		run_name : name of this run, str
	* output status format :
		(status = 1 : processing or 2 : finished)
		-> summary.txt
		status : xxx
		time : xxx
	* project structure :
		|- runname.tag.remarks (dir)
			|- status (dir)
				|- summary.txt
			|- runtime.json
			|- config.ini
			|- runname (dir)
				|- ... (phasing input/output)
	'''

	runtime_json = sys.argv[1]
	config_ini   = sys.argv[2]

	if mpi_rank == 0:
		start_time = time.time()

	with open(runtime_json, 'r') as fp:
		runtime = json.load(fp)
	savepath = runtime['savepath']
	config = ConfigParser()
	config.read(config_ini)
	sec = config.sections()[0]

	if mpi_rank == 0:
		print("- Submit %d jobs for phasing of %s." % (mpi_size, runtime['run_name']))
		print("- Read config file %s." % os.path.split(config_ini)[-1])

	# 2 or 3
	proj_type = int(su.findnumber(config.get(sec, "dtype"))[-1])
	mask_file = config.get(sec, "mask")
	if not os.path.isfile(mask_file):
		mask_file = None

	# check dataset
	dataset_yes = False
	dsize = None
	if mpi_rank == 0:
		inputf = runtime['dataset'][0]
		if os.path.splitext(inputf)[-1] == ".npy":
			inputd = np.load(inputf)
			if len(inputd.shape) != proj_type:
				dataset_yes = False
		elif os.path.splitext(inputf)[-1] == ".bin":
			inputd = np.fromfile(inputf, dtype=float)
			ssize = int(np.round(len(inputd)**(1.0/proj_type)))
			if ssize**proj_type != len(inputd):
				dataset_yes = False
			inputd = inputd.reshape([ssize]*proj_type)
		elif os.path.splitext(inputf)[-1] == ".mat":
			inputd = sio.loadmat(inputf)
			inputd = inputd.values()[0]
			if len(inputd.shape) != proj_type:
				dataset_yes = False
		else:
			dataset_yes = False
		dsize = list(inputd.shape)
		dataset_yes = True

	# bcast data size
	dataset_yes, dsize = comm.bcast([dataset_yes, dsize], root=0)

	if not dataset_yes:
		if mpi_rank == 0:
			raise RuntimeError("Input dataset dimension mismatch !")
		MPI.Finalize()
		sys.exit(1)
	if proj_type == 2:
		input_shape = "%d,%d" % (dsize[0], dsize[1])
	else:
		input_shape = "%d,%d,%d" % (dsize[0], dsize[1], dsize[2])

	# prepare parameters
	params_essential = {'input|shape' : input_shape, 'input|padd_to_pow2' : False, \
		'input|inner_mask' : config.getint(sec, "inner_mask"), 'input|outer_mask' : config.getint(sec, "outer_mask"), \
		'input|outer_outer_mask' : config.getint(sec, "o_o_mask"), 'input|mask_edges' : True, \
		'phasing|repeats' : config.getint(sec, "repeat"), \
		'phasing|iters' : '%d%s %d%s %d%s' % \
							(config.getint(sec, "iter_num_1"), config.get(sec, "iter_type_1").split(',')[-1], \
							 config.getint(sec, "iter_num_2"), config.get(sec, "iter_type_2").split(',')[-1], \
							 config.getint(sec, "iter_num_3"), config.get(sec, "iter_type_3").split(',')[-1]), \
		'phasing_parameters|beta' : config.getfloat(sec, "beta")}
	if proj_type == 2:
		params_essential['phasing_parameters|support_size'] = config.getint(sec, "support")
	else:
		params_essential['phasing_parameters|voxel_number'] = config.getint(sec, "support")
	params_optional = {'input|subtract_percentile' : None, 'input|spherical_support' : None, \
		'phasing_parameters|background' : True, 'input|init_model' : config.get(sec, "start_model")}

	if params_essential['input|inner_mask'] == 0:
		params_essential['input|inner_mask'] = None
	if params_essential['input|outer_mask'] == 0:
		params_essential['input|outer_mask'] = None
	if params_essential['input|outer_outer_mask'] == 0:
		params_essential['input|outer_outer_mask'] = None
	if not os.path.isfile(params_optional['input|init_model']):
		params_optional['input|init_model'] = None
	parameters = dict(params_essential, **params_optional)

	# create project
	if mpi_rank == 0:

		try:
			if proj_type == 2:
				phr.phase2d.new_project(data_mask_path=[runtime['dataset'][0],mask_file], path=runtime['savepath'], name=runtime['run_name'])
				phr.phase2d.config(params = parameters)
			else:
				phr.phase3d.new_project(data_path=runtime['dataset'][0],mask_path = mask_file, path=runtime['savepath'], name=runtime['run_name'])
				phr.phase3d.config(params = parameters)
		except Exception as err:
			MPI.Finalize()
			raise RuntimeError(str(err))


	# add source code path to system
	# compatible to spipy 'version 2.x' project structure
	if proj_type == 2:
		sys.path.append(os.path.join(os.path.dirname(phr.phase2d.__file__), "template_2d"))
	else:
		sys.path.append(os.path.join(os.path.dirname(phr.phase3d.__file__), "template_3d"))

	import phase

	# make input
	if mpi_rank == 0:

		try:
			import make_input
			if proj_type == 2:
				make_input.make_input(os.path.join(phr.phase2d._workpath,'config.ini'))
			else:
				make_input.make_input(os.path.join(phr.phase3d._workpath,'config.ini'))
		except Exception as err:
			MPI.Finalize()
			raise RuntimeError(str(err))

	# barrier
	comm.barrier()

	# do phasing
	if proj_type == 2:
		inputh5 = os.path.join(savepath, runtime['run_name'], "input.h5")
	else:
		inputh5 = os.path.join(savepath, runtime['run_name'], "input.h5")

	diff, support, good_pix, sample_known, params = phase.utils.io_utils.read_input_h5(inputh5)

	out = phase.phase(diff, support, params, good_pix = good_pix, sample_known = sample_known)

	out = phase.out_merge(out, diff, good_pix)

	
	if mpi_rank == 0:

		# write h5 output
		phase.utils.io_utils.write_output_h5(\
			params['output']['path'], diff, out['I'], support, out['support'], \
			good_pix, sample_known, out['O'], out['eMod'], out['eCon'], None,   \
			out['PRTF'], out['PRTF_rav'], out['PSD'], out['PSD_I'], out['B_rav'])
		print("\nDone ! Phasing result is stored in " + params['output']['path'] + '/output.h5\n')

		# write summary
		end_time = time.time()
		status_file = os.path.join(savepath, "status/summary.txt")
		su.write_status(status_file, ["status", "time"], \
					[2, end_time - start_time])

	MPI.Finalize()






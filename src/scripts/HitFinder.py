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
	python HitFinder.py [runtime.json] [config.ini]
	* runtime.json should contain : 
		dataset : paths of input data files, list
		darkcal : path of dark calibration, str
		savepath : path (dir) for saving results, str
		run_name  : name of this run, str
	* output status format :
		(status = 1 : processing or 2 : finished)
		-> summary.txt
		status : xxx
		processed : xxx
		hits : xxx
		time : xxx
		-> status_%{rank}.txt
		status : xxx
		processed : xxx
		hits : xxx
		-> summary.txt (darkcal)
		status : xxx
		time : xxx
		-> status_%{rank}.txt (darkcal)
		status : xxx
	* output h5 file format :
		|- Hits
			|- data
			|- index
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

	if sec == "darkcal":

		if mpi_rank == 0:

			# read runtime param
			data_files = runtime['dataset']

			# read runtime param
			savepath   = runtime['savepath']
			run_name   = runtime['run_name']

			# this is dark calibration
			inh5 = su.compile_h5loc(config.get(sec, 'inh5'), run_name)
			mask_thres = int(config.get(sec, 'mask_thres'))

			status_file = os.path.join(savepath, "status/status_0.txt")
			su.write_status(status_file, ["status"], [1])

			# # calibration
			meanBg = None
			num_patterns = 0
			for ind, df in enumerate(data_files):
				fp = h5py.File(df, 'r')
				if meanBg is None:
					meanBg = np.sum(fp[inh5], axis=0)
				else:
					meanBg += np.sum(fp[inh5], axis=0)
				num_patterns += fp[inh5].shape[0]
				fp.close()
			meanBg /= num_patterns

			bad_points = np.where(np.isnan(meanBg) & np.isinf(meanBg))
			meanBg[bad_points] = 0

			# mask bg
			center = spipy.analyse.saxs.friedel_search(meanBg, np.array(meanBg.shape)/2, None, 10, 50)
			meanBg_Iq = spipy.image.radp.radial_profile_2d(meanBg, center)

			maskBg = np.zeros(meanBg.shape, dtype=int)
			meshgrids = np.indices(meanBg.shape)
			rinfo = np.sqrt(sum( ((grid - c)**2 for grid, c in zip(meshgrids, center)) ))
			rinfo = np.round(rinfo).astype(np.int)

			for r, I, std in meanBg_Iq:
				maskBg[(rinfo == r) & (meanBg > I+std*mask_thres)] = 1
			maskBg[bad_points] = 1

			# save file
			save_file = os.path.join(savepath, run_name+".darkcal.h5")
			fp = h5py.File(save_file, 'w')
			fp.create_dataset("mask", data=maskBg)
			fp.create_dataset("bg", data=meanBg)
			fp.close()

			# end time
			end_time = time.time()

			status_file = os.path.join(savepath, "status/summary.txt")
			su.write_status(status_file, ["status", "time"], [2, end_time - start_time])


	else:

		# read runtime param
		data_files = runtime['dataset']
		savepath   = runtime['savepath']
		run_name   = runtime['run_name']
		# read config param
		inh5    = su.compile_h5loc(config.get(sec, 'data-path in cxi/h5'), run_name)
		roi     = su.findnumber(config.get(sec, 'roi radii range'))
		chi_cut = config.getint(sec, 'chi-square cut-off')
		save_hits  = config.getint(sec, 'save hits')
		if save_hits > 0:
				downsampling = config.getint(sec, 'downsampling')

		status_file = os.path.join(savepath, "status/status_%d.txt" % mpi_rank)
		su.write_status(status_file, ["status", "processed", "hits"], [1, 0, 0])

		# pre-define
		darkbg = None
		mask = None
		radii_range = None

		# broadcast data
		if mpi_rank == 0:

			# read runtime param
			dark_file  = runtime['darkcal']
			# read config param
			mask_file  = config.get(sec, 'mask (.npy)')

			print("- Submit %d jobs for hit-finding of %s." % (mpi_size, run_name))
			print("- Read config file %s." % os.path.split(config_ini)[-1])

			# load mask
			if os.path.exists(mask_file):
				user_mask = np.load(mask_file)
			else:
				user_mask = None
				print("- Mask file is not given.")

			# load darkcal
			if dark_file is not None:
				darkcal_fp = h5py.File(dark_file, 'r')
				darkmask = darkcal_fp['mask'][...]
				darkbg = darkcal_fp['bg'][...]
				darkcal_fp.close()
			else:
				print("- No dark calibration found, creating poisson background ...")
				fp = h5py.File(data_files[0], 'r')
				meanV = np.mean(fp[inh5])
				datashape = fp[inh5].shape[-2:]
				fp.close()
				darkbg = np.random.poisson(meanV, datashape)
				darkmask = np.zeros(datashape, dtype=int)

			# combine darkmask and user mask
			if user_mask is not None:
				mask = user_mask & darkmask
			else:
				mask = darkmask

			# center, radii_range
			center = np.array(mask.shape)/2
			radii_range = np.array([center[0], center[1], int(roi[0]), int(roi[1])])

			# prepare save file
			save_file = os.path.join(savepath, run_name+".h5.params")
			fp = h5py.File(save_file, 'w')
			grp2 = fp.create_group("Raw-Parameters")
			grp2.create_dataset("rawfiles", data=','.join(data_files))
			grp2.create_dataset("radii_range", data=radii_range)
			grp2.create_dataset("darkcal", data=dark_file)
			grp2.create_dataset("chi-square-cut", data=chi_cut)
			grp2.create_dataset("downsampling", data=downsampling)
			grp3 = fp.create_group("Middle-Output")
			grp3.create_dataset("mask", data=mask, chunks=True, compression="gzip")
			grp3.create_dataset("background", data=darkbg, chunks=True, compression="gzip")
			fp.close()
			# print log
			print("- Save parameters to %s." % save_file)

			'''
			ranki = 0
			for ind, dataf in enumerate(data_files):
				numdata = pat_num[ind]
				bins = np.linspace(0, numdata, job_num[ind]+1, dtype=int)
				for i, low in enumerate(bins[:-1]):
					datapart = [ind, low, bins[i+1]]
					comm.send([datapart, darkbg, mask, save_file], dst=ranki+i)
				ranki += job_num[ind]
				print("Submit %d jobs for hit-finding of %s" % (job_num[ind], dataf))

		# parallel
		datapart, background, mask, save_file = comm.recv(source=0)
		'''
		
		# data broadcast
		darkbg, mask, radii_range = comm.bcast([darkbg, mask, radii_range], root=0)

		# cal shell_index (spipy.image.preprocess.hit_find)
		shell_index = None
		if radii_range[2] < 0 or radii_range[3] < 0 or radii_range[3] <= radii_range[2]:
			inner_shell = spipy.analyse.radp.circle(2, radii_range[2]) + np.array(radii_range[:2]).astype(int)
			outer_shell = spipy.analyse.radp.circle(2, radii_range[3]) + np.array(radii_range[:2]).astype(int)
			shell = np.zeros(mask.shape)
			shell[outer_shell[:,0], outer_shell[:,1]] = 1
			shell[inner_shell[:,0], inner_shell[:,1]] = 0
			shell[np.where(mask > 0)] = 0
			shell_index = np.where(shell == 1)
			del shell, inner_shell, outer_shell

		# tmp status file
		status_file = os.path.join(savepath, "status/status_%d.txt" % mpi_rank)
		num_processed = 0
		num_hits = 0

		for df in data_files:

			if mpi_rank == 0:
				print("- Processing %s ..." % df)

			# load data
			fp = h5py.File(df, 'r')
			num_patterns = fp[inh5].shape[0]
			chi_score_tmp = {}
			label_tmp = {}

			for i in range(num_patterns):

				if i % mpi_size != mpi_rank:
					continue

				pat = fp[inh5][i] * (1 - mask)
				# cal chi-score
				if shell_index is not None:
					chi_score_tmp[i] = np.sum( (pat[shell_index] - darkbg[shell_index])**2 )\
								/ np.sum( (darkbg[shell_index] - np.mean(darkbg[shell_index]))**2 )
				else:
					chi_score_tmp[i] = np.sum( (pat - darkbg)**2 ) / np.sum( (darkbg - np.mean(darkbg))**2 )
				
				# get label
				if chi_score_tmp[i] > chi_cut:
					label_tmp[i] = 1
					num_hits += 1

				num_processed += 1
				su.write_status(status_file, ["status", "processed", "hits"], [1, num_processed, num_hits])

			# barrier
			comm.Barrier()
			chi_score_gather = comm.gather(chi_score_tmp, root=0)
			label_gather = comm.gather(label_tmp, root=0)

			# write to file
			if mpi_rank == 0:

				# print status
				print("- Write results of %s." % os.path.split(df)[-1])

				# get labels
				chi_score = np.zeros(num_patterns)
				label = np.zeros(num_patterns, dtype=int)
				for tmp in chi_score_gather:
					chi_score[tmp.keys()] = tmp.values()
				for tmp in label_gather:
					label[tmp.keys()] = tmp.values()

				# save hits
				single_h = np.where(label==1)[0]
				if len(single_h) == 0 or save_hits <= 0:
					pass
				else:
					if dark_file is not None:
						tmp = fp[inh5][single_h,:,:] * (1 - mask) - darkbg
					else:
						tmp = fp[inh5][single_h,:,:] * (1 - mask)

					if downsampling > 1:
						hfhits = su.avg_pooling(tmp, downsampling)
					else:
						hfhits = tmp

					# save hits
					this_rank_file = os.path.splitext(os.path.split(df)[-1])[0]
					save_file = os.path.join(savepath, this_rank_file + ".hfhits.h5")
					sfp = h5py.File(save_file, 'w')
					sfp.create_dataset("Hits/data", data=hfhits, chunks=True, compression="gzip")
					sfp.create_dataset("Hits/index", data=single_h, chunks=True, compression="gzip")
					sfp.create_dataset("Hits/rawpath", data=df)
					sfp.close()
					# print log
					print("- Save patterns to %s." %save_file)

					# save chi_scores
					save_file = os.path.join(savepath, this_rank_file+".hfscores.dat")
					np.savetxt(save_file, chi_score)
					# print log
					print("- Save chi-scores to %s." %save_file)

			# close data file
			fp.close()


		# gather status
		num_processed_gather = comm.gather(num_processed, root = 0)
		num_hits_gather = comm.gather(num_hits, root = 0)

		if mpi_rank == 0:
			# end time
			end_time = time.time()

			status_file = os.path.join(savepath, "status/summary.txt")
			su.write_status(status_file, ["status", "processed", "hits", "time"], \
				[2, sum(num_processed_gather), sum(num_hits_gather), end_time - start_time])

			print("- Finish.")


	MPI.Finalize()
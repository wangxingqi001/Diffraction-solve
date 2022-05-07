from PyQt4 import QtGui
from PyQt4 import QtCore

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'pygui'))

import subprocess
import shlex
import re
import random
import time
import threading
from ConfigParser import ConfigParser
import json


information = {"report_bug" : "You run into a bug. But it's not your mistake, report it to shiyc12@csrc.ac.cn",\
			}


class TableMonitor(threading.Thread):

	def __init__(self, interval, func):
		threading.Thread.__init__(self)
		self.interval = interval
		self.stopflag = threading.Event()
		self.func = func

	def run(self):
		while not self.stopflag.wait(self.interval):
			self.func()

	def stop(self):
		self.stopflag.set()


##########

def fmt_process_status(dataformat, hits = None, patterns = None):
	h = "---"
	p = "---"
	hr = "---"
	if type(hits) == int and type(patterns) == int:
		h = hits
		p = patterns
		if patterns > 0:
			hr = float(h)/float(p)*100
		else:
			hr = 0
		hr = "%.2f" % hr
	
	status = "Raw-Data  :  %s   ,  Hit-Rate  :  %s%%  ( %s / %s ) " %(dataformat, hr, str(h), str(p))
	return status


def fmt_job_dir(run_name, tag, remarks):
	'''
	define job_dir format, return 'run_name.tag.remarks'
	[linked] : 
		main_app.py  :  get_existing_runtags
						get_latest_runtag
						combine_tag_remarks
						split_tag_remarks
	'''
	return "%s.%s.%s" % (run_name, tag, remarks)


def split_jobdir_runviewkey(job_dir):
	tmp = job_dir.split(".")
	if len(tmp) == 3:
		return {'run_name': tmp[0], 'tag': tmp[1], 'remarks': tmp[2]}
	elif len(tmp) == 4:
		return {'assignments': tmp[0], 'run_name': tmp[1], 'tag': tmp[2], 'remarks': tmp[3]}
	else:
		return None


def fmt_runview_key(assgn, run_name, tag, remarks):
	'''
	return 'assgn.run_name.tag.remarks'
	'''
	return "%s.%s.%s.%s" % (assgn, run_name, tag, remarks)


def fmt_remarks(remarks):
	return re.sub('\"|\'|\s|\.|\$','',remarks)


def extract_tag(assignments, tagfilename):
	'''
	configuration file format : 'assgn_tag.ini'
	[linked] :
		process.py : extract_tag
		jobc.py    : extract_tag
	'''
	return tagfilename.split(assignments+"_")[-1].split('.ini')[0]


def split_config(tagfilename):
	'''
	configuration file format : 'assgn_tag.ini'
	[linked] :
		process.py : extract_tag
		jobc.py    : extract_tag
	'''
	return tagfilename.strip(".ini").split("_")


def findnumber(string):
	return re.findall(r"\d+\.?\d*", string)


def split_runview_key(run_view_key, ret):
	'''
	asg : assignments
	rn  : run_name
	tag : tag
	rm  : remarks
	'''
	tmp = run_view_key.split('.')
	if ret == "asg":
		return tmp[0]
	elif ret == "rn":
		return tmp[1]
	elif ret == "tag":
		return tmp[2]
	elif ret == "rm":
		return tmp[3]
	else:
		return tmp


# ./scripts/scripts_utils.py has a copy
def compile_h5loc(loc, run_name):
	# %r means run name
	tmp = re.sub(r"%r", run_name, loc)
	# ...
	# tmp = ...
	return tmp



##########

def show_message(message, informative=None):
	msgBox = QtGui.QMessageBox()
	msgBox.setTextFormat(QtCore.Qt.PlainText)
	msgBox.setIcon(QtGui.QMessageBox.Information)
	msgBox.setText(message)
	if informative is not None:
		msgBox.setInformativeText(informative)
	msgBox.setStandardButtons(QtGui.QMessageBox.Ok)
	ret = msgBox.exec_()
	if ret == QtGui.QMessageBox.Ok:
		return 1


def show_warning(message, informative=None):
	msgBox = QtGui.QMessageBox()
	msgBox.setTextFormat(QtCore.Qt.PlainText)
	msgBox.setIcon(QtGui.QMessageBox.Warning)
	msgBox.setText(message)
	if informative is not None:
		msgBox.setInformativeText(informative)
	msgBox.addButton(QtGui.QPushButton('NO'), QtGui.QMessageBox.NoRole)
	msgBox.addButton(QtGui.QPushButton('YES'), QtGui.QMessageBox.YesRole)
	ret = msgBox.exec_()
	# ret == 1 -> YES ; ret == 0 -> NO
	return ret



##########

def check_PBS():
	cmd = "command -v qsub qstat pestat"
	cmds = shlex.split(cmd)
	try:
		tmp = subprocess.check_output(cmds)
		return True
	except:
		return False


def check_LSF():
	cmd = "command -v bsub bkill bjobs"
	cmds = shlex.split(cmd)
	try:
		tmp = subprocess.check_output(cmds)
		return True
	except:
		return False


def check_datadir(datadir, fmt_ind, all_fmts, subDir):
	"""
	return code:
		[0, 0]   : no semingly data files found
		[1, -1]  : format is correct, subDir is wrong
		[str, 1] : format is wrong (return guessed format), subDir is correct
		[str, -1]: format is wrong (return guessed format), subDir is wrong
		[1, 1]   : test pass 
		[-1, -1] : error
	"""

	def check_subDir(dirs, fmt_ind, all_fmts):
		check_dir_num = min(10,len(dirs))
		for i in range(len(all_fmts)):
			count[i] = 0
		for i in range(check_dir_num):
			thisdir = random.choice(dirs)
			thisfiles_sub = [f.split(".")[-1].lower() for f in os.listdir(thisdir)\
										 if os.path.isfile(os.path.join(thisdir,f))]
			for i in range(len(all_fmts)):
				if thisfiles_sub.count(all_fmts[i]) > 0:
					count[i] += 1
			dirs.remove(thisdir)
		if count[fmt_ind] >= 1:
			# yes, secondary dirs
			return 1
		elif max(count) == 0:
			# empty
			return 0
		else:
			# data format problem
			return all_fmts[count.index(max(count))]


	allpath = [f for f in os.listdir(datadir) if f[0]!="." and f[0]!="$"]
	if len(allpath) == 0:
		return [0, 0]

	allfiles_ext = [f.split(".")[-1].lower() for f in allpath \
					if os.path.isfile(os.path.join(datadir,f))]
	alldirs = [os.path.join(datadir,d) for d in allpath if \
					os.path.isdir(os.path.join(datadir,d))]

	if subDir == False:
		# format is correct ?
		count = [0] * len(all_fmts)
		for i in range(len(all_fmts)):
			count[i] = allfiles_ext.count(all_fmts[i])
		most = count.index(max(count))
		if max(count) == 0:   # subDir incorrect
			if len(alldirs) == 0:   # empty
				return [0, 0]
			else:
				tmp = check_subDir(alldirs, fmt_ind, all_fmts)
				if tmp == 0:        # empty
					return [0, 0]
				elif tmp == 1:      # format correct
					return [1, -1]
				else:               # format incorrect
					return [tmp, -1]
		elif most == fmt_ind:  # subDir correct, format correct
			return [1, 1]
		else:                  # subDir correct, format incorrect
			return [all_fmts[most], 1]
	else:
		# format is correct ?
		count = [0] * len(all_fmts)
		for i in range(len(all_fmts)):
			count[i] = allfiles_ext.count(all_fmts[i])
		most = count.index(max(count))
		if len(alldirs) == 0:     # no dir
			if max(count) == 0:   # empty
				return [0, 0]
			elif most == fmt_ind: # format correct, subDir incorrect
				return [1, -1]
			else:                 # format incorrect, subDir incorrect
				return [all_fmts[most], -1]
		else:
			tmp = check_subDir(alldirs, fmt_ind, all_fmts)
			if tmp == 0:            # no data in folder
				if max(count) == 0:
					return [0, 0]   # empty
				elif most == fmt_ind:
					return [1, -1]  # format correct, subDir incorrect
				else:
					return [all_fmts[most], -1]   # format incorrect, subDir incorrect
			elif tmp == 1:          # format correct, subDir correct
				return [1, 1]
			else:                   # format incorrect, subDir correct
				return [tmp, 1]
		


def parse_multi_runs_nosubdir(path, dataformat):
	# xtc file name format :
	# 	https://confluence.slac.stanford.edu/display/PSDM/Data+Formats
	# default format do not support multi-files per run,
	# where no sub-dir exists.
	all_files = [f for f in os.listdir(path) if f[0]!='.']
	if dataformat.lower() == "xtc":
		runs_multi = [findnumber(r)[1] for r in all_files if \
							os.path.isfile(os.path.join(path, r)) \
							and r.split('.')[-1].lower() == dataformat.lower()]
	else:
		runs_multi = [os.path.splitext(r)[0] for r in all_files if \
							os.path.isfile(os.path.join(path, r)) \
							and r.split('.')[-1].lower() == dataformat.lower() \
							and '.' not in os.path.splitext(r)[0]]
	runs = list(set(runs_multi))
	counts = [runs_multi.count(run) for run in runs]
	runs = [runs[i]+"?_?^=^%d" % counts[i] for i in range(len(runs))]
	return runs


def parse_multi_run_streams(path, runname, dataformat, subdir=True):
	# xtc file name format :
	# 	https://confluence.slac.stanford.edu/display/PSDM/Data+Formats
	all_files = [f for f in os.listdir(path) if f[0]!='.']
	if dataformat.lower() == "xtc":
		run_num = findnumber(runname)[1]
		streams = [os.path.join(path,s) for s in all_files if os.path.isfile(os.path.join(path, s)) \
					and s.split('.')[-1].lower() == dataformat.lower() and findnumber(s)[1] == run_num]
	else:
		if subdir:
			streams = [os.path.join(path,s) for s in all_files if os.path.isfile(os.path.join(path, s)) \
					and s.split('.')[-1].lower() == dataformat.lower()]
		else:
			streams = [os.path.join(path,runname)]
	return streams


def submit_job(workdir, cmd, jss):
	'''
		# jss can be 'PBS', 'LSF' and None
		# return subprocess.Popen object / string (job id of jss)
		## this function is used in jobc.packSubmit
		## Link:
			submission.sh
			app_namespace.ini
	'''
	os.chdir(workdir)

	try:
		if jss is None:
			pobj = subprocess.Popen(cmd, shell=True, preexec_fn=os.setsid)
			return pobj
		elif jss.upper() == "PBS":
			tmp = subprocess.check_output(cmd, shell=True)
			pobj = tmp.strip('\n').strip()
			return pobj
		elif jss.upper() == "LSF":
			tmp = subprocess.check_output(cmd, shell=True)
			pobj = findnumber(tmp)[0]
			return pobj
		else:
			return None
	except:
		return None


def get_status(jss, pid):
	'''
		if jss is None, check whether it is running
		if jss is "PBS" or "LSF", return status index in app_namespace.ini
	'''
	if jss is None:
		return subprocess.check_output("ps -p %d" %pid, shell=True)
	elif jss.upper() == "PBS":
		tmp = subprocess.check_output("qstat %s" % pid, shell=True).split("\n")[2]
		stat = re.split("\s+", tmp)[4]
		if stat.upper() == "R":
			return 1
		elif stat.upper() == "Q" or stat.upper() == "H":
			return 7
		elif stat.upper() == "S":
			return 4
		else:
			return None
	elif jss.upper() == "LSF":
		tmp = subprocess.check_output("bjobs -o stat: %s" %pid, shell=True).split("\n")[1]
		stat = tmp.strip()
		if stat.upper() == "RUN":
			return 1
		elif stat.upper() in ["PEND", "WAIT", "PROV"]:
			return 7
		elif stat.upper() in ["PSUSP", "USUSP", "SSUSP"]:
			return 4
		elif stat.upper() == "DONE":
			return 2
		else:
			return None
	else:
		return None


def kill_job(jss, job_obj):
	'''
		kill job 'pid' on different jss
	'''
	import signal
	try:
		if jss is None:
			if type(job_obj) == subprocess.Popen:
				this_pid = job_obj.pid
			else:
				this_pid = int(job_obj)
			os.killpg(os.getpgid(this_pid),signal.SIGTERM)
			return 1
		elif jss.upper() == "LSF":
			tmp = subprocess.check_output("bkill %s" % job_obj, shell=True)
			return 1
		elif jss.upper() == "PBS":
			tmp = subprocess.check_output("qdel %s" % job_obj, shell=True)
			return 1
		else:
			return 0
	except:
		return 0



##########

def write_config(file, dict, mode='w'):
	'''
		input dict = {'section':{'option':value, ...}, ...}
	'''
	if mode=='w' and os.path.exists(file):
		os.remove(file)
	config = ConfigParser()
	config.read(file)
	sections = config.sections()
	for key, val in dict.items():
		if key not in sections:
			config.add_section(key)
		for k,v in val.items():
			config.set(key, k, v)
	f = open(file, 'w')
	config.write(f)
	f.close()


def read_config(file, item=None):
	config = ConfigParser()
	config.read(file)
	if item is not None:
		return config.get(item[0], item[1])
	else:
		return config


def rawdata_changelog(prev, now):
	nowtime = time.ctime()
	update = {}
	update[nowtime] = [prev, now]
	return update


def print2projectLog(rootdir, message):
	nowtime = time.ctime()
	st = "[INFO](%s) : %s\n" % (nowtime, message)
	with open(os.path.join(rootdir, "project.log"), 'a+') as f:
		f.write(st)


def readprojectLog(rootdir):
	logfile = os.path.join(rootdir, "project.log")
	if os.path.exists(logfile):
		with open(logfile, 'r') as f:
			lines = f.readlines()
		return lines
	else:
		return None


def logging_table(info_dict, changelog_dict, processdir):
	path = os.path.join(processdir, 'table.info')
	with open(path, 'w') as outfile:
		json.dump(info_dict, outfile)
	path = os.path.join(processdir, 'table.change')
	with open(path, 'w') as outfile:
		json.dump(changelog_dict, outfile)


def load_changelog(processdir):
	path = os.path.join(processdir, 'table.change')
	info = {}
	if os.path.isfile(path):
		try:
			with open(path, 'r') as readfile:
				info = json.load(readfile)
		except:
			pass
	return info


def load_table(processdir):
	path = os.path.join(processdir, 'table.info')
	table = {}
	if os.path.isfile(path):
		try:
			with open(path, 'r') as readfile:
				table = json.load(readfile)
		except:
			pass
	return table


def read_ini():
	conf = ConfigParser()
	conf.read(os.path.join(os.path.split(os.path.realpath(__file__))[0],"app_namespace.ini"))
	namespace = {}
	namespace['ini'] = conf.get('start', 'appini')
	namespace['log'] = conf.get('start', 'applog')
	namespace['project_structure'] = conf.get('start', 'project_structure').split(',')
	namespace['project_ini'] = conf.get('start', 'project_ini').split(':')
	namespace['JSS_support'] = conf.get('start', 'JSS_support').split(',')
	namespace['monitor_time'] = conf.getfloat('start', 'monitor_time')
	namespace['config_head'] = conf.get('start', 'config_head')
	namespace['data_format'] = conf.get('start', 'data_format').split(',')
	namespace['process_assignments'] = conf.get('process', 'assignments').split(',')
	namespace['process_status'] = conf.get('process', 'status').split(',')
	namespace['process_pat_per_job'] = conf.getint('process', 'pat_per_job')
	namespace['max_jobs_per_run'] = conf.getint('process', 'max_jobs_per_run')
	process_colors = conf.get('process', 'colors').split(',')
	namespace['process_colors'] = [[0,0,0]]*len(process_colors)
	for i,cl in enumerate(process_colors):
		tmp = cl.split('.')
		namespace['process_colors'][i] = [int(tmp[0]),int(tmp[1]),int(tmp[2])]
	namespace['darkcal'] = conf.get('process', 'darkcal')
	namespace['classify_assignments'] = conf.get('classify', 'assignments').split(',')
	namespace['classify_decomp'] = conf.get('classify', 'decomp').split(',')
	namespace['merge_sym'] = conf.get('merge', 'sym').split(',')
	namespace['merge_assignments'] = conf.get('merge', 'assignments').split(',')
	namespace['phasing_method'] = conf.get('phasing', 'method').split(',')
	namespace['phasing_assignments'] = conf.get('phasing', 'assignments').split(',')
	namespace['simulation_assignments'] = conf.get('simulation', 'assignments').split(',')
	# now add nickname
	namespace['process_HF'] = conf.get('process', 'HF')
	namespace['process_FA'] = conf.get('process', 'FA')
	namespace['process_FAA'] = conf.get('process', 'FAA')
	namespace['process_AP'] = conf.get('process', 'AP')
	namespace['process_CLF'] = "decomp"
	namespace['process_MRG'] = "merge"
	namespace['process_PHS'] = "phasing"
	namespace['classify_SVD'] = conf.get('classify', 'SVD')
	namespace['classify_LLE'] = conf.get('classify', 'LLE')
	namespace['classify_SPEM'] = conf.get('classify', 'SPEM')
	namespace['classify_TSNE'] = conf.get('classify', 'TSNE')
	namespace['classify_DCPS'] = conf.get('classify', 'DCPS')
	namespace['phasing_PJ'] = conf.get('phasing', 'PJ')
	namespace['phasing_RAAR'] = conf.get('phasing', 'RAAR')
	namespace['phasing_DM'] = conf.get('phasing', 'DM')
	namespace['phasing_ERA'] = conf.get('phasing', 'ERA')
	namespace['merge_ICOSYM'] = conf.get('merge', 'ICOSYM')
	namespace['merge_emc'] = conf.get('merge', 'emc')
	namespace['simulation_FFT'] = conf.get('simulation', 'FFT')
	namespace['simulation_AS'] = conf.get('simulation', 'AS')
	return namespace


def read_status(file):
	values = {}
	with open(file, "r") as fp:
		for line in fp.readlines():
			info, value = line.split(":")
			values[info.strip()] = value.strip()
	return values


def get_scripts():
	conf = ConfigParser()
	conf.read(os.path.join(os.path.split(os.path.realpath(__file__))[0],"scripts/scripts.ini"))
	section = conf.sections()[0]
	scripts = {}
	for op in conf.options(section):
		scripts[op] = conf.get(section, op)
	return scripts

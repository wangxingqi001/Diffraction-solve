from PyQt4 import QtGui
from PyQt4 import QtCore
from PyQt4 import QtWebKit

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'pygui'))

import shutil
import glob
import subprocess
import json
import random
import time
import Queue
import subprocess
import h5py

import utils
from run_gui import Ui_Run_Dialog

"""
job struct, should contain:
	assignments  (str)
	datafile     (list)
	run_name     (str)
	run_tag      (str)
	run_remarks  (str)
	config       (str)
	process_obj  (subprocess.popen() object)
	status       (str)
	savepath     (str)
	submit_time  (float)
"""
class ajob:

	def __init__(self, thetype, datafile, run_name, run_tag, run_remarks, config, process_obj, jss, savepath, stime):
		self.assignments = thetype
		self.datafile = datafile
		self.run_name = run_name
		self.run_tag = run_tag
		self.run_remarks = run_remarks
		self.config = config
		self.process_obj = process_obj
		self.jss = jss
		self.savepath = savepath
		self.submit_time = stime



class JobCenter(QtGui.QDialog, QtCore.QEvent):

	# job status
	PRE = ""
	SUB = ""
	RUN = ""
	ERR = ""
	FIN = ""
	TER = ""
	JOBQMAX = 10000

	def __init__(self, jss, project_root, data_format, main_gui):
		QtGui.QWidget.__init__(self)
		# setup ui
		self.ui = Ui_Run_Dialog()
		self.ui.setupUi(self)
		# father
		self.main_gui = main_gui
		# dict, structure is "jid : ajob"
		self.jobs = {}
		# dict, {'assgn.runname.tag.remarks' : jid}
		self.run_view = {}
		# dict, store jid of waiting jobs, submitted jobs and returned jobs
		self.job_queue = {'waiting':Queue.Queue(JobCenter.JOBQMAX), \
		'submitted':Queue.Queue(JobCenter.JOBQMAX), 'returned':Queue.Queue(JobCenter.JOBQMAX)}
		# locations of run-scripts
		self.python_scripts = utils.get_scripts()
		# submit queue
		self.submit_queue = None
		# given runtime
		self.given_runtime = None

		self.jss = jss
		self.rootdir = project_root
		self.data_format = data_format
		self.namespace = utils.read_ini()
		JobCenter.PRE = self.namespace['process_status'][0]
		JobCenter.SUB = self.namespace['process_status'][7]
		JobCenter.RUN = self.namespace['process_status'][1]
		JobCenter.ERR = self.namespace['process_status'][3]
		JobCenter.FIN = self.namespace['process_status'][2]
		JobCenter.TER = self.namespace['process_status'][4]
		# job hub file
		prev_jobs, prev_run_view = self.load_job_hub()
		self.jobs = dict(prev_jobs, **self.jobs)
		self.run_view = dict(prev_run_view, **self.run_view)
		# force overwrite
		self.force_overwrite = False
		# darkcal in h5
		self.darkcal_inh5 = utils.read_config(os.path.join(self.rootdir, self.namespace['project_structure'][0], 'config/darkcal.ini'), ['darkcal', 'inh5'])

		# triggers
		# self.ui.comboBox.currentIndexChanged.connect(self.tag_changed)
		self.connect(self.ui.pushButton, QtCore.SIGNAL(("clicked()")), self.darkcal_dir)
		self.connect(self.ui.pushButton_3, QtCore.SIGNAL(("clicked()")), self.run)
		self.connect(self.ui.pushButton_2, QtCore.SIGNAL(("clicked()")), self.cancel)
		self.ui.comboBox.currentIndexChanged.connect(self.config_change)


	def setjss(self, jss):
		self.jss = jss


	def get_jid(self, assignments, run_name, tag, remarks):
		run_view_key = utils.fmt_runview_key(assignments, run_name, tag, remarks)
		return self.run_view[run_view_key]


	def get_runviewkey(self, jid):
		if not self.jobs.has_key(jid):
			return "None"
		return utils.fmt_runview_key(self.jobs[jid].assignments, self.jobs[jid].run_name, \
			self.jobs[jid].run_tag, self.jobs[jid].run_remarks)


	def del_job_record(self, jid):
		run_view_key = self.get_runviewkey(jid)
		try:
			self.jobs.pop(jid)
			self.run_view.pop(run_view_key)
			return 1
		except:
			return 0


	def reverseForceOverwrite(self):
		self.force_overwrite = not self.force_overwrite
		utils.print2projectLog(self.rootdir, "Set project force overwrite to %s" % str(self.force_overwrite))


	def get_config_path(self, module, assignments, tagname=None):
		if tagname is None:
			return glob.glob(os.path.join(os.path.join(self.rootdir, module), 'config/%s_*' % assignments))
		else:
			return glob.glob(os.path.join(os.path.join(self.rootdir, module), 'config/%s_%s.ini' % (assignments, tagname)))


	def extract_tag(self, tagfilename):
		'''
			consistent with process.py extract_tag
		'''
		return tagfilename.split("_")[-1].split('.ini')[0]


	def write_job_hub(self, jid=None):

		def tostr(datafile):
			df = ""
			for f in datafile:
				df = df + "," + f
			return df[1:]

		with open(os.path.join(self.rootdir, "JobHub.txt"), 'a+') as f:
			if jid is None:
				for jid in self.jobs.keys():
					a = self.jobs[jid]
					f.write("\n")
					f.write("jid         = %s \n" % str(jid))
					f.write("assignments = %s \n" % a.assignments)
					f.write("data_files  = %s \n" % tostr(a.datafile))
					f.write("run_name    = %s \n" % a.run_name)
					f.write("run_tag     = %s \n" % a.run_tag)
					f.write("run_remarks = %s \n" % a.run_remarks)
					f.write("config      = %s \n" % a.config)
					if type(a.process_obj) == str:
						f.write("pid         = %s \n" % a.process_obj)
					else:
						try:
							f.write("pid         = %s \n" % str(a.process_obj.pid))
						except:
							f.write("pid         = -1 \n")
					f.write("status      = %s \n" % a.status)
					f.write("savepath    = %s \n" % a.savepath)
					f.write("submit_time = %s \n" % a.submit_time)
			else:
				a = self.jobs[jid]
				f.write("\n")
				f.write("jid         = %s \n" % str(jid))
				f.write("assignments = %s \n" % a.assignments)
				f.write("data_files  = %s \n" % tostr(a.datafile))
				f.write("run_name    = %s \n" % a.run_name)
				f.write("run_tag     = %s \n" % a.run_tag)
				f.write("run_remarks = %s \n" % a.run_remarks)
				f.write("config      = %s \n" % a.config)
				if type(a.process_obj) == str:
					f.write("pid         = %s \n" % a.process_obj)
				else:
					try:
						f.write("pid         = %s \n" % str(a.process_obj.pid))
					except:
						f.write("pid         = -1 \n")
				f.write("jss         = %s \n" % a.jss)
				f.write("savepath    = %s \n" % a.savepath)
				f.write("submit_time = %s \n" % a.submit_time)



	def load_job_hub(self):
		'''
		load only in self.__init__
		'''
		prev_jobs = {}
		prev_run_view = {}
		if not os.path.exists(os.path.join(self.rootdir, "JobHub.txt")):
			f = open(os.path.join(self.rootdir, "JobHub.txt"), 'w')
			f.close()
		else:
			jid = None
			run_view_key = None
			with open(os.path.join(self.rootdir, "JobHub.txt"), 'r') as f:
				for line in f.readlines():
					line = line.strip('\n')
					if "jid" in line:
						jid = int(line.split("=")[-1].strip())
						prev_jobs[jid] = ajob(None, None, None, None, None, None, None, None, None, None)
					elif "assignments" in line:
						prev_jobs[jid].assignments = line.split("=")[-1].strip()
					elif "data_files" in line:
						prev_jobs[jid].datafile = line.split("=")[-1].strip().split(",")
					elif "run_name" in line:
						prev_jobs[jid].run_name = line.split("=")[-1].strip()
					elif "run_tag" in line:
						prev_jobs[jid].run_tag = line.split("=")[-1].strip()
					elif "run_remarks" in line:
						prev_jobs[jid].run_remarks = line.split("=")[-1].strip()
						run_view_key = utils.fmt_runview_key(prev_jobs[jid].assignments, \
							prev_jobs[jid].run_name, prev_jobs[jid].run_tag, prev_jobs[jid].run_remarks)
						if prev_run_view.has_key(run_view_key):
							prev_jid = prev_run_view[run_view_key]
							prev_jobs.pop(prev_jid)
						prev_run_view[run_view_key] = jid
					elif "config" in line:
						prev_jobs[jid].config = line.split("=")[-1].strip()
					elif "pid" in line:
						prev_jobs[jid].process_obj = line.split("=")[-1].strip()
					elif "jss" in line:
						tmp = line.split("=")[-1].strip()
						if tmp.upper() == "NONE":
							prev_jobs[jid].jss = None
						else:
							prev_jobs[jid].jss = tmp
					elif "savepath" in line:
						prev_jobs[jid].savepath = line.split("=")[-1].strip()
						# if the job do not exist
						if not os.path.exists(prev_jobs[jid].savepath):
							prev_jobs.pop(jid)
							prev_run_view.pop(run_view_key)
							jid = None
							run_view_key = None
					elif "submit_time" in line:
						if jid is not None and run_view_key is not None:
							prev_jobs[jid].submit_time = float(line.split("=")[-1].strip())
					else:
						pass				

		return prev_jobs, prev_run_view


	"""
		Events of table's pop-window ,
		from TableRun_showoff()
		#####################################################################
	"""


	def TableRun_showoff(self, job_type, run_names, datafile, runtime=None):
		# only 'Process' module needs to call this function
		# job_type:
		#	(e.g.) Process/Hit-Finding
		# run_names:
		#	(e.g.) ['r0001', 'r0002', ...]
		# datafile:
		# 	(e.g.) {'r0001':['.../r0001-1.h5', '.../r0001-2.h5', ...], ...}
		# runtime:
		#	(e.g.) {'param1':value1, 'param2':value2, ...}

		module, assg = job_type.split('/')
		# add param tag
		params_tags = self.get_config_path(module, assg)
		self.ui.comboBox.clear()
		for tag in params_tags:
			self.ui.comboBox.addItem(os.path.split(tag)[-1])
		# hit-finding has darkcal
		if self.namespace['process_HF'] == assg:
			self.ui.comboBox.addItem("darkcal.ini")
			self.ui.widget_4.setVisible(True)
		else:
			self.ui.widget_4.setVisible(False)
		# default remarks
		self.ui.lineEdit.setText("default")
		# queue editor
		if self.jss is None:
			self.ui.lineEdit_3.setText("None")
			self.ui.lineEdit_3.setEnabled(False)
		else:
			self.ui.lineEdit_3.setText(str(self.submit_queue))
			self.ui.lineEdit_3.setEnabled(True)
		# hit-finding has darkcal
		if assg == self.namespace['process_HF']:
			darkcal_file = os.path.join(self.rootdir, job_type, self.namespace['darkcal'])
			if os.path.exists(darkcal_file):
				self.ui.lineEdit_2.setText(darkcal_file)
			else:
				self.ui.lineEdit_2.setText("None")

		for run_name in run_names:
			# generate an id
			while True:
				jid = random.randint(100000, 1000000)
				if len(self.jobs) >= 500000 or not self.jobs.has_key(jid):
					break

			jobdir_rootpath = os.path.join(self.rootdir, job_type)
			self.jobs[jid] = ajob(assg, datafile[run_name], run_name, None, None, None, None, JobCenter.SUB, jobdir_rootpath, None)

			# push in job_queue['waiting']
			self.job_queue['waiting'].put(jid)
	
		# update runtime
		if runtime is not None:
			self.given_runtime = runtime

		# show modal
		self.setWindowTitle("%s.%s" % (module, assg))
		self.setModal(True)
		self.show()


	def run(self):
		# update self.run_view
		# update run_tag, config, run_remarks, pid, savepath, stime, status in self.jobs

		# read tag, remarks and darkcal
		title = str(self.windowTitle())
		module, assignments = title.split('.')
		config_file_name = str(self.ui.comboBox.currentText())  # 'xxx.ini'

		config_file = os.path.join(self.rootdir, module, "config", config_file_name)
		tag_name = self.extract_tag(config_file_name)
		self.submit_queue = str(self.ui.lineEdit_3.text())

		if not os.path.isfile(config_file):
			utils.show_message("Choose a configuration file !")
			return
		remarks = utils.fmt_remarks(str(self.ui.lineEdit.text()))
		if len(remarks) == 0:
			remarks = "default"

		# hit-finding has darkcal
		if assignments == self.namespace['process_HF']:
			if config_file_name == "darkcal.ini":
				if len(str(self.ui.lineEdit_2.text())) == 0:
					utils.show_message("Dark calibration needs data location in raw h5/cxi file.")
					return
				if not str(self.ui.lineEdit_2.text()) == self.darkcal_inh5:
					self.darkcal_inh5 = str(self.ui.lineEdit_2.text())
					utils.write_config(os.path.join(self.rootdir, self.namespace['project_structure'][0], 'config/darkcal.ini'), {'darkcal':{'inh5':self.darkcal_inh5}}, "a")
			else:
				if not os.path.isfile(str(self.ui.lineEdit_2.text())):
					re = utils.show_warning("Dark calibration file is invalid, \
						which may affect the hit-finding accuracy. Ignore and go on?")
					if re == 0:
						return
					else:
						self.ui.lineEdit_2.setText("None")

		while not self.job_queue['waiting'].empty():
			wjid = self.job_queue['waiting'].get()
			jobdir = utils.fmt_job_dir(self.jobs[wjid].run_name, tag_name, remarks)
			self.jobs[wjid].run_tag = tag_name
			self.jobs[wjid].config = config_file
			self.jobs[wjid].run_remarks = remarks
			self.jobs[wjid].savepath = os.path.join(self.jobs[wjid].savepath, jobdir)
			# submit
			pobj = self.packSubmit(self.jobs[wjid])
			if pobj is not None:
				self.jobs[wjid].jss = self.jss
				self.jobs[wjid].submit_time = time.time()
				self.jobs[wjid].process_obj = pobj
				# self.run_view
				run_view_key = utils.fmt_runview_key(self.jobs[wjid].assignments, \
					self.jobs[wjid].run_name, tag_name, remarks)
				self.run_view[run_view_key] = wjid
				# write to project log
				utils.print2projectLog(self.rootdir, "Submit %s job on %s : pid is %s" % (self.jobs[wjid].assignments, self.jobs[wjid].run_name, str(pobj.pid)) )
				# write job infomation into jobHub
				self.write_job_hub(wjid)
				# refresh
				self.main_gui.update_table_runs()
				self.main_gui.draw_table()
			else:
				utils.show_message("Fail to submit job %s" % jobdir)
				utils.print2projectLog(self.rootdir, "Fail to submit job : %s (%s.%s)" % (self.jobs[wjid].assignments, self.jobs[wjid].run_name, self.jobs[wjid].run_tag))
				self.jobs.pop(wjid)
		self.close()
		pass


	def darkcal_dir(self):
		h5file = str(QtGui.QFileDialog(self).getOpenFileName(None, "Select darkcal (h5) file to open", "", "DARKCAL (*.h5)"))
		self.ui.lineEdit_2.setText(h5file)


	def config_change(self):
		title = str(self.windowTitle())
		if self.namespace['process_HF'] in title:
			current_tag = str(self.ui.comboBox.currentText())
			if current_tag == "darkcal.ini":
				if self.data_format.lower() in ['cxi', 'h5']:
					self.ui.widget_4.setVisible(True)
					self.ui.label_3.setText("Data loc in H5/cxi")
					self.ui.lineEdit_2.setText(self.darkcal_inh5)
					self.ui.pushButton.setVisible(False)
				else:
					self.ui.widget_4.setVisible(False)
			else:
				darkcal_file = os.path.join(self.rootdir, self.namespace['project_structure'][0], self.namespace['process_HF'], self.namespace['darkcal'])
				self.ui.widget_4.setVisible(True)
				self.ui.label_3.setText("Darkcal (h5)      ")
				if os.path.exists(darkcal_file):
					self.ui.lineEdit_2.setText(darkcal_file)
				else:
					self.ui.lineEdit_2.setText("None")
				self.ui.pushButton.setVisible(True)


	def cancel(self):
		while not self.job_queue['waiting'].empty():
			jid = self.job_queue['waiting'].get()
			tmp = self.jobs.pop(jid)
			del tmp
		self.close()


	"""
		############################End of this part##########################
	"""


	def kill_job(self, jid):
		job_jss = self.jobs[jid].jss
		job_obj = self.jobs[jid].process_obj
		re = utils.kill_job(job_jss, job_obj)
		return [re, job_jss]


	def __get_run_status_private(self, lastest_job, job_dir, job_jss):

		if type(lastest_job) is str:
			if os.path.isfile(os.path.join(job_dir, "status/summary.txt")):
				return JobCenter.FIN
			elif lastest_job.strip() == "-1":
				return None
			elif job_jss is None:
				# the job was submitted directly
				pid = int(lastest_job)
				if pid < 0:
					return None
				else:
					try:
						tmp = utils.get_status(None, pid)
						return JobCenter.RUN
					except:
						errlog = glob.glob(os.path.join(job_dir, "*.err"))
						if len(errlog) == 0:
							return JobCenter.PRE
						else:
							with open(errlog[0]) as fp:
								for line in fp.readlines():
									if "traceback" in line.lower():
										return JobCenter.ERR
									elif "terminated" in line.lower() or "killed" in line.lower():
										return JobCenter.TER
							return JobCenter.TER
			else:
				# the job was submitted using jss
				pid = lastest_job
				try:
					stat = utils.get_status(job_jss, pid)
					return self.namespace['process_status'][stat]
				except:
					errlog = glob.glob(os.path.join(job_dir, "*.err"))
					if len(errlog) == 0:
						return JobCenter.PRE
					else:
						with open(errlog[0]) as fp:
							for line in fp.readlines():
								if "traceback" in line.lower():
									return JobCenter.ERR
								elif "terminated" in line.lower():
									return JobCenter.TER
						return JobCenter.TER
		elif type(lastest_job) == subprocess.Popen:
			# directly submit
			recode = lastest_job.poll()
			if recode is None:
				return JobCenter.RUN
			elif recode < 0:
				return JobCenter.TER
			elif recode > 0:
				if recode >= 128:
					return JobCenter.TER
				else:
					return JobCenter.ERR
			else:
				return JobCenter.FIN
		else:
			return None


	def get_run_status(self, run_name, module, assg, tag, remarks):
		'''
		input tag is 'tag' (no remarks)
		return status or None
		'''
		# get status of runs
		run_view_key = utils.fmt_runview_key(assg, run_name, tag, remarks)
		if not self.run_view.has_key(run_view_key):
			job_jss = None
			job_dir = os.path.join(self.rootdir, module, assg, utils.fmt_job_dir(run_name, tag, remarks))
			if os.path.isdir(job_dir):
				lastest_job = "0"
			else:
				lastest_job = "-1"
		else:
			jid = self.run_view[run_view_key]
			lastest_job = self.jobs[jid].process_obj
			job_dir = self.jobs[jid].savepath
			job_jss = self.jobs[jid].jss

		status = self.__get_run_status_private(lastest_job, job_dir, job_jss)
		return status


	def get_run_status_2(self, jid):
		# for given jid
		# return status or None
		if not self.jobs.has_key(jid):
			return JobCenter.ERR
		lastest_job = self.jobs[jid].process_obj
		job_dir = self.jobs[jid].savepath
		job_jss = self.jobs[jid].jss

		status = self.__get_run_status_private(lastest_job, job_dir, job_jss)
		return status



	'''
		This is the most important function. It packs information and submits jobs.
	'''
	def packSubmit(self, job_obj):


		def get_mpi_ranks(runtime_data, inh5, pat_per_job, max_jobs):
			pat_num = 0
			try:
				for ind, f in enumerate(runtime_data):
					fp = h5py.File(f, 'r')
					if len(fp[inh5].shape) != 3:
						return None
					pat_num += fp[inh5].shape[0]
					fp.close()
			except:
				return None
			return min( pat_num/pat_per_job+1, max_jobs )



		proj_conf = utils.read_config(os.path.join(self.rootdir, self.namespace['ini']))
		pat_per_job = int(proj_conf.get(self.namespace['project_ini'][0], self.namespace['project_ini'][1].split(',')[4]))
		max_jobs = int(proj_conf.get(self.namespace['project_ini'][0], self.namespace['project_ini'][1].split(',')[5]))
		del proj_conf
		# runtime json
		runtime = {}
		# config file
		config_file = None

		# jss
		sub_jss = self.jss
		# queue
		sub_submit_queue = self.submit_queue
		# write run shell to workdir
		sub_prepare_sub_script = os.path.join(os.path.split(os.path.realpath(__file__))[0], "scripts/submission.sh")
		# python script or exec command
		sub_exec = None
		# number of process
		sub_nproc = 0
		# workdir
		sub_workdir = None
		# sub_ppn, only for PBS
		sub_ppn = 24
		# submission type : standard (mpi4py parallel) or customed (mpi C parallel)
		sub_type = 'standard'
		# control whether to overwrite exisiting projects, or just continue from it
		sub_resume = False


		############ These are irrelevent to assignments' type
		runtime['run_name'] = job_obj.run_name
		runtime['dataset'] = job_obj.datafile
		runtime['savepath'] = job_obj.savepath
		config_file = job_obj.config
		sub_workdir = job_obj.savepath
		# get inh5
		try:
			inh5 = utils.read_config(config_file, [self.namespace['config_head'], 'data-path in cxi/h5'])
		except:
			try:
				inh5 = utils.read_config(config_file, ['darkcal', 'inh5'])
			except:
				inh5 = None
		if inh5 is not None:
			inh5 = utils.compile_h5loc(inh5, job_obj.run_name)
		############


		'''
			choose correct code blocks to run according to the given job type
		'''
		if job_obj.assignments == self.namespace['process_HF']:
		# hit-finding

			if str(self.ui.lineEdit_2.text()).lower() == "none":
				runtime['darkcal'] = None
			else:
				runtime['darkcal'] = str(self.ui.lineEdit_2.text())
			
			sub_exec = os.path.join(os.path.split(os.path.realpath(__file__))[0], "scripts", self.python_scripts["hf"])
			
			# decide mpi rank size
			# actually I want to be free from inh5, but can not find any good ways
			if self.data_format.lower() in ["cxi", "h5"]:
				sub_nproc = get_mpi_ranks(runtime['dataset'], inh5, pat_per_job, max_jobs)
				if sub_nproc is None:
					utils.show_message("The data file '%s' seem seem to have some problems. Check whether the 'Data-path in cxi/h5' is correct." %f)
					return None
			else:
				# TODO
				pass

		elif job_obj.assignments == self.namespace['process_AP']:
		# adu2photon

			sub_exec = os.path.join(os.path.split(os.path.realpath(__file__))[0], "scripts", self.python_scripts["ap"])
			# get data source path
			this_datadir = utils.read_config(config_file, [self.namespace['config_head'], 'Data Dir'])
			# Note : job_obj.datafile contains raw data files selected in 'process table',
			# but if adu2photon do not use raw data (e.g. use hit-finding results), then
			if not job_obj.datafile[0].startswith(this_datadir):
				assignments = os.path.split(this_datadir)[-1]
				try:
					tag_remarks = self.main_gui.tag_buffer[assignments][runtime['run_name']]
					tag_remarks = self.main_gui.split_tag_remarks(tag_remarks)
					if len(tag_remarks) != 2:
						raise ValueError("boomb")
				except:
					utils.show_message("%s:\nI cannot find the data source, please check the parameters agian." % runtime['run_name'])
					return None
				datafile = os.path.join(this_datadir, utils.fmt_job_dir(runtime['run_name'], tag_remarks[0], tag_remarks[1]), '*.h5')
				datafile = glob.glob(datafile)
				if len(datafile) == 0:
					utils.show_message("%s:\nI cannot find the data source, please check the parameters agian." % runtime['run_name'])
					return None
				runtime['dataset'] = datafile

			# decide mpi rank size
			sub_nproc = get_mpi_ranks(runtime['dataset'], inh5, pat_per_job, max_jobs)
			if sub_nproc is None:
				utils.show_message("The data files in '%s' seem to have some problems.\n" % this_datadir + \
									"Check : data location, 'Data-path in cxi/h5' are correct;\n" + \
									"Check : data files are multi-pattern HDf5 format.")
				return None

		elif job_obj.assignments == self.namespace['merge_emc']:
		# EMC
			import spipy

			sub_type = "customed"
			sub_resume = True
			sub_ppn = 2
			try:
				sub_nproc = self.given_runtime['num_proc']
				emc_nthreads = self.given_runtime['num_thread']
				emc_niters = self.given_runtime['iters']
				emc_resume = self.given_runtime['resume']
			except Exception as err:
				utils.show_message("A software bug occurs, please report it.", str(err))
				return None

			config = utils.read_config(config_file)
			chead = self.namespace['config_head']
			# config dict
			emc_config_essential = {'parameters|detd' : config.getfloat(chead, "clen"), 'parameters|lambda' : config.getfloat(chead, "lambda"), \
						'parameters|detsize' : '%d %d' % (config.getint(chead, "det_x"), config.getint(chead, "det_y")), \
						'parameters|pixsize' : config.getfloat(chead, "pix_size"), \
						'parameters|stoprad' : config.getint(chead, "beam_stop"), 'parameters|polarization' : 'x', \
						'emc|num_div' : config.getint(chead, "num_div"), 'emc|need_scaling' : config.getint(chead, "scaling"), \
						'emc|beta' : config.getfloat(chead, "beta"), \
						'emc|beta_schedule' : '%.3f %d' % (config.getfloat(chead, "beta_t"), config.getint(chead, "beta_i")) }
			emc_config_optional = {'parameters|ewald_rad' : config.getfloat(chead, "ewald_rad"), \
						'make_detector|in_mask_file' : config.get(chead, "mask"), \
						'emc|sym_icosahedral' : 0, 'emc|selection' : None, \
						'emc|start_model_file' : None}
			if emc_config_optional['parameters|ewald_rad'] <= 0.0:
				emc_config_optional['parameters|ewald_rad'] = -1.0
			symmetry = config.get(chead, "symmetry").split(',')[1]
			if symmetry == self.namespace['merge_ICOSYM']:
				emc_config_optional['emc|sym_icosahedral'] = 1
			selection = config.get(chead, "data_selection").split(',')[1]
			if selection.lower() != "all":
				emc_config_optional['emc|selection'] = selection
			if config.get(chead, "start_model").lower() != "random":
				emc_config_optional['emc|start_model_file'] = config.get(chead, "start_model")
			emc_parameters = dict(emc_config_essential, **emc_config_optional)

			# from here, stdout is redirected to blackhole
			save_stdout = sys.stdout
			sys.stdout = open(os.devnull, 'w')

			# job_obj.datafile (a list) contains only 1 h5 file, which is exactly the data we need
			# create new project ? or resume
			try:
				# runtime['savepath'] and job_obj.savepath doesn't change, it is pointed to "project dir"
				# sub_workdir change to "emc dir"
				# the project dir structure should like this:
				#     |- project dir
				#     	|- status dir
				#     	|- config.ini
				#       |- runtime.json
				#       |- emc dir
				#         |- ... (real EMC project)
				sub_workdir = os.path.join(runtime['savepath'], job_obj.run_name)
				if not emc_resume:
					if os.path.isdir(runtime['savepath']):
						if not self.force_overwrite:
							re = utils.show_warning("project %s already exists, overwrite it?" % sub_workdir)
							if re == 0:
								return None
						shutil.rmtree(runtime['savepath'])
					os.mkdir(runtime['savepath'])
					spipy.merge.emc.new_project(job_obj.datafile[0], inh5, runtime['savepath'], job_obj.run_name)
				else:
					spipy.merge.emc.use_project(sub_workdir)
				spipy.merge.emc.config(emc_parameters)
				'''
					tmp example : 'mpirun -np 10 ./emc -c config.ini -t 12 (-r) 30'
				'''
				tmp = spipy.merge.emc.run(num_proc=sub_nproc, num_thread=emc_nthreads, iters=emc_niters, nohup=False, resume=emc_resume, cluster=True)
				for tf in glob.glob(os.path.join(sub_workdir, "*.sh")):
					os.remove(tf)
			except Exception as err:
				sys.stdout = save_stdout
				utils.show_message("Fail to create/use project '%s'. Check input parameters !" % job_obj.run_name, str(err))
				return None

			tmp = tmp.strip().split()
			tmp[0] = spipy.info.EMC_MPI
			# prevent any chance of confliction with project 'config.ini'
			tmp[5] = "config_emc.ini"
			shutil.move(os.path.join(sub_workdir, 'config.ini'), os.path.join(sub_workdir, 'config_emc.ini'))
			sub_exec = " ".join(tmp)+";touch status/summary.txt"
			# end of stdout redirection
			sys.stdout.close()
			sys.stdout = save_stdout

		elif job_obj.assignments == self.namespace['phasing_PJ']:
		# phasing

			sub_exec = os.path.join(os.path.split(os.path.realpath(__file__))[0], "scripts", self.python_scripts["pr"])
			# runtime['dataset'] (a list) contains only 1 file, which is exactly the data we need

			# decide mpi rank
			sub_nproc = self.given_runtime['num_proc']

		elif job_obj.assignments == self.namespace['simulation_AS']:
		# atom-scattering simulation

			sub_exec = os.path.join(os.path.split(os.path.realpath(__file__))[0], "scripts", self.python_scripts["as"])
			# runtime['dataset'] (a list) contains only 1 file, which is exactly the data we need

			# decide mpi rank
			sub_nproc = self.given_runtime['num_proc']
			runtime['num_pattern'] = self.given_runtime['num_pattern']

		elif job_obj.assignments == self.namespace['simulation_FFT']:
		# FFT simulation

			sub_exec = os.path.join(os.path.split(os.path.realpath(__file__))[0], "scripts", self.python_scripts["fs"])
			# runtime['dataset'] (a list) contains only 1 file, which is exactly the data we need

			# decide mpi rank
			sub_nproc = 1
			runtime['num_pattern'] = self.given_runtime['num_pattern']

		elif job_obj.assignments in [self.namespace['classify_DCPS'], self.namespace['classify_TSNE']]:
		# manifold decomposition

			if job_obj.assignments == self.namespace['classify_DCPS']:
				sub_exec = os.path.join(os.path.split(os.path.realpath(__file__))[0], "scripts", self.python_scripts["sp"])
			else:
				sub_exec = os.path.join(os.path.split(os.path.realpath(__file__))[0], "scripts", self.python_scripts["ts"])

			# runtime['dataset'] (a list) contains only 1 file, which is exactly the data we need
			runtime['inh5'] = self.given_runtime['inh5']
			runtime['mask'] = self.given_runtime['mask']
			runtime['benchmark'] = self.given_runtime['benchmark']
			# decide mpi rank
			fp = h5py.File(runtime['dataset'][0], 'r')
			num = fp[runtime['inh5']].shape[0]
			fp.close()
			gp_size = int(utils.read_config(config_file, [self.namespace['config_head'], 'group_size']))
			sub_nproc = num // gp_size

		else:
			return None


		'''
			prepare and submit jobs : 
				job_dir/status stores all status_xxx.txt of every process
				and there will be a status.txt if the job finished
		'''
		try:
			# make workdir if not exists
			if not os.path.isdir(runtime['savepath']):
				os.mkdir(runtime['savepath'])
				os.mkdir(os.path.join(runtime['savepath'], 'status'))
			else:
				if not sub_resume:
					if not self.force_overwrite:
						re = utils.show_warning("project %s already exists, overwrite it?" % runtime['savepath'])
						if re == 0:
							return None
					shutil.rmtree(runtime['savepath'])
					os.mkdir(runtime['savepath'])
					os.mkdir(os.path.join(runtime['savepath'], 'status'))
				else:
					tmp = os.path.join(runtime['savepath'], 'status')
					if os.path.exists(tmp):
						shutil.rmtree(tmp)
					tmp = os.path.join(runtime['savepath'], 'config.ini')
					if os.path.exists(tmp):
						os.remove(tmp)
					tmp = os.path.join(runtime['savepath'], 'runtime.json')
					if os.path.exists(tmp):
						os.remove(tmp)
					os.mkdir(os.path.join(runtime['savepath'], 'status'))
			# write runtimejson and config to workdir
			with open(os.path.join(runtime['savepath'], 'runtime.json'), 'w') as rjson:
				json.dump(runtime, rjson)
			shutil.copyfile(config_file, os.path.join(runtime['savepath'], 'config.ini'))
			# write submit script to workdir
			SUB_cmd = subprocess.check_output("bash %s -s '%s' -t %d -p %s -q %s -y %s -z %d -e %s" % \
				(sub_prepare_sub_script, sub_exec, sub_nproc, sub_workdir, sub_submit_queue, sub_jss, sub_ppn, sub_type), shell=True )
			SUB_cmd = SUB_cmd.strip("\n")
			if len(SUB_cmd) == 0:
				return None
			# submit
			pobj = utils.submit_job(sub_workdir, SUB_cmd, self.jss)
			# return
			if pobj is None:
				return None
			else:
				return pobj
		except Exception as err:
			utils.show_message("Error !", str(err))
			return None


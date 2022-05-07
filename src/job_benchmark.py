from PyQt4 import QtGui
from PyQt4 import QtCore

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'pygui'))

import shutil
import glob
import subprocess
import json
import h5py
import matplotlib.pyplot as plt
import numpy as np
import argparse

import utils
from benchmark_gui import Ui_Dialog as benchmark_Dialog


def plot_classify_features(hdf5_path):
	fp = h5py.File(hdf5_path, 'r')
	features = fp['features'][...]
	labels = fp['labels'][...]
	fp.close()
	ndim = features.shape[-1]
	nlabel = len(set(labels.tolist()))
	if len(features.shape) == 1 or ndim == 1:
		plt.subplot(1,2,1)
		plt.plot(features, 'b.')
		plt.ylabel("feature_1")
		plt.title("Features Scatter Plot")
	else:
		plt.subplot(1,2,1)
		yall = list(set(labels.tolist()))
		y1 = np.where(labels == yall[0])[0]
		y2 = np.where(labels == yall[1])[0]
		plt.scatter(features[y1,0], features[y1,1], c='m')
		plt.scatter(features[y2,0], features[y2,1], c='b')
		plt.xlabel("feature_1")
		plt.ylabel("feature_2")
		plt.title("Feature Space Plot")
	plt.subplot(1,2,2)
	try:
		plt.hist(labels, bins=nlabel, facecolor='g', alpha=0.75)
	except:
		plt.hist(labels, bins=nlabel, facecolor='g', alpha=0.75)
	plt.xlabel("label")
	plt.ylabel("Count")
	plt.text(1.5,0.5,'N=%d'%len(labels))
	plt.title("Labels Statistics")
	plt.show()




class JobBenchmark(QtGui.QDialog, QtCore.QEvent):

	def __init__(self, runtime, config, main_gui, title="Job Benchmark"):
		'''
			runtime keys: 
				dataset
				assignments
				mask
				inh5
				savepath
		'''
		QtGui.QWidget.__init__(self)
		# setup ui
		self.ui = benchmark_Dialog()
		self.ui.setupUi(self)
		# father
		self.main_gui = main_gui
		# trigger
		self.ui.pushButton.clicked.connect(self.kill_job)
		self.ui.pushButton_2.clicked.connect(self.run)
		self.ui.pushButton_3.clicked.connect(self.plot)

		# runtime
		self.assignments = runtime['run_name']
		self.dataset     = runtime['dataset']    # list with 1 item
		if runtime.has_key('mask'):
			self.mask    = runtime['mask']
		if runtime.has_key('inh5'):
			self.inh5    = runtime['inh5']
		self.savepath    = runtime['savepath']

		# description
		description = self.assignments
		if runtime.has_key('mask'):
			if self.mask is None:
				description = description + ", without mask"
			else:
				description = description + ", with mask"
		if runtime.has_key('inh5'):
			description = description + ", inh5 = %s" % self.inh5
		self.ui.lineEdit.setText(description)

		# savepath
		if os.path.exists(self.savepath):
			shutil.rmtree(self.savepath)
		try:
			os.mkdir(self.savepath)
			os.mkdir(os.path.join(self.savepath, "status"))
			# write config, see jobc.py line 843
			utils.write_config(os.path.join(self.savepath, "config.ini"), config)
			self.config = config.values()[0]
			# write runtimejson and config to workdir, see jobc.py line 841
			with open(os.path.join(self.savepath, 'runtime.json'), 'w') as rjson:
				json.dump(runtime, rjson)
			self.ui.lineEdit_2.setText(self.savepath)
		except:
			utils.show_message("Job path is invalid !")
			self.close()
			return

		# timer id
		self.timer_id = None
		# jid
		self.jid = None
		# locations of run-scripts
		self.python_scripts = utils.get_scripts()

		# title
		self.setWindowTitle(title)


	def showWindow(self):
		self.exec_()


	def closeEvent(self, event):
		try:
			utils.kill_job(None, self.pobj)
		except:
			pass
		self.close()


	def timerEvent(self, event):
		try:
			self.__load_log()
			recode = self.pobj.poll()
			if recode is not None:
				self.killTimer(self.timer_id)
				return
		except Exception as err:
			raise RuntimeError(str(err))


	def __load_log(self):
		savepath = self.savepath
		stdout = glob.glob(os.path.join(savepath, "*.out"))
		if len(stdout) == 0:
			utils.show_message("Cannot find any log file in project")
			self.killTimer(self.timer_id)
			return
		with open(stdout[0]) as fp:
			lines = fp.readlines()
		# insert text
		prev_text = self.ui.plainTextEdit.toPlainText()
		insertText = "".join(lines)[len(prev_text):].strip("\n")
		if len(insertText.strip("\n").strip()) == 0:
				return
		self.ui.plainTextEdit.appendPlainText(insertText)


	def kill_job(self):
		success = utils.kill_job(None, self.pobj)
		if not success:
			utils.show_message("Fail to kill this job !")


	def run(self):
		njob = 1   # remember to change the default value !
		# determine job type, if you want to extend functions, update here
		if self.assignments == self.main_gui.namespace['classify_DCPS']:
			# manifold calssification
			scripts = os.path.join(os.path.split(os.path.realpath(__file__))[0], "scripts", self.python_scripts['sp'])
			# determine job amount
			fp = h5py.File(self.dataset[0], 'r')
			num = fp[self.inh5].shape[0]
			fp.close()
			njob = num // self.config['group_size']
		elif self.assignments == self.main_gui.namespace['classify_TSNE']:
			# tsne calssification
			scripts = os.path.join(os.path.split(os.path.realpath(__file__))[0], "scripts", self.python_scripts['ts'])
			# determine job amount
			fp = h5py.File(self.dataset[0], 'r')
			num = fp[self.inh5].shape[0]
			fp.close()
			njob = num // self.config['group_size']
			if njob == 0:
				njob = 1
		else:
			return
		# chdir
		prev_dir = os.getcwd()
		os.chdir(self.savepath)

		# cmd
		cmd = "mpirun -n %d python -W ignore %s runtime.json config.ini 1>>job.out 2>>job.out" % (njob, scripts)
		# run
		self.timer_id = self.startTimer(1000)
		try:
			self.ui.plainTextEdit.clear()
			self.pobj = subprocess.Popen(cmd, shell=True, preexec_fn=os.setsid)
		except Exception as err:
			utils.show_message("Fail to run job !", str(err))

		os.chdir(prev_dir)


	def plot(self):
		if self.pobj is None or self.pobj.poll() != 0:
			utils.show_message("The Job is not finished yet !")
			return
		if self.assignments in [self.main_gui.namespace['classify_DCPS'], self.main_gui.namespace['classify_TSNE']]:
			h5file = os.path.join(self.savepath, "output.h5")
			cmd = "python %s --type 0 %s" % (os.path.join(os.path.dirname(__file__), "job_benchmark.py"), h5file)
			subprocess.check_call(cmd, shell=True)



if __name__ == '__main__':
	
	parser = argparse.ArgumentParser()
	parser.add_argument("--type", type=int, help="0 : plot classification features")
	parser.add_argument("file", type=str, help="hdf5 file to open")
	args = parser.parse_args()

	if args.type == 0:
		plot_classify_features(args.file)
	else:
		sys.exit(1)






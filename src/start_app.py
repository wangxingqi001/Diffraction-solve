from PyQt4 import QtGui
from PyQt4 import QtCore
from PyQt4 import QtWebKit

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'pygui'))

import subprocess
import shutil
from ConfigParser import ConfigParser

import utils
from start_gui import Ui_StartWindow
from main_app import SPIPY_MAIN


class SPIPY_START(QtGui.QMainWindow, QtCore.QEvent):

	def __init__(self, parent=None):
		QtGui.QWidget.__init__(self, parent)
		# find spipy
		try:
			import spipy
		except:
			utils.show_message("I cannot find spipy package in your Python !")
			return
		# setup ui
		self.ui = Ui_StartWindow()
		self.ui.setupUi(self)
		self.ui.lineEdit.setText('')
		self.ui.lineEdit_2.setText('')
		self.ui.comboBox.setCurrentIndex(0)
		# setup workdir browser
		self.connect(self.ui.pushButton, QtCore.SIGNAL(("clicked()")), self.workdir)
		self.connect(self.ui.pushButton_2, QtCore.SIGNAL(("clicked()")), self.OK)
		self.connect(self.ui.pushButton_3, QtCore.SIGNAL(("clicked()")), self.datadir)
		# other attributes
		self.dirname = None
		self.datadir = None
		self.jss = None
		self.format_index = None
		self.subDir = False
		self.job_control = None
		# read namespace
		self.namespace = utils.read_ini()
		self.job_control = [self.namespace['process_pat_per_job'], self.namespace['max_jobs_per_run']]
		# set jss ui
		for jss in self.namespace['JSS_support']:
			self.ui.comboBox.addItem(jss)
		# set up data format
		for fmt in self.namespace['data_format']:
			self.ui.comboBox_2.addItem(fmt)
		# set mainapp
		self.mainapp = SPIPY_MAIN()

	#def closeEvent(self, QCloseEvent):
	#	QtGui.qApp.quit()

	def datadir(self):
		dirname = str(QtGui.QFileDialog(self).getExistingDirectory())
		if not os.path.isdir(dirname):
			self.datadir = None
		self.datadir = dirname
		self.ui.lineEdit_2.setText(self.datadir)

	def workdir(self):
		dirname = str(QtGui.QFileDialog(self).getExistingDirectory())
		if os.path.isdir(dirname):
			# if it is a existing project
			if os.path.exists(os.path.join(dirname, self.namespace['ini'])):
				try:
					config_name = self.namespace['project_ini'][0]
					config_item = self.namespace['project_ini'][1].split(',')
					config = utils.read_config(os.path.join(dirname, self.namespace['ini']))
					# jss
					jss = config.get(config_name, config_item[1])
					if jss in self.namespace['JSS_support']:
						self.ui.comboBox.setCurrentIndex(self.namespace['JSS_support'].index(jss))
						self.jss = jss
					else:
						self.ui.comboBox.setCurrentIndex(0)
						self.jss = None
					# datapath
					datapath = config.get(config_name, config_item[0])
					self.ui.lineEdit_2.setText(datapath)
					self.datadir = datapath
					# format
					format_index = config.getint(config_name, config_item[2])
					self.ui.comboBox_2.setCurrentIndex(format_index)
					# sub dir
					subDir = config.getboolean(config_name, config_item[3])
					if subDir == True:
						self.ui.checkBox.setCheckState(2)
					else:
						self.ui.checkBox.setCheckState(0)
					# job control
					self.job_control[0] = int(config.get(config_name, config_item[4]))
					self.job_control[1] = int(config.get(config_name, config_item[5]))
				except:
					utils.show_message("There seems to be some problems in your 'project.ini' at project directory.")
					return
			self.dirname = dirname
			self.ui.lineEdit.setText(self.dirname)
		else:
			self.dirname = None

	def OK(self):
		if self.datadir is None:
			utils.show_message("Please choose data directory !")
			return False
		if self.dirname is None:
			utils.show_message("Please choose work directory !")
			return False

		# check & set jss
		if str(self.ui.comboBox.currentText()).strip()[:2] == "NO":
			self.jss = None
		elif str(self.ui.comboBox.currentText()).strip()[:3] == "PBS":
			# check if PBS is working
			if not utils.check_PBS():
				utils.show_message("NO PBS detected !")
				return False
			self.jss = "PBS"
		elif str(self.ui.comboBox.currentText()).strip()[:3] == "LSF":
			# check if LSF is working
			if not utils.check_LSF():
				utils.show_message("NO LSF detected !")
				return False
			self.jss = "LSF"
		else:
			utils.show_message("I can not recognize the job submitting system")
			return False

		# check data format
		self.format_index = self.ui.comboBox_2.currentIndex()

		# check subDir
		if self.ui.checkBox.checkState() == QtCore.Qt.Checked:
			self.subDir = True

		# check data dir
		if not os.path.exists(os.path.join(self.datadir)):
			utils.show_message("Data directory is invalid !")
			return False
		else:
			stat = utils.check_datadir(self.datadir, self.format_index, self.namespace['data_format'], self.subDir)
			# checking results, interact with user
			if stat[0] == 0 and stat[1] == 0:
				re = utils.show_warning("I can not find any %s files in the data folder, still go on ?" \
								% self.namespace['data_format'][self.format_index])
				if re == 1:
					pass
				else:
					return False
			elif stat[0] == -1 and stat[1] == -1:
				utils.show_message("Some error occurred ! Please report it")
				return False
			elif stat[0] == 1:
				if stat[1] == 1:
					pass
				elif stat[1] == -1:
					subdre = ''
					if self.subDir:
						subdre = ' no'
					re = utils.show_warning("It seems there are%s sub directories in data folder, still go on ?" % subdre)
					if re == 1:
						pass
					else:
						return False
				else:
					utils.show_message("Some error occurred ! Please report it")
					return False
			elif type(stat[0]) == str:
				if stat[1] == 1:
					re = utils.show_warning("It seems the data format is '%s', still use '%s' ?" \
								% (stat[0], self.namespace['data_format'][self.format_index]) )
					if re == 1:
						pass
					else:
						return False
				elif stat[1] == -1:
					subdre = ''
					if self.subDir:
						subdre = ' no'
					re = utils.show_warning("It seems there are%s sub directories in data folder, \
								and the data format is '%s', still go on and use '%s' ?" \
								% (subdre, stat[0], self.namespace['data_format'][self.format_index]) )
					if re == 1:
						pass
					else:
						return False
				else:
					utils.show_message("Some error occurred ! Please report it")
					return False

		# make dir
		# if not os.path.exists(os.path.join(self.dirname, self.namespace['ini'])):
		self.makedirs()
		# copy darkcal to Process/config
		project_darkcal_path = os.path.join(self.dirname, self.namespace['project_structure'][0], "config/darkcal_default.ini")
		if not os.path.exists(project_darkcal_path):
			shutil.copyfile(os.path.join(os.path.split(os.path.realpath(__file__))[0], "darkcal.ini"), project_darkcal_path )
		subprocess.check_call( "ln -fs %s %s" % ( project_darkcal_path, \
			os.path.join(self.dirname, self.namespace['project_structure'][0], "config/darkcal.ini") ), shell=True )

		# write project.ini
		config_name = self.namespace['project_ini'][0]
		config_item = self.namespace['project_ini'][1].split(',')
		utils.write_config(os.path.join(self.dirname, self.namespace['ini']),\
				{config_name:{config_item[0]:self.datadir, config_item[1]:self.jss, \
				config_item[2]:self.format_index, config_item[3]:self.subDir, \
				config_item[4]:self.job_control[0], config_item[5]:self.job_control[1]}}, 'w')

		utils.print2projectLog(self.dirname, "Select data dir %s" % self.datadir)
		utils.print2projectLog(self.dirname, "Choose work dir %s" % self.dirname)
		utils.print2projectLog(self.dirname, "Spipy GUI opened successfully")

		# close and open main gui
		self.mainapp.setup(self.dirname, self.datadir, self.jss, self.subDir, self.format_index)
		self.mainapp.show()
		self.close()

	def makedirs(self):
		if self.dirname is None:
			return
		for n in self.namespace['project_structure']:
			path = os.path.join(self.dirname, n)
			if not os.path.exists(path):
				os.mkdir(path)
			path2 = os.path.join(path, "config")
			if not os.path.exists(path2):
				os.mkdir(path2)
			if self.namespace.has_key('%s_assignments' % n.lower()):
				for m in self.namespace['%s_assignments' % n.lower()]:
					path2 = os.path.join(path, m)
					if not os.path.exists(path2):
						os.mkdir(path2)
		geom = os.path.join(self.dirname, self.namespace['project_structure'][5], "geom")
		if not os.path.exists(geom):
			os.mkdir(geom)
		mask = os.path.join(self.dirname, self.namespace['project_structure'][5], "mask")
		if not os.path.exists(mask):
			os.mkdir(mask)


if __name__ == "__main__":
    import sys
    app = QtGui.QApplication(sys.argv)
    myapp = SPIPY_START()
    myapp.show()
    app.exec_()

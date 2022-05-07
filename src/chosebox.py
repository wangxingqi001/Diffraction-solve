from PyQt4 import QtGui
from PyQt4 import QtCore
from PyQt4 import QtWebKit

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'pygui'))

import utils
from chosebox_gui import Ui_Chosebox


class chosebox(QtGui.QDialog, QtCore.QEvent):

	def __init__(self, label, choices, ret, title=None):
		QtGui.QWidget.__init__(self)
		# setup ui
		self.ui = Ui_Chosebox()
		self.ui.setupUi(self)
		self.ui.label.setText(label)
		for item in choices:
			self.ui.comboBox.addItem(item)
		if title is not None:
			self.setWindowTitle(title)
		# return obj
		self.ret = ret
		# setup trigger
		self.connect(self.ui.pushButton, QtCore.SIGNAL(("clicked()")), self.OK)
		self.connect(self.ui.pushButton_2, QtCore.SIGNAL(("clicked()")), self.Cancel)


	def OK(self):
		self.ret[0] = self.ui.comboBox.currentIndex()
		self.close()

	def Cancel(self):
		self.close()
		

def show_chosebox(label, choices, ret, title=None):
	# returned 'ret' is seleted index in choices
	# if 'cancel' then ret will be the same as its original value
	cb = chosebox(label, choices, ret, title)
	cb.setModal(True)
	cb.exec_()
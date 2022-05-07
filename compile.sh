# !/bin/bash

set -e

root_folder=`pwd`

cd $root_folder/ui
bash ./compile_ui.sh

cd $root_folder
if [ -f "spipy.gui" ];then
	rm spipy.gui
fi

touch spipy.gui
echo '# !/bin/bash' >> spipy.gui
echo 'if [ -z $1 ];then' >> spipy.gui
echo '	SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"' >> spipy.gui
echo '	/usr/bin/env python -W ignore $SCRIPTPATH/src/start_app.py' >> spipy.gui
echo 'else' >> spipy.gui
echo '	echo "**************************************"' >> spipy.gui
echo '	echo "GUI of spipy program, VERSION 1.0"' >> spipy.gui
echo '	echo "	GNU LICENCE v3.0 "' >> spipy.gui
echo '	echo "	Compatible with spipy version 2.x "' >> spipy.gui
echo '	echo "Contributor :"' >> spipy.gui
echo '	echo "	shiyingchen, CSRC"' >> spipy.gui
echo '	echo "	             Tsinghua Univ."' >> spipy.gui
echo '	echo "**************************************"' >> spipy.gui
echo 'fi' >> spipy.gui
chmod u+x spipy.gui
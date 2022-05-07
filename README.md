# spipy GUI
#### Designed for researchers to use XFEL SPI (single particle imaging) python toolkits (spipy) easily and friendly. Linux and MacOS are supported.

## Install

1. Download and install [spipy](https://github.com/LiuLab-CSRC/spipy) first.
2. Install PyQt4 using conda:

```shell
bogon:~ myuser$ conda install pyqt=4.11
```

3. Download [spipy GUI](https://github.com/estonshi/spipy_gui/archive/v1.0.zip), and run:

```shell
bogon:~ myuser$ unzip spipy_gui-1.0.zip
bogon:~ myuser$ cd spipy_gui-1.0
bogon:~ myuser$ ./compile.sh
```

## Running GUI
```
# Get version information
bogon:~ myuser$ ./spipy.gui -v

# Run GUI
bogon:~ myuser$ ./spipy.gui
```

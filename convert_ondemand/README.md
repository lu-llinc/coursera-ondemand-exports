# Coursera on-demand data conversion script

* written by: Jasper Ginn
* date: 27-11-2015
* Email: j.h.ginn[at]cdh.leidenuniv.nl

## Introduction

The `convert_ondemand.py` script in this directory is a python script that converts the Coursera export data to a PostGreSQL database.

## Questions/Feedback

Please send any feedback/questions to: j.h.ginn[at]cdh.leidenuniv.nl

## Steps

These steps presume that you have installed the dependencies for Python and have installed PostGreSQL on your system. If you have not yet done so, please see [this README file](https://github.com/JasperHG90/coursera-ondemand-exports/blob/master/README.md) for more information.

1) Extract the compressed CSV and HTML files in a single folder on your computer.
2) Modify details of the `convert_ondemand_config.py` file such that it points towards this folder and contains the necessary details to connect to the PostGreSQL server.
	
	* You do not need to add a '/' at the end of the folder path. 
	* You do also not need to create a database. The script will create one for you and drop any existing databases by that name. To specify the name of the PostGreSQL database, see the `convert_ondemand_config.py` file.
	* By default, logging is enabled. To disable logging, change the value after `log` from `True` to `False`. If you enable logging, make sure to change the file path for the log output.

3) Run the script by running `python convert_ondemand.py` in a terminal.




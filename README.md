# [DEPRECEATED] Convert Coursera on-demand export data to a PostGreSQL database

** This program is no longer maintained because Coursera supplies a more convenient alternative with data exports **

* Written by: Jasper Ginn
* Email: j.h.ginn[at]fgga.leidenuniv.nl

## Feedback/Questions

If you have any questions or feedback, please contact me at: j.h.ginn[at]cdh.leidenuniv.nl

## Introduction

This repository contains a python script to convert on-demand export data to a PostGreSQL database. It further contains an R script showing how to connect to the database. More scripts will be added later as I spend more time on analyzing the data.

You can find more information on the python conversion script in the `convert_ondemand` folder.

### Why PostGreSQL?

If you look at the headers that are supplied in the .html files, you see that some of these contain header specifications that are illegal in e.g. MySQL. Additionally, PostGreSQL can import CSV files pretty painlessly. Of course, you can choose to not use these header files and use another database.

## Dependencies

### PostGreSQL

To install PostGreSQL, please refer to the [official documentation](https://wiki.postgresql.org/wiki/Detailed_installation_guides)

Then, follow the 'first steps' tutorial [here](https://wiki.postgresql.org/wiki/First_steps)

### Python

Python version: 2.7.9

The python script to convert the data depends on the following modules:

* BeautifulSoup 4 (tested with version == 4.3.2)
* psycopg (tested with version == 2.6.1)

You can install these modules with [easy_install](https://pypi.python.org/pypi/setuptools) or [pip](https://pypi.python.org/pypi/pip). To install pip, please see [this link](http://pip.readthedocs.org/en/stable/installing/) (NB: To install psycopg2 in Ubuntu, you need to first do: sudo apt-get install python-dev)

Alternatively, you can install [Anaconda](https://www.continuum.io/downloads).

## Dumping your data

These steps presume that you have installed the dependencies for Python and have installed PostGreSQL on your system. If you have not yet done so, please see for more information.

1. Extract the compressed CSV and HTML files in a single folder on your computer.
2. Modify details of the `convert_ondemand_config.py` file such that it points towards this folder and contains the necessary details to connect to the PostGreSQL server.
	
	* You do not need to add a '/' at the end of the folder path. 
	* You do also not need to create a database. The script will create one for you and drop any existing databases by that name. To specify the name of the PostGreSQL database, see the `convert_ondemand_config.py` file.

3. Run the script by running `python convert_ondemand.py` in a terminal.




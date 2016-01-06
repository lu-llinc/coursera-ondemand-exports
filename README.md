# Convert Coursera on-demand export data to a PostGreSQL database

* Written by: Jasper Ginn
* Date: 27-11-2015
* Email: j.h.ginn[at]cdh.leidenuniv.nl

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

### R

The R script depends on the `RPostGreSQL` package. Installation instructions can be found in the script.


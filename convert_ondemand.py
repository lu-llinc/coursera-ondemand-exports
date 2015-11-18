#!/usr/bin/env python2.7
# encoding: utf-8

'''
Convert the on-demand course exports to a sqlite table
'''

import bs4 as BeautifulSoup
import convert_ondemand_config as config
import sqlite3
import os
import pandas
from time import localtime, strftime

'''

This script takes on-demand data exports from coursera and dumps it in a sqlite database.

You need to create two folders:
	1) Folder with html files provided with the exports
	2) Folder with csv files provided with the exports

Specify the file paths to these folders in the 'convert_ondemand_config.py' file 

The third line in the config file specifies the location and name of the sqlite database.

	Example: "/home/jasper/Desktop/federalism.db" <-- Do not forget the '.db'.

The fourth line specifies the location of a simple file for logging

Run this script using the command 'python convert_ondemand.py'

'''

class scraper:

	def __init__(self, fileN):
		self.file = fileN

	def scrape(self):
		# Soup page
		soup = BeautifulSoup.BeautifulSoup(open(self.file).read(), "html.parser")
		# Find SQL statement
		try:
			return soup.find("pre").text
		except AttributeError:
			return None

class insert_SQL:

	def __init__(self, conn):
		self.conn = conn

	def insert_headers(self, SQLstatement):
		self.statement = SQLstatement
		c = self.conn.cursor()
		c.execute(self.statement)
		self.conn.commit()
		return None

class logger:

	def __init__(self, location):
		self.location = location

	def logMessage(self, loglevel, message):
		self.loglevel = loglevel
		self.message = message
		self.time = strftime("%Y-%m-%d %H:%M:%S", localtime())
		# Write
		with open(self.location, 'a') as f:
			f.write("{}	{}	{}".format(self.time, self.loglevel, self.message))
			f.write("\n")

class conversion:

	def __init__(self, format_):
		self.format = format_

	

def convertData(p, conn):
	if "readme" in p:
		continue
	try:
		# load data
		df = pandas.read_csv(config.path_to_data + "/" + p + ".csv")
		# Initiate scraper and scrape
		scr = scraper(config.path_to_variables + "/" + p + ".html").scrape()
		# Create SQL tables
		insert_SQL(conn).insert_headers(scr)
		# Insert data
		df.to_sql(name = p, con = conn, if_exists = 'append', index=False)
	except (pandas.parser.CParserError, sqlite3.ProgrammingError, sqlite3.IntegrityError) as error:
		print "ERROR: {}".format(type(error))
		log = logger(config.logger_location)
		log.logMessage("ERROR", "Could not process file {} file due to following error {}".format(p, type(error)))
		# If CPparserError, try ...
		if str(type(error)) == "<class 'pandas.parser.CParserError'>":
			try:
				# Read line without errors
				df = pandas.read_csv(config.path_to_data + "/" + p + ".csv", error_bad_lines=False)
				df.to_sql(name = p, con = conn, if_exists = 'append', index=False)
			except (sqlite3.ProgrammingError, sqlite3.IntegrityError) as error:
				print "ERROR: {}".format(type(error))
				log.logMessage("ERROR", "Could not process file {} file due to following error {}".format(p, type(error)))

# Call

if __name__ == "__main__":
	# Get file names
	files = [fileN.replace(".html", "") for fileN in os.listdir(config.path_to_variables)]
	# Connect to db
	conn = sqlite3.connect(config.SQL_database_name)
	# Get sql statements for each file
	for file_ in files:


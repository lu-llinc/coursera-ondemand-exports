#!/usr/bin/env python2.7
# encoding: utf-8

'''
Convert the on-demand course exports to a postgresql table
'''

# Written by: Jasper Ginn
# Copyright @ Leiden University
# Last mod: 27-11-2015

'''

This script takes on-demand data exports from coursera and dumps it in a postgresql database.

You need to unzip the export file and store the html AND the CSV files in a folder. 

Specify the parameters of this script in the 'convert_ondemand_config.py' file.

Run this script using the command 'python convert_ondemand.py'

See the README.md file in this directory for more information.

'''

''' COPYRIGHT INFORMATION

The MIT License (MIT)

Copyright (c) 2015 Leiden University

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

'''

import bs4 as BeautifulSoup
import convert_ondemand_config as config
import psycopg2
import os
from time import localtime, strftime

# SCRAPER

class scraper:

	def __init__(self, folder, file_):
		self.filePath = "{}/{}.html".format(folder,file_)

	def scrape(self):
		# Soup page
		soup = BeautifulSoup.BeautifulSoup(open(self.filePath).read(), "html.parser")
		# Find SQL statement
		try:
			return soup.find("pre").text
		except AttributeError:
			return None

# POSTGRES

class postgresql:

	def __init__(self, database, user, password, host):
		self.database = database
		self.host = host
		self.user = user
		self.password = password

	def psql_connect(self, db = True):
		# If db == True, then connect to specific db specified in self.database
		if db:
			try:
				conn = psycopg2.connect(dbname = self.database, user = self.user, host = self.host, password = self.password)
				# Set isolation level
				conn.set_isolation_level(0)
			except:
				print "ERROR: Could not connect to database {}. Check settings in config file.".format(self.database)
				if config.log:
					log.logMessage("POSTGRES-CONNECTERROR", "ERROR: Could not connect to database {}. Check settings in config file.".format(self.database))
				return None
		# Else, db == False. This is called when db is created.
		else:
			try:
				conn = psycopg2.connect(user = self.user, host = self.host, password = self.password)
				# Set isolation level
				conn.set_isolation_level(0)
			except:
				print "ERROR: Could not connect to postgresql. Check settings in config file."
				if config.log:
					log.logMessage("POSTGRES-CONNECTERROR", "Could not connect to postgresql. Check settings in config file.")

		return conn
		
	def create_database(self):
		# Connect
		conn = self.psql_connect(db=False)

		with conn:
			c = conn.cursor()
			c.execute("""DROP DATABASE IF EXISTS {}""".format(self.database))
			c.execute("""CREATE DATABASE {}""".format(self.database))
			conn.commit()

		if conn:
			conn.close()

	def insert_headers(self, header):
		# Connect
		conn = self.psql_connect(db=True)

		with conn:
			try:
				c = conn.cursor()
				c.execute(header)
				conn.commit()
			except psycopg2.ProgrammingError as e:
				print "ERROR: Could not send header for {} to database {}. Postgres returned error 'psycopg2.ProgrammingError'".format(file_, self.database)
				if config.log:
					log.logMessage("POSTGRES-QUERYERROR", "Could not send header for {} to database {}. Postgres returned error 'psycopg2.ProgrammingError'".format(file_, self.database))
				return False

		if conn:
			conn.close()

		return None

	def insert_data(self, folder, file_):
		# Connect
		conn = self.psql_connect(db=True)

		with conn:
			c = conn.cursor()
			# remove csv headers
			helpers(folder).remove_headers_csv(file_)
			# Copy data to PostGres table
			try:
				c.execute("""COPY {} FROM '{}/{}_temp.csv' CSV DELIMITER ',' NULL '' QUOTE '"' ESCAPE '\\' HEADER;""".format(file_, folder, file_))
				print "TRUE"
				if config.log:
					log.logMessage("SUCCESS", "Successfully inserted data from dataset {} into {}.".format(file_, self.database))
				conn.commit()
			except (psycopg2.DataError, psycopg2.ProgrammingError) as e:
				if str(type(e)) == '<class psycopg2.ProgrammingError>':
					print "ERROR: could not insert data for {} into {}. Table does not exist.".format(file_, self.database)
					if config.log:
						log.logMessage("POSTGRES-MISSINGTABLE", "could not insert data for {} into {}. Table does not exist.".format(file_, self.database))
				else:
					print "ERROR: could not insert data for {} into {}. This is most likely due to 'extra data after last expected column' error.".format(file_, self.database)
					if config.log:
						log.logMessage("CSV-EOFERROR", "could not insert data for {} into {}. This is most likely due to 'extra data after last expected column' error.".format(file_, self.database))
			# Delete file
			os.remove("{}/{}_temp.csv".format(folder, file_))

		if conn:
			conn.close()

		return None

class helpers:

	def __init__(self, folder):
		self.folder = folder

	def unique_files(self):
		files = os.listdir(self.folder)
		count = 0
		for file_ in files:
			for exs in [".html", ".csv"]:
				if exs in file_:
					files[count] = file_.replace(exs, "")
					count += 1
		return(set(files))

	def near_empty_files(self, file_):
		self.file_ = file_
		try:
			num_lines = sum(1 for line in open("{}/{}.csv".format(self.folder, self.file_)))
			if num_lines <= 2:
				return True
			else:
				return False
		except IOError:
			print "ERROR: could not open {}/{}.csv. File does not exist.".format(self.folder, self.file_)
			if config.log:
				log.logMessage("CSV-DOESNOTEXIST", "could not open {}/{}.csv. File does not exist.".format(self.folder, self.file_))
			return True

	def remove_headers_csv(self, file_):
		self.file_ = file_
		# Open csv file and delete header
		try:
			# Remove header and save data in temporary file
			with open(r"{}/{}.csv".format(self.folder, self.file_), 'r') as f:
				with open(r"{}/{}_temp.csv".format(self.folder, self.file_), 'w') as f1:
					next(f)
					for line in f:
						f1.write(line)
		except:
			print "ERROR: could not open {}/{}.csv. Check if path_to_data in config file is correct.".format(self.folder, self.file_)
			if config.log:
				log.logMessage("CSV-OPENERROR", "could not open {}/{}.csv. Check if path_to_data in config file is correct.".format(self.folder, self.file_))
			return(None)

# Simple logging function

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

# Call

if __name__ == "__main__":
	# Set up log
	if config.log:
		log = logger(config.log_location)
		log.logMessage("INFO", "started log")
	# Get file names
	files = helpers(config.path_to_files).unique_files()
	# Create database
	psql = postgresql(config.postgres_database_name, config.postgres_user, config.postgres_pwd, config.postgres_host)
	psql.create_database()
	# Get sql statements for each file
	for file_ in files:
		if file_ == "readme" or helpers(config.path_to_files).near_empty_files(file_) == True:
			continue
		# Initiate scraper and scrape
		scr = scraper(config.path_to_files, file_).scrape()
		print scr
		# Create SQL tables
		psql.insert_headers(scr)
		# Insert CSV data
		psql.insert_data(config.path_to_files, file_)
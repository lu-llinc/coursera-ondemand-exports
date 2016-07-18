#!/usr/bin/env python2.7
# encoding: utf-8

''' COPYRIGHT INFORMATION
Copyright (C) 2015  Leiden University
This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.
This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.
You should have received a copy of the GNU General Public License
along with this program.  If not, see [http://www.gnu.org/licenses/].
'''

# Written by: Jasper Ginn
# Copyright @ Leiden University
# Last mod: 17-05-2016

'''
Convert the on-demand course exports to a postgresql table:
This script takes on-demand data exports from coursera and dumps it in a postgresql database.
You need to unzip the export file and store the html AND the CSV files in a folder.
Specify the parameters of this script in the 'convert_ondemand_config.py' file.
Run this script using the command 'python convert_ondemand.py'
See the README.md file in this directory for more information.
'''

import bs4 as BeautifulSoup
import convert_ondemand_config as config
import psycopg2
import os
from time import localtime, strftime
import re

# SCRAPER

class scraper:

	def __init__(self, folder, file_):
		self.filePath = "{}/{}.html".format(folder,file_)

	def scrape(self):
		# Soup page
		soup = BeautifulSoup.BeautifulSoup(open(self.filePath).read(), "html.parser")
		# Find SQL statement
		try:
			query = soup.find("pre").text
			# Temporary fix to eliminate foreign and primary key contstraints before inserting data
			return re.sub('((,FOREIGN KEY)|(,PRIMARY KEY)).*','',query)
			#return query
		except AttributeError as e:
			print e
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
			except psycopg2.OperationalError as e:
				print e
		# Else, db == False. This is called when db is created.
		else:
			try:
				conn = psycopg2.connect(user = self.user, host = self.host, password = self.password)
				# Set isolation level
				conn.set_isolation_level(0)
			except psycopg2.OperationalError as e:
				print e

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
				print e

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
				fi = open('{}/{}_temp.csv'.format(folder, file_))
				c.copy_expert("""COPY {} FROM STDIN WITH CSV DELIMITER '{}' NULL '' QUOTE '"' ESCAPE '\\';""".format(file_, config.delimiter), fi)
				print "TRUE"
				# Commit changes
				conn.commit()
			except (psycopg2.DataError, psycopg2.ProgrammingError) as e:
				print e
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
		files_new = list()
		for file_ in range(0, len(files)):
		    # If programming possible responses etc., then change behaviour. This is new. Corresponding html file has a shortened name
		    if files[file_] == "programming_assignment_submission_schema_part_xbkvdx.html":
		        continue
		    #print file_
		    for exs in [".html", ".csv"]:
		        if exs in files[file_]:
		            files_new.append(files[file_].replace(exs, ""))
		return(set(files_new))

	def near_empty_files(self, file_):
		self.file_ = file_
		try:
			num_lines = sum(1 for line in open("{}/{}.csv".format(self.folder, self.file_)))
			if num_lines <= 2:
				return True
			else:
				return False
		except IOError as e:
			print "Could not open {}/{}.csv. File does not exist.".format(self.folder, self.file_)

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
			print "Could not open {}/{}.csv. Check if path_to_data in config file is correct.".format(self.folder, self.file_)

# Call

if __name__ == "__main__":
	# Get file names
	files = helpers(config.path_to_files).unique_files()
	# Create database
	psql = postgresql(config.postgres_database_name, config.postgres_user, config.postgres_pwd, config.postgres_host)
	psql.create_database()
	# Get sql statements for each file
	for file_ in files:
		if file_ == "readme" or file_ == "guide.pdf" or helpers(config.path_to_files).near_empty_files(file_) == True:
			continue
		# Initiate scraper and scrape
		scr = scraper(config.path_to_files, file_).scrape()
		print scr
		# Create SQL tables
		psql.insert_headers(scr)
		# Insert CSV data
		psql.insert_data(config.path_to_files, file_)

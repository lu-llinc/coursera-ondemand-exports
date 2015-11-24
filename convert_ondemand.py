#!/usr/bin/env python2.7
# encoding: utf-8

'''
Convert the on-demand course exports to a postgresql table
'''

import bs4 as BeautifulSoup
import convert_ondemand_config as config
import psycopg2
import os
import pandas
from time import localtime, strftime

'''

This script takes on-demand data exports from coursera and dumps it in a postgresql database.

You need to create two folders:
	1) Folder with html files provided with the exports
	2) Folder with csv files provided with the exports

Specify the parameters of this script in the 'convert_ondemand_config.py' file.

Run this script using the command 'python convert_ondemand.py'

'''

# SCRAPER

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

# POSTGRES

class postgresql:

	def __init__(self, database, user, password, host):
		self.database = database
		self.host = host
		self.user = user
		self.password = password
		
	def create_database(self):
		# Connect
		try:
			conn = psycopg2.connect(user = self.user, host = self.host, password = self.password)
			# Set isolation level
			conn.set_isolation_level(0)
		except:
			print "ERROR: Could not connect to postgresql. Check settings in config file."
			log.logMessage("POSTGRES-CONNECTERROR", "Could not connect to postgresql. Check settings in config file.")

		with conn:
			c = conn.cursor()
			c.execute("""DROP DATABASE IF EXISTS {}""".format(self.database))
			c.execute("""CREATE DATABASE {}""".format(self.database))
			conn.commit()

		if conn:
			conn.close()

	def insert_headers(self, header):
		# Connect
		try:
			conn = psycopg2.connect(dbname = self.database, user = self.user, host = self.host, password = self.password)
			# Set isolation level
			conn.set_isolation_level(0)
		except:
			print "ERROR: Could not connect to database {}. Check settings in config file.".format(self.database)
			log.logMessage("POSTGRES-CONNECTERROR", "ERROR: Could not connect to database {}. Check settings in config file.".format(self.database))
			return None

		with conn:
			try:
				c = conn.cursor()
				c.execute(header)
				conn.commit()
			except psycopg2.ProgrammingError as e:
				print "ERROR: Could not send header for {} to database {}. Postgres returned error 'psycopg2.ProgrammingError'".format(file_, self.database)
				log.logMessage("POSTGRES-QUERYERROR", "Could not send header for {} to database {}. Postgres returned error 'psycopg2.ProgrammingError'".format(file_, self.database))
				return False

		if conn:
			conn.close()

		return None

	def insert_data(self, data_folder, file_):
		# Connect
		try:
			conn = psycopg2.connect(dbname = self.database, user = self.user, host = self.host, password = self.password)
			# Set isolation level
			conn.set_isolation_level(0)
		except:
			print "ERROR: Could not connect to database {}. Check settings in config file.".format(self.database)
			log.logMessage("POSTGRES-CONNECTERROR", "ERROR: Could not connect to database {}. Check settings in config file.".format(self.database))
			return None

		with conn:
			c = conn.cursor()
			# Open csv file and delete header
			try:
				#num_lines = sum(1 for line in open("{}/{}.csv".format(data_folder, file_)))
				#if num_lines <= 2:
					#return None
				# Remove header and save data in temporary file
				with open(r"{}/{}.csv".format(data_folder, file_), 'r') as f:
					with open(r"{}/{}_temp.csv".format(data_folder, file_), 'w') as f1:
						next(f)
						for line in f:
							f1.write(line)
			except:
				print "ERROR: could not open {}/{}.csv. Check if path_to_data in config file is correct.".format(data_folder, file_)
				log.logMessage("CSV-OPENERROR", "could not open {}/{}.csv. Check if path_to_data in config file is correct.".format(data_folder, file_))
				return(None)
				
			# Copy data to PostGres table
			try:
				c.execute("""COPY {} FROM '{}/{}_temp.csv' CSV DELIMITER ',' NULL '' QUOTE '"' ESCAPE '\\' HEADER;""".format(file_, data_folder, file_))
				print "TRUE"
				log.logMessage("SUCCESS", "Successfully inserted data from dataset {} into {}.".format(file_, self.database))
				conn.commit()
			except (psycopg2.DataError, psycopg2.ProgrammingError) as e:
				if str(type(e)) == '<class psycopg2.ProgrammingError>':
					print "ERROR: could not insert data for {} into {}. Table does not exist.".format(file_, self.database)
					log.logMessage("POSTGRES-MISSINGTABLE", "could not insert data for {} into {}. Table does not exist.".format(file_, self.database))
				else:
					print "ERROR: could not insert data for {} into {}. This is most likely due to 'extra data after last expected column' error.".format(file_, self.database)
					log.logMessage("CSV-EOFERROR", "could not insert data for {} into {}. This is most likely due to 'extra data after last expected column' error.".format(file_, self.database))
			# Delete file
			os.remove("{}/{}_temp.csv".format(data_folder, file_))
			#except psycopg2.DataError as e:
				#print "ERROR: Could not send data for {} to database {}. Postgres returned error 'psycopg2.DataError'".format(file_, self.database)
				#log.logMessage("POSTGRES-INSERTERROR", "Could not send data for {} to database {}. Postgres returned error 'psycopg2.DataError'".format(file_, self.database))


			#if f:
				#f.close()

		if conn:
			conn.close()

		return None

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
	files = [fileN.replace(".html", "") for fileN in os.listdir(config.path_to_variables)]
	# Create database
	psql = postgresql(config.postgres_database_name, config.postgres_user, config.postgres_pwd, config.postgres_host)
	psql.create_database()
	# Get sql statements for each file
	for file_ in files:
		if(file_ == "readme"):
			continue
		# Initiate scraper and scrape
		scr = scraper(config.path_to_variables + "/" + file_ + ".html").scrape()
		print scr
		# Create SQL tables
		psql.insert_headers(scr)
		# Insert CSV data
		psql.insert_data(config.path_to_data, file_)

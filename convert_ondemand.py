#!/usr/bin/env python2.7
# encoding: utf-8

'''
Convert the on-demand course exports to a sqlite table
'''

import bs4 as BeautifulSoup
import convert_ondemand_config as config
import MySQLdb as mdb
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

# SQL C

class insert_SQL:

	def __init__(self):
		None
		
	def create_database(self, SQLdatabase):
		conn = mdb.connect("localhost", "root", "root")

		with conn:
			c = conn.cursor()
			c.execute("DROP DATABASE IF EXISTS {};".format(SQLdatabase))
			c.execute("CREATE DATABASE {};".format(SQLdatabase))
			conn.commit()

		if conn:
			conn.close()

	def insert_headers(self, SQLdatabase, SQLstatement):
		conn = mdb.connect("localhost", "root", "root", SQLdatabase)

		with conn:
			c = conn.cursor()
			c = conn.cursor()
			c.execute(SQLstatement)
			conn.commit()

		if conn:
			conn.close()

		return None

	def checkQuery(self, SQLstatement):
		if "varchar" in SQLstatement:
			SQLstatement = SQLstatement.replace("varchar", "VARCHAR(1000)")
		if "VARCHAR(65535)" in SQLstatement:
			SQLstatement = SQLstatement.replace("VARCHAR(65535)", "TEXT")
		if "programming_submission_part_grid_grading_status_executor_run_start_ts" in SQLstatement:
			SQLstatement = SQLstatement.replace("programming_submission_part_grid_grading_status_executor_run_start_ts", "programming_submission_part_status_executor_run_start_ts")
		if "programming_submission_part_grid_grading_status_executor_run_status" in SQLstatement:
			SQLstatement = SQLstatement.replace("programming_submission_part_grid_grading_status_executor_run_status", "programming_submission_part_status_executor_run_status")

		if "programming_assignment_submission_schema_part_possible_response_order" in SQLstatement:
			SQLstatement = SQLstatement.replace("programming_assignment_submission_schema_part_possible_response_order", "programming_assignment_submission_schema_response_order")
		if "programming_assignment_submission_schema_part_possible_response_is_correct" in SQLstatement:
			SQLstatement = SQLstatement.replace("programming_assignment_submission_schema_part_possible_response_is_correct", "programming_assignment_submission_schema_response_correct")
		if "programming_assignment_submission_schema_part_possible_response_feedback" in SQLstatement:
			SQLstatement = SQLstatement.replace("programming_assignment_submission_schema_part_possible_response_feedback", "programming_assignment_submission_schema_response_feedback")
		if "programming_assignment_submission_schema_part_possible_response_answers" in SQLstatement:
			SQLstatement = SQLstatement.replace("programming_assignment_submission_schema_part_possible_response_answers", "programming_assignment_submission_schema_response_answers")

		if "programming_submission_part_grid_submission_custom_grader_parameters" in SQLstatement:
			SQLstatement = SQLstatement.replace("programming_submission_part_grid_submission_custom_grader_parameters", "programming_submission_part_grader_parameters")

		if "programming_assignment_submission_schema_part_grid_schema_expected_file_name" in SQLstatement:
			SQLstatement = SQLstatement.replace("programming_assignment_submission_schema_part_grid_schema_expected_file_name", "programming_assignment_submission_schema_file_name")
		if "programming_assignment_submission_schema_part_grid_schema_executor_id" in SQLstatement:
			SQLstatement = SQLstatement.replace("programming_assignment_submission_schema_part_grid_schema_executor_id", "programming_assignment_submission_schema_executor_id")
		if "programming_assignment_submission_schema_part_grid_schema_timeout" in SQLstatement:
			SQLstatement = SQLstatement.replace("programming_assignment_submission_schema_part_grid_schema_timeout", "programming_assignment_submission_timeout")
		if "programming_assignment_submission_schema_part_grid_custom_grader_parameters" in SQLstatement:
			SQLstatement = SQLstatement.replace("programming_assignment_submission_schema_part_grid_custom_grader_parameters", "programming_assignment_submission_schema_grader_parameters")

		if "programming_assignment_submission_schema_default_incorrect_feedback" in SQLstatement:
			SQLstatement = SQLstatement.replace("programming_assignment_submission_schema_default_incorrect_feedback", "programming_assignment_submission_incorrect_feedback")

		return SQLstatement

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
	# Get file names
	files = [fileN.replace(".html", "") for fileN in os.listdir(config.path_to_variables)]
	# Create database
	insert_SQL().create_database(config.SQL_database_name)
	# Get sql statements for each file
	for file_ in files:
		if(file_ == "readme"):
			continue
		# Initiate scraper and scrape
		scr = scraper(config.path_to_variables + "/" + file_ + ".html").scrape()
		scr = insert_SQL().checkQuery(scr)
		print scr
		# Create SQL tables
		insert_SQL().insert_headers(config.SQL_database_name, scr)


#!/usr/bin/python

# Created 27 October, 2016
# Analyst: Olivia Wilson
# Purpose: Extracts settlement data from Axiom Atlas Petroleum Price Curves PDF and inserts it into MySQL Database

import MySQLdb 												# To interact with MySQL
import time 												# For script tracking purposes
from datetime import date, timedelta 						# For data date insertion
import urllib2												# For dealing with the world wide web
import requests 											# Also for dealing with the internets :-)
import xlrd 												# For dealing with Excel files
import csv 													# For dealing with CSV files
import re 													# More help with the Excel beast
import os 													# Housekeeping... specifically, for renaming a file					


# Start of the ETL
start_time = time.time()
print ("Program started at %s ") % (start_time)

# Relevant Dates
yesterday = date.today() - timedelta(1)
yesterday_fmt = yesterday.strftime('%Y%m%d')

# File to be scraped is at the following URL:
xls_url = 'http://www.axiomcommoditygroup.com/crudedaily/Current.xlsx'
request = urllib2.Request(xls_url)

# Open the URL and download the excel file, rename the file so that it is appended with SettlementDate -- this ensures that we have all historical files archived for later reference
try:
	response = urllib2.urlopen(request)
except urllib2.HTTPError, e:
    checksLogger.error('HTTPError = ' + str(e.code))
except urllib2.URLError, e:
    checksLogger.error('URLError = ' + str(e.reason))
except httplib.HTTPException, e:
    checksLogger.error('HTTPException')
except Exception:
    import traceback
    checksLogger.error('generic exception: ' + traceback.format_exc())

# Open Excel File
with open('Current.xlsx', 'wb') as local_file:
	local_file.write(response.read())
print ("Successfully opened file from site")


current_rename = 'Current_%s.xlsx' % yesterday_fmt
os.rename('Current.xlsx', current_rename)

# Database Connection Details
db = MySQLdb.connect(	host="",			# Host
						user="",		    # Username
						passwd="",			# Password
						db="" 				# Name of Database
						 )

# Create cursor object to execute all queries
try:
   cur = db.cursor()
   print ('Successfully connected to database')
except:
   print ('Connection unsuccessful')

# Extract data from Excel file and insert into MySQL db, table: warehouse.settlement_axiom_web_scrape_daily
sql_str = "insert into warehouse.settlement_atlas_web_scrape_daily (SettlementDate, Market, P_Market, Instrument, SettlementValue, MonthType, Basis, SourceName) values (%s, %s, %s, %s, %s, %s, %s, 'ATLAS')"

# Open excel workbook
workbook = xlrd.open_workbook(current_rename)
# Open to the first sheet in the file
worksheet = workbook.sheet_by_index(0)

# Extract SettlementDate for all records in worksheet and convert it into ingestible form for MySQL
SettlementDate = worksheet.cell(2,1).value
SD_SQL = date(1899,12,30) + timedelta(days=SettlementDate)
print "Settlement Date: %s " % SD_SQL

# Establish Locations of each series parameters and put in series sequential order in each of the lists to prepare for the loop (There are 9 series) (P.S. there's a copy of the excel with arrows pointing to physical series location. ask Olivia Wilson how to get your hands on it)

s_range_max = [20, 16, 17, 15, 4, 4, 8, 12, 16]				# each record represents the maximum cell column value + 1 of the instrument series
s_row = [4, 10, 10, 16, 16, 22, 22, 22, 22]					# each record represents the excel row value where the series starts
s_col = [1, 14, 14, 1, 1, 1, 5, 9, 13]						# each record represents the excel column value where the series starts
s_market_min = [5, 11, 13, 17, 19, 23, 23, 23, 23]			# each record represents the minimum row value for the market specification of the series
s_market_max = [9, 13, 15, 19, 20,  26, 25, 24, 24]			# each record represents the maximum row value + 1 for the market specification of the series

# Loop over each of the 9 series
series_range = range(0,9)
for sn in series_range:
	# Series 0, 1, 2 : Use Basis WTI
	# Series 3, 4, 5, 6, 7, 8: Use Basis WTI-CMA
	if sn < 3:
		basis_name = 'WTI'
		monthtype = 'TRADE'
	else:
		basis_name = 'WTI-CMA'
		monthtype = 'CALENDAR'
	s_range_min = s_col[sn] + 1
	s_market_range = range(s_market_min[sn], s_market_max[sn])
	s_range = range(s_range_min, s_range_max[sn])
	s_insert_range = range(0, (s_range_max[sn] - s_range_min))
	# Retrieve the Instruments for the Series and ensure they are in a format compatible with MySQL 
	s_instruments = []
	for n in range(s_range_min, s_range_max[sn]):
		s_inst = worksheet.cell(s_row[sn], n).value
		s_inst_sql = date(1899,12,30) + timedelta(days=s_inst)				# Converts to form yyyy-mm-dd, compatible with MySQL
		print s_inst_sql
		s_instruments.append(s_inst_sql)
	print s_instruments
	# Retrieve Market Names
	for r in s_market_range:
		p_market = worksheet.cell(s_row[sn], s_col[sn]).value
		market_name = worksheet.cell(r, s_col[sn]).value
		# Retrieve Settlement Values for each prescribed market:
		settlement_values = []
		for c in s_range:
			# Extract the settlement values for each market, for each instrument, then append to list of all settlement values for the market
			settlement_val = worksheet.cell(r,c).value
			settlement_values.append(settlement_val)
			x = 0
		print settlement_values
		# Insert the extracted records into the MySQL database table: warehouse.settlement_axiom_web_scrape_daily
		for val in s_insert_range:
			market_insert = (SD_SQL, market_name, p_market, s_instruments[val], settlement_values[val], monthtype, basis_name)
			cur.execute(sql_str, market_insert)
			db.commit()
			x += 1
		print "Inserted %s records for Market %s %s  " % (x, market_name, p_market)

# Close the connection to the database
cur.close()
print ('Closed the connection')


# Complete this SoB
print('This ETL is now complete')
print('The program took %s seconds to execute') % (time.time()-start_time)
print ("This program took %s minutes to execute") % ((time.time()-start_time)/60)
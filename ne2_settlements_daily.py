#!/usr/bin/python

# Created 26 October, 2016
# Analyst: Olivia Wilson
# Purpose: Extracts settlement data from ne2 API and inserts it into MySQL Database

import MySQLdb 
import time
import csv
import requests
from datetime import date, timedelta

# Start of the ETL
start_time = time.time()
print ("Program started at %s ") % (start_time)

# ne2 authentication constants
username = ''
password = ''

# Generate List of dates to loop over and feed into API request
# d1 = date(2016,8,1)
# d2 = date(2016,10,25)
# List of all dates between d1 and d2
# dd = [d1 + timedelta(days=x) for x in range((d2-d1).days + 1)]

dd = date.today() - timedelta(1)

# New Database Connection Details
db = MySQLdb.connect(	host="",			# Host
						user="",		    # Username
						passwd="",			# Password
						db="" 				# Name of Database
						 )
try:
   cur = db.cursor()
   print ('Successfully connected to database')
except:
   print ('Connection unsuccessful')



z = 0
# Loop for all dates
# for d in dd:
input_date = dd.strftime('%Y%m%d')													# format date strings according to API specifications
print input_date
url = 'http://ne2.ca/nedd/exp/settlement-export?date=%s' % input_date				# feed date parameters into the API url string
response = requests.get(url, auth=(username, password))								# send the GET request to the API
# Save file as CSV
with open('settlement_export.csv', 'wb') as output:
	output.write(response.content)
# Open and read the CSV imported file
csv_data = csv.reader(file('settlement_export.csv'))
# skip first line
firstline = True
# insert remaining lines into table: warehouse.settlement_web_scrape_daily
for row in csv_data:
	if firstline:
		firstline = False
		continue
	cur.execute("insert into warehouse.settlement_ne2_web_scrape_daily (SettlementDate, Market, Instrument, SettlementValue, InstrumentStartDate, InstrumentEndDate) values (%s, %s, %s, %s, %s, %s)", row)
	db.commit()
print ('Loaded the data for %s into the table') % input_date
z = z + 1


print "finished loading all new records for %s dates " % z


# Close the connection to the database
cur.close()
print ('Closed the connection')
print('This ETL is now complete')
print('The program took %s seconds to execute') % (time.time()-start_time)
print ("This program took %s minutes to execute") % ((time.time()-start_time)/60)

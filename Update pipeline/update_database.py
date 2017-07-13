'''
This code updates the smaller.db database.
To run this file, open a command prompt in this folder and run the command
python update_database.py
If you have any difficulties or questions, contact Max Vilgalys (vilgalys@mit.edu)
'''

print("Determining maximum date in database...")
# First find how current the database is - this finds the most recent month of data in 
# smaller.db, so it won't pull any more recent data.
import sqlite3
import pandas
con = sqlite3.connect('smaller.db')
max_date = pandas.read_sql('select max(op_date) from data', con)
for i, row in max_date.iterrows():
    date = str(row[0])
max_month = date[0:2]
max_year = date[-4:]
print("Max date found: " + max_month + " " + max_year)

print()
print("Downloading additional zip files...")

from ftplib import FTP
import os.path

import zipfile
from pathlib2 import Path

ftp = FTP('ftp.epa.gov', timeout=60)
ftp.login()
ftp.cwd('dmdnload')
ftp.cwd('emissions')
ftp.cwd('hourly')
ftp.cwd('monthly')
years = []
ftp.retrlines('NLST', years.append)
parent_directory = ftp.pwd()

def zip_fetch(ftp, entry):
    print("Now fetching: " + entry)
    outfile = open('data/' + entry, 'wb')
    ftp.retrbinary('RETR ' + entry, outfile.write)
    outfile.close()
    return
states = ['wa', 'or', 'ca', 'id', 'nv', 'ut', 'az', 'nm', 'co', 'wy', 'mt', 'tx']
def inWest(string):
    return string[4:6] in states

if not os.path.exists('data'):
    os.makedirs('data')


def unzip(name):
    with zipfile.ZipFile(name, "r") as z:
        z.extractall('unzipped')

if not os.path.exists('unzipped'):
    os.makedirs('unzipped')

for year in years:
    if (int(year) < int(max_year)): continue
    ftp.cwd(str(year))
    files = []
    ftp.retrlines('NLST', files.append)
    for entry in files:
        if (int(year) == int(max_year) and int(entry[6:8]) <= int(max_month)) or not inWest(entry):
            continue
        try:
            zip_fetch(ftp, entry)

        except:
            pass
    for n_month in range(1, 13):		
        for state in ['wa', 'or', 'ca', 'id', 'nv', 'ut', 'az', 'nm', 'co', 'wy', 'mt', 'tx']:
            month = str(n_month) if n_month > 9 else "0" + str(n_month)
            name = "data/" + str(year) + state + month + ".zip"
            if os.path.isfile(name):
            	print("Now unzipping: " + name )
                unzip(name)           
    ftp.cwd('..')


# Begin reading in data, combining it
print("Reading data into database:")
import csv
con = sqlite3.connect("to_insert.db")
cur = con.cursor()

# Set up table to receive CEMS data
cur.execute('drop table if exists data;')
cur.execute(
"create table data( \
    state varchar, \
    facility_name varchar, \
    orispl_code int, \
    unitid varchar, \
    op_date date, \
    op_hour int, \
    op_time float, \
    gload float, \
    SO2_MASS float, \
    NOX_MASS float, \
    CO2_MASS float, \
    HEAT_INPUT float \
);")

# Iterate through files to read data into database
for year in years:
    print(year)
    for n_month in range(1, 13):
        for state in ['wa', 'or', 'ca', 'id', 'nv', 'ut', 'az', 'nm', 'co', 'wy', 'mt', 'tx']:
            month = str(n_month) if n_month > 9 else "0" + str(n_month)
            name = "unzipped/" + str(year) + state + month + ".csv"
            if os.path.isfile(name):
                with open(name, 'rb') as fin:
                    dr = csv.DictReader(fin)
                    try:
                        to_db = [(i['STATE'], i['FACILITY_NAME'], i['ORISPL_CODE'], i['UNITID'], i['OP_DATE'], i['OP_HOUR'], i['OP_TIME'], i['GLOAD (MW)'], 
                                  i['SO2_MASS (lbs)'], i['NOX_MASS (lbs)'], i['CO2_MASS (tons)'], i['HEAT_INPUT (mmBtu)']) for i in dr]
                    except:
                        try:
                            to_db = [(i['STATE'], i['FACILITY_NAME'], i['ORISPL_CODE'], i['UNITID'], i['OP_DATE'], i['OP_HOUR'], i['OP_TIME'], i['GLOAD'], 
                                  i['SO2_MASS'], i['NOX_MASS'], i['CO2_MASS'], i['HEAT_INPUT']) for i in dr]
                        except:
                            print(name)
#                     print to_db
                    cur.executemany("insert into data (STATE, FACILITY_NAME, ORISPL_CODE, UNITID, OP_DATE, \
OP_HOUR, OP_TIME, GLOAD, SO2_MASS, NOX_MASS, CO2_MASS, HEAT_INPUT ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", to_db)

con.commit()

cur.execute('drop table if exists labels;')
cur.execute(
"create table labels( \
    state varchar, \
    facility_name varchar, \
    orispl_code int, \
    unitid varchar, \
    year int, \
    county varhcar, \
    latitude float, \
    longitude float, \
    fuel_type varchar, \
    fule_type_secondary varchar \
);")

with open('EPADownload/labels.csv', 'rb') as fin:
    dr = csv.DictReader(fin)
    to_db = [(i['State'],  i[' Facility Name'], i[' Facility ID (ORISPL)'], i[' Unit ID'], i[' Year'], i[' County'],  
              i[' Facility Latitude'], i[' Facility Longitude'], i[' Fuel Type (Primary)'], i[' Fuel Type (Secondary)']) for i in dr]
cur.executemany("insert into labels (    state, facility_name, orispl_code, unitid, year, \
    county, latitude, longitude, fuel_type, fule_type_secondary \
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", to_db)
con.commit()

print("Combining databases: ")

con = sqlite3.connect("smaller.db")
cur = con.cursor()
cur.execute("ATTACH 'to_insert.db' as cems;")
cur.execute("INSERT into data select test.state as state, test.facility_name as name,\
 test.orispl_code as orispl_code, test.unitid as unitid, op_date, year, op_hour, op_time, gload, SO2_MASS, NOX_MASS, CO2_MASS,\
 HEAT_INPUT as heat_input, county, latitude, longitude, fuel_type, fule_type_secondary as fuel_type_2, \
 CASE WHEN (fuel_type is 'Natural Gas' or fuel_type is 'Pipeline Natural Gas' or fuel_type is 'Natural Gas, Pipeline Natural Gas')    THEN ('NG') ELSE (fuel_type) END as fuel_actual \
 from cems.data as test, cems.labels as labels \
 where substr(test.op_date, -4) is cast(labels.year as varchar) \
 AND labels.orispl_code is test.orispl_code AND test.unitid is labels.unitid;")
cur.execute("delete from data where state is 'TX' and (county is not 'El Paso' or name is 'Montana Power Station');")
cur.execute("vacuum")


con.commit()

print("Mostly done! Just cleaning up a bit")

import shutil
import os
shutil.rmtree('data')
shutil.rmtree('unzipped')
os.remove('to_insert.db')


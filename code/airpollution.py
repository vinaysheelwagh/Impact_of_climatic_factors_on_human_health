#Retriving the data using API 

import json
import pandas as pd
from sodapy import Socrata
import unicodecsv as csv
import pymongo
from pymongo import MongoClient
import requests
from bson import json_util
import sys, urllib.request
import pandas.io.sql as sqlio
import pymongo
import psycopg2 as pg
import csv
import psycopg2 as pg

client = Socrata("data.cdc.gov", 'iRDwRSoM9dx59SsPZmzXZ0q4y', username="visaka_10gmail.com", password="qwerty@34")

results5 = client.get("fcqm-xrf4", statefips = '36', year = '2014', limit=200000)

for data in results5:
    for item in data:
        if 'date' in item:
            if "JAN2014" not in data[item]:
                results5.remove(data)

#inserting into mongodb under the database test and under the collection dataset4V.

client = MongoClient("mongodb://grp_h:grp_h123@87.44.4.13/test")
mydb = client["test"]
if23 = mydb["dataset4V"]
x = if23.insert_many(results5)


keys = arra[0].keys()
with open('airpollution.csv', 'wb') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(str_data)

import pandas as pd
air_pollution_df = pd.read_csv("airpollution.csv")
#renaming the column
air_pollution_df.rename(columns={'ds_pm_pred':'AirPollutionLevel'},inplace=True)
#type casting the data to numeric
air_pollution_df['Year'] = pd.to_numeric(air_pollution_df['Year'])
air_pollution_df['StateFips'] = pd.to_numeric(air_pollution_df['StateFips'])
air_pollution_df['Countyfips'] = pd.to_numeric(air_pollution_df['Countyfips'])
air_pollution_df['AirPollutionLevel']= pd.to_numeric(air_pollution_df['AirPollutionLevel'],downcast='float')

air_pollution_df.to_csv('air_pollution.csv', sep=',')


#creating the table in postgresql

import psycopg2 as pg
try:
    connection = pg.connect(
    user = "raghav_dap",password = "dap@123",
    host = "87.44.4.13",
    port = "5432",
    database = "dap_h")
    connection.set_isolation_level(0) # AUTOCOMMIT
    dbCursor = connection.cursor()
    dbCursor.execute("""
CREATE TABLE air_pollution(
ID                  text    PRIMARY KEY,
StateFips                integer,
countyfips                 text,
year                 integer,
date				 timestamp,		
ctfips            	integer,	
latitude			integer,
longitude			integer,
AirPollutionLevel					integer,
ds_pm_stdd					integer);
""")
    dbCursor.close()
except (Exception , pg.Error) as dbError :
    print ("Error while connecting to PostgreSQL", dbError)
finally:
    if(connection): connection.close()



import csv
file = input(air_pollution.csv)
sql_insert = """INSERT INTO diseases(ID,StateFips,countyfips,year,date,ctfips,latitude,longitude,AirPollutionLevel,ds_pm_stdd)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
try:
    connection = pg.connect(
    user = "raghav_dap",password = "dap@123",
    host = "87.44.4.13",
    port = "5432",
    database = "dap_h")
    cursor = connection.cursor()
    with open(file, 'r') as f:
        reader = csv.reader(f)
        next(reader) # This skips the 1st row which is the header.
        for record in reader:
            cursor.execute(sql_insert, record)
            connection.commit()
except (Exception, pg.Error) as e:
    print(e)
finally:
        if (connection):
            cursor.close()
            connection.close()
            print("Connection closed.")


sql_1 = """select ID,StateFips,countyfips,year,date,ctfips,latitude,longitude,AirPollutionLevel,ds_pm_stdd;""" 
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2 as pg

 try:
    connection = pg.connect(
    user = "raghav_dap",password = "dap@123",
    host = "87.44.4.13",
    port = "5432",
    database = "dap_h")
    air_pollution_final = sqlio.read_sql_query(sql_1, connection)
except (Exception , pg.Error) as dbError :
    print ("Error:", dbError)
finally:
    if(connection): connection.close()

#data transformation
air_pollution_final.drop(["ID", "year","date", "latitude","longitude","ds_pm_stdd","ctfips"], axis = 1,inplace = True)

air_pollution_final.to_csv('Updated_airpollution.csv', sep=',')

try:
    connection = pg.connect(
    user = "raghav_dap",password = "dap@123",
    host = "87.44.4.13",
    port = "5432",
    database = "dap_h")
    connection.set_isolation_level(0) # AUTOCOMMIT
    dbCursor = connection.cursor()
    dbCursor.execute("""
CREATE TABLE Updated_airpollution(
StateFips                integer,
countyfips                 text PRIMARY KEY,			
AirPollutionLevel            integer);
""")
    dbCursor.close()
except (Exception , pg.Error) as dbError :
    print ("Error while connecting to PostgreSQL", dbError)
finally:
    if(connection): connection.close()

file = input(Updated_airpollution.csv)
sql_insert = """INSERT INTO Updated_airpollution(StateFips,countyfips,AirPollutionLevel)
                VALUES(%s, %s, %s)"""
try:
    connection = pg.connect(
    user = "raghav_dap",password = "dap@123",
    host = "87.44.4.13",
    port = "5432",
    database = "dap_h")
    cursor = connection.cursor()
    with open(file, 'r') as f:
        reader = csv.reader(f)
        next(reader) # This skips the 1st row which is the header.
        for record in reader:
            cursor.execute(sql_insert, record)
            connection.commit()
except (Exception, pg.Error) as e:
    print(e)
finally:
        if (connection):
            cursor.close()
            connection.close()
            print("Connection closed.")







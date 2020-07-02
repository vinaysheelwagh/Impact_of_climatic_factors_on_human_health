####Connecting to socrata api and extracting the data in json format

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


client = Socrata("data.cdc.gov", 'iRDwRSoM9dx59SsPZmzXZ0q4y', username="Kamalesh.palani@hotmail.com", password="POL@2019")

results3 = client.get("372p-dx3h", statefips = '36', year = '2014', limit=100000)

for data in results3:
    for item in data:
        if 'date' in item:
            if "JAN2014" not in data[item]:
                results.remove(data)

#### Inserting the raw data into mango DB 

client = MongoClient("mongodb://grp_h:grp_h123@87.44.4.13/test")
mydb = client["test"]
ifsc = mydb["dataset3K"]
x = ifsc.insert_many(results3)

arra = list(data.find())
keys = arra[0].keys()
with open('Ozone.csv', 'wb') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(arra)

import pandas as pd
Ozone_df = pd.read_csv("Ozone.csv")

Ozone_df['FIPS'] = Ozone_df['countyfips']

Ozone_df.set_index('FIPS')

#### Web scrapping to extract the USA County FIPS codes and names


import urllib.request
from bs4 import BeautifulSoup
url = "https://en.wikipedia.org/wiki/List_of_United_States_FIPS_codes_by_county"
page = urllib.request.urlopen(url)
soup = BeautifulSoup(page, "lxml")
#print(soup.prettify())

all_tables=soup.find_all("table")
#all_tables
right_table=soup.find('table', class_='wikitable sortable')
#right_table

## processing the scrapped data and storing the info in the array
A=[]
B=[]
for row in right_table.findAll('tr'):
    cells=row.findAll('td')
    if len(cells)==2:
        A.append(cells[0].find(text=True))
        B.append(cells[1].find(text=True))

####  Writing the scraped data to the dataframe

import pandas as pd
df1 = pd.DataFrame()
df1['FIPS'] = A
df1['County'] = B
df1 = df1.replace('\n','', regex=True)
df1['Numeric_code'] = df1['FIPS'].str[:2]
df1['County'] = df1['County'].replace('County','', regex=True)

#### merging the dataset for state and county

counties = pd.merge(ozone_df, df1, on='FIPS', how='inner')

#### Converting the columns to numeric 

counties['ds_o3_pred'] = pd.to_numeric(counties['ds_o3_pred'])
counties.hist(column="ds_o3_pred")
 
counties['ds_o3_stdd'] = pd.to_numeric(counties['ds_o3_stdd'])

counties.to_csv('ozone.csv', sep=',')

#### Connecting to the POSTGRESQL and creating the table 

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
CREATE TABLE ozone_level(
ID                  text    PRIMARY KEY,
statefips                integer,
countyfips                 text,
year                 	integer,
date					 timestamp,			
ctfips		            integer,
latitude				float,
longitude					float,
ds_o3_pred					float,
ds_o3_stdd					float,
county						text);
""")
    dbCursor.close()
except (Exception , pg.Error) as dbError :
    print ("Error while connecting to PostgreSQL", dbError)
finally:
    if(connection): connection.close()

#### Loading the Data to the POSTGRESQL 

import csv
file = input(ozone.csv)
sql_insert = """INSERT INTO ozone_level(ID,statefips,countyfips,year,date,ctfips,latitude,longitude,ds_o3_pred,ds_o3_stdd,county)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
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
			
			
#### Retriving the data from POSTGREDQL to perform  Transformation
			
sql_1 = """ID,statefips,countyfips,year,date,ctfips,latitude,longitude,ds_o3_pred,ds_o3_stdd,county;""" 
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2 as pg

 

try:
    connection = pg.connect(
    user = "raghav_dap",password = "dap@123",
    host = "87.44.4.13",
    port = "5432",
    database = "dap_h")
    ozone_var = sqlio.read_sql_query(sql_1, connection)
except (Exception , pg.Error) as dbError :
    print ("Error:", dbError)
finally:
    if(connection): connection.close()
	
#### Data Transformation

ozone_var.drop(["ID","latitude", "longitude","ctfips", "ds_o3_stdd", "year","date"], axis = 1,inplace = True)

ozone_var['ozone_con'] = ozone_var['ds_o3_pred']

ozone_var.to_csv('Updated_ozone.csv', sep=',')

#### Creating the new table to load the transformed data
try:
    connection = pg.connect(
    user = "raghav_dap",password = "dap@123",
    host = "87.44.4.13",
    port = "5432",
    database = "dap_h")
    connection.set_isolation_level(0) # AUTOCOMMIT
    dbCursor = connection.cursor()
    dbCursor.execute("""
CREATE TABLE Updated_ozone(
statefips                integer,
countyfips                 text PRIMARY KEY,			
ozone_con            float,
county                 text);
""")
    dbCursor.close()
except (Exception , pg.Error) as dbError :
    print ("Error while connecting to PostgreSQL", dbError)
finally:
    if(connection): connection.close()

#### Loading the Final data to POSTGRESQL  

file = input(Updated_ozone.csv)
sql_insert = """INSERT INTO Updated_ozone(statefips,countyfips,ozone_con,county)
                VALUES(%s, %s, %s, %s)"""
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





#Connecting to the CDC.gov API and retriving JSON data

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

client = Socrata("data.cdc.gov", 'iRDwRSoM9dx59SsPZmzXZ0q4y', username="raghav_krishna@gmail.com", password="WOW@2018")

results = client.get("h28b-t43q", statefips = '36', year = '2014',month='1', limit=10000)

with open('raghavData.json','w') as fp:
    json.dump(results,fp)


#inserting into mongodb under the database test and under the collection dataset1.

client = MongoClient("mongodb://grp_h:grp_h123@87.44.4.13/test")
mydb = client["test"]
svs = mydb["dataset1"]
x = svs.insert_many(results)


arra = list(data.find())

keys = arra[0].keys()
with open('UV_Irradiance.csv', 'wb') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(arra)

import pandas as pd
formatted = pd.read_csv("UV_Irradiance.csv")
    
#web scrapping the state names from wikipedia

from datetime import datetime
start_time = datetime.now()


# import the library we use to open URLs
import urllib.request
from bs4 import BeautifulSoup
url = "https://en.wikipedia.org/wiki/Federal_Information_Processing_Standard_state_code"
page = urllib.request.urlopen(url)
soup = BeautifulSoup(page, "lxml")
#print(soup.prettify())


all_tables=soup.find_all("table")
#all_tables
right_table=soup.find('table', class_='wikitable sortable')
right_table


## processing the scrapped data and storing the info in the array
A=[]
B=[]
C=[]
for row in right_table.findAll('tr'):
    cells=row.findAll('td')
    if len(cells)==4:
        A.append(cells[0].find(text=True))
        B.append(cells[1].find(text=True))
        C.append(cells[2].find(text=True))


## Writing the scraped data to the dataframe
import pandas as pd
df = pd.DataFrame()
df['State'] = A
df['State_fips'] = B
df['Numeric_code'] = C


end_time = datetime.now()
print('Duration: {}'.format(end_time - start_time))

df.drop(["Numeric_code"], axis = 1,inplace = True)


#merging the dataset for state and county

states = pd.merge(formatted, df, on='State_fips', how='inner')

states.rename(columns={'statefips':'State_fips','edd':'Irradationvalue'},inplace=True)

states.to_csv('states_full.csv', sep=',')

# Creating the table in postgresql 

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
CREATE TABLE UV_Irradiance(
ID                  text    PRIMARY KEY,
StateFips                integer,
countyfips                 text,
year                 integer,
month				 integer,
day					 integer,			
Irradationvalue            integer,
edr						integer,
i305					integer,
i310					integer,
i324					integer,
i380					integer,
State                 text);
""")
    dbCursor.close()
except (Exception , pg.Error) as dbError :
    print ("Error while connecting to PostgreSQL", dbError)
finally:
    if(connection): connection.close()


import csv
file = input(states_full.csv)
sql_insert = """INSERT INTO diseases(ID,StateFips,countyfips,year,month,day,edr,Irradationvalue,i305,i310,i324,i380,State)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
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
			
#Retriving the data from the postgresql
			
sql_1 = """select ID,StateFips,countyfips,year,month,day,edr,Irradationvalue,i305,i310,i324,i380,State;""" 
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2 as pg

 

try:
    connection = pg.connect(
    user = "raghav_dap",password = "dap@123",
    host = "87.44.4.13",
    port = "5432",
    database = "dap_h")
    irradation = sqlio.read_sql_query(sql_1, connection)
except (Exception , pg.Error) as dbError :
    print ("Error:", dbError)
finally:
    if(connection): connection.close()
	
#Data Transformation

irradation.drop(["ID", "year","month", "day", "edr","i305","i310","i324","i380"], axis = 1,inplace = True)

irradation.to_csv('Updated_uvirradation.csv', sep=',')

try:
    connection = pg.connect(
    user = "raghav_dap",password = "dap@123",
    host = "87.44.4.13",
    port = "5432",
    database = "dap_h")
    connection.set_isolation_level(0) # AUTOCOMMIT
    dbCursor = connection.cursor()
    dbCursor.execute("""
CREATE TABLE Updated_uvirradation(
StateFips                integer,
countyfips                 text PRIMARY KEY,			
Irradationvalue            integer,
State                 text);
""")
    dbCursor.close()
except (Exception , pg.Error) as dbError :
    print ("Error while connecting to PostgreSQL", dbError)
finally:
    if(connection): connection.close()
	
# Loading the Final data to the newly created updated table

file = input(Updated_uvirradation.csv)
sql_insert = """INSERT INTO Updated_uvirradation(StateFips,countyfips,Irradationvalue,State)
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
















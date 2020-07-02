############# Using API to retrive the data from the cdc.gov website 

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


url ="https://chronicdata.cdc.gov/resource/k86t-wghb.json?"

client = Socrata("chronicdata.cdc.gov", 'iRDwRSoM9dx59SsPZmzXZ0q4y', username="vinaysheel_wagh@hotmail.com", password="vinIND@19")

results1 = client.get("k86t-wghb", limit=28000)

removeKey = ('placefips','tractfips','place_tractid','access2_crudeprev','access2_crude95ci','arthritis_crude95ci','binge_crudeprev','binge_crude95ci','bphigh_crude95ci','bpmed_crudeprev','bpmed_crude95ci','cancer_crude95ci','casthma_crude95ci','chd_crude95ci','checkup_crudeprev','checkup_crude95ci','cholscreen_crude95ci','colon_screen_crudeprev','colon_screen_crude95ci','copd_crudeprev','copd_crude95ci','corem_crude95ci','corew_crudeprev','corew_crude95ci','csmoking_crude95ci','dental_crude95ci','diabetes_crude95ci','highchol_crude95ci','kidney_crude95ci','lpa_crude95ci','mammouse_crude95ci','mhlth_crude95ci','obesity_crude95ci','paptest_crude95ci','phlth_crude95ci','sleep_crude95ci','stroke_crude95ci','teethlost_crude95ci')

for item in results:
    for key in removeKey:
        if key in item:
            del item[key]
    item['year'] = 2014

	
############ Inserting into mongodb under the database test and under the collection dataset2V.	

client = MongoClient("mongodb://grp_h:grp_h123@87.44.4.13/test")
mydb = client["test"]
ifs = mydb["dataset2V"]
x = ifs.insert_many(results)

str_data = list(data.find())

keys = arra[0].keys()
with open('Diseases.csv', 'wb') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(str_data)

#import pandas as pd
df = pd.read_csv("Diseases.csv")

####### Renaming the columns 

df.rename(columns={'stateabbr':'State','placename':'County','population2010':'Population','arthritis_crudeprev':'Arthritis',
                          'bphigh_crudeprev':'BloodPressure','cancer_crudeprev':'Cancer','casthma_crudeprev':'Asthma','chd_crudeprev':
                           'HeartDiseases','cholscreen_crudeprev':'Cholesterol','corem_crudeprev':'ClinicalPriventiveServices',
                           'csmoking_crudeprev':'SmokingRateAdult', 'dental_crudeprev':'DentistVisit', 'diabetes_crudeprev':'Diabetes',
                           'highchol_crudeprev':'HighCholesterol ', 'kidney_crudeprev':'KidneyDiseases', 'lpa_crudeprev':'NoTimeForPhysicalActivities',
                           'mammouse_crudeprev':'Mammography', 'mhlth_crudeprev':'MentalHealth>14Days', 'obesity_crudeprev':'Obesity',
                           'paptest_crudeprev':'PapTest', 'phlth_crudeprev':'PhysicalHealth>14Days', 'sleep_crudeprev':'SleepDeficiency',
                           'stroke_crudeprev':'Stroke', 'teethlost_crudeprev':'TeethLost'},inplace=True)
results_df.head(5)

disease_df= df[["State","CityName","Population","BloodPressure","Cancer","Asthma","HeartDiseases","Diabetes","PhysicalHealth>14Days","MentalHealth>14Days","ClinicalPriventiveServices"]]

####### Typecasting the columns to numeric

disease_df['Population'] = pd.to_numeric(disease_df['Population'])
disease_df['BloodPressure'] = pd.to_numeric(disease_df['BloodPressure'],downcast='float')
disease_df['Cancer'] = pd.to_numeric(disease_df['Cancer'],downcast='float')
disease_df['Asthma'] = pd.to_numeric(disease_df['Asthma'],downcast='float')
disease_df['HeartDiseases'] = pd.to_numeric(disease_df['HeartDiseases'],downcast='float')
disease_df['ClinicalPriventiveServices'] = pd.to_numeric(disease_df['ClinicalPriventiveServices'],downcast='float')
disease_df['Diabetes'] = pd.to_numeric(disease_df['Diabetes'],downcast='float')
disease_df['PhysicalHealth>14Days'] = pd.to_numeric(disease_df['PhysicalHealth>14Days'],downcast='float')
disease_df['MentalHealth>14Days'] = pd.to_numeric(disease_df['MentalHealth>14Days'],downcast='float')

disease_df.to_csv('disease_df.csv', sep=',')

###### Creating the table in postgresql

#import psycopg2 as pg
try:
    connection = pg.connect(
    user = "raghav_dap",password = "dap@123",
    host = "87.44.4.13",
    port = "5432",
    database = "dap_h")
    connection.set_isolation_level(0) # AUTOCOMMIT
    dbCursor = connection.cursor()
    dbCursor.execute("""
CREATE TABLE diseases(
ID                  text    PRIMARY KEY,
State                integer,
County                 text,
Population                 integer,
BloodPressure            integer,
Cancer                 integer,
Asthma              integer(20),
HeartDiseases             integer(20),
Diabetes            integer(20),
PhysicalHealth>14Days    integer(20),
MentalHealth>14Days           integer(20),
ClinicalPriventiveServices         integer(20));
""")
    dbCursor.close()
except (Exception , pg.Error) as dbError :
    print ("Error while connecting to PostgreSQL", dbError)
finally:
    if(connection): connection.close()


#import csv
file = input(disease_df.csv)
sql_insert = """INSERT INTO diseases(State,County,Population,BloodPressure,Cancer,Asthma,HeartDiseases,Diabetes,PhysicalHealth>14Days,MentalHealth>14Days,ClinicalPriventiveServices)
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

#### Converting the place names to county names

cityNameStr =["Adelanto", "Agoura Hills", "Alameda", "Albany", "Alhambra", "Aliso Viejo", "Alturas", "Amador City", "American Canyon", "Anaheim", "Anderson", "Angels Camp", "Antioch", "Apple Valley", "Arcadia", "Arcata", "Arroyo Grande", "Artesia", "Arvin", "Atascadero", "Atherton", "Atwater", "Auburn", "Avalon", "Avenal", "Azusa", "Bakersfield", "Baldwin Park", "Banning", "Barstow", "Beaumont", "Bell", "Bell Gardens", "Bellflower", "Belmont", "Belvedere", "Benicia", "Berkeley", "Beverly Hills", "Big Bear Lake", "Biggs", "Bishop", "Blue Lake", "Blythe", "Bradbury", "Brawley", "Brea", "Brentwood", "Brisbane", "Buellton", "Buena Park", "Burbank", "Burlingame", "Calabasas", "Calexico", "California City", "Calimesa", "Calipatria", "Calistoga", "Camarillo", "Campbell", "Canyon Lake", "Capitola", "Carlsbad", "Carmel-by-the-Sea", "Carpinteria", "Carson", "Cathedral City", "Ceres", "Cerritos", "Chico", "Chino", "Chino Hills", "Chowchilla", "Chula Vista", "Citrus Heights", "Claremont", "Clayton", "Clearlake", "Cloverdale", "Clovis", "Coachella", "Coalinga", "Colfax", "Colma", "Colton", "Colusa", "Commerce", "Compton", "Concord", "Corcoran", "Corning", "Corona", "Coronado", "Corte Madera", "Costa Mesa", "Cotati", "Covina", "Crescent City", "Cudahy", "Culver City", "Cupertino", "Cypress", "Daly City", "Dana Point", "Danville", "Davis", "Del Mar", "Del Rey Oaks", "Delano", "Desert Hot Springs", "Diamond Bar", "Dinuba", "Dixon", "Dorris", "Dos Palos", "Downey", "Duarte", "Dublin", "Dunsmuir", "East Palo Alto", "Eastvale", "El Cajon", "El Centro", "El Cerrito", "El Monte", "El Segundo", "Elk Grove", "Emeryville", "Encinitas", "Escalon", "Escondido", "Etna", "Eureka", "Exeter", "Fairfax", "Fairfield", "Farmersville", "Ferndale", "Fillmore", "Firebaugh", "Folsom", "Fontana", "Fort Bragg", "Fort Jones", "Fortuna", "Foster City", "Fountain Valley", "Fowler", "Fremont", "Fresno", "Fullerton", "Galt", "Garden Grove", "Gardena", "Gilroy", "Glendale", "Glendora", "Goleta", "Gonzales", "Grand Terrace", "Grass Valley", "Greenfield", "Gridley", "Grover Beach", "Guadalupe", "Gustine", "Half Moon Bay", "Hanford", "Hawaiian Gardens", "Hawthorne", "Hayward", "Healdsburg", "Hemet", "Hercules", "Hermosa Beach", "Hesperia", "Hidden Hills", "Highland", "Hillsborough", "Hollister", "Holtville", "Hughson", "Huntington Beach", "Huntington Park", "Huron", "Imperial", "Imperial Beach", "Indian Wells", "Indio", "Industry", "Inglewood", "Ione", "Irvine", "Irwindale", "Isleton", "Jackson", "Jurupa Valley", "Kerman", "King City", "Kingsburg", "La Cañada Flintridge", "La Habra", "La Habra Heights", "La Mesa", "La Mirada", "La Palma", "La Puente", "La Quinta", "La Verne", "Lafayette", "Laguna Beach", "Laguna Hills", "Laguna Niguel", "Laguna Woods", "Lake Elsinore", "Lake Forest", "Lakeport", "Lakewood", "Lancaster", "Larkspur", "Lathrop", "Lawndale", "Lemon Grove", "Lemoore", "Lincoln", "Lindsay", "Live Oak", "Livermore", "Livingston", "Lodi", "Loma Linda", "Lomita", "Lompoc", "Long Beach", "Loomis", "Los Alamitos", "Los Altos", "Los Altos Hills", "Los Angeles", "Los Banos", "Los Gatos", "Loyalton", "Lynwood", "Madera", "Malibu", "Mammoth Lakes", "Manhattan Beach", "Manteca", "Maricopa", "Marina", "Martinez", "Marysville", "Maywood", "McFarland", "Mendota", "Menifee", "Menlo Park", "Merced", "Mill Valley", "Millbrae", "Milpitas", "Mission Viejo", "Modesto", "Monrovia", "Montague", "Montclair", "Monte Sereno", "Montebello", "Monterey", "Monterey Park", "Moorpark", "Moraga", "Moreno Valley", "Morgan Hill", "Morro Bay", "Mount Shasta", "Mountain View", "Murrieta", "Napa", "National City", "Needles", "Nevada City", "Newark", "Newman", "Newport Beach", "Norco", "Norwalk", "Novato", "Oakdale", "Oakland", "Oakley", "Oceanside", "Ojai", "Ontario", "Orange", "Orange Cove", "Orinda", "Orland", "Oroville", "Oxnard", "Pacific Grove", "Pacifica", "Palm Desert", "Palm Springs", "Palmdale", "Palo Alto", "Palos Verdes Estates", "Paradise", "Paramount", "Parlier", "Pasadena", "Paso Robles", "Patterson", "Perris", "Petaluma", "Pico Rivera", "Piedmont", "Pinole", "Pismo Beach", "Pittsburg", "Placentia", "Placerville", "Pleasant Hill", "Pleasanton", "Plymouth", "Point Arena", "Pomona", "Port Hueneme", "Porterville", "Portola", "Portola Valley", "Poway", "Rancho Cordova", "Rancho Cucamonga", "Rancho Mirage", "Rancho Palos Verdes", "Rancho Santa Margarita", "Red Bluff", "Redding", "Redlands", "Redondo Beach", "Redwood City", "Reedley", "Rialto", "Richmond", "Ridgecrest", "Rio Dell", "Rio Vista", "Ripon", "Riverbank", "Riverside", "Rocklin", "Rohnert Park", "Rolling Hills", "Rolling Hills Estates", "Rosemead", "Roseville", "Ross", "Sacramento", "St. Helena", "Salinas", "San Anselmo", "San Bernardino", "San Bruno", "San Carlos", "San Clemente", "San Diego", "San Dimas", "San Fernando", "San Francisco", "San Gabriel", "San Jacinto", "San Joaquin", "San Jose", "San Juan Bautista", "San Juan Capistrano", "San Leandro", "San Luis Obispo", "San Marcos", "San Marino", "San Mateo", "San Pablo", "San Rafael", "San Ramon", "Sand City", "Sanger", "Santa Ana", "Santa Barbara", "Santa Clara", "Santa Clarita", "Santa Cruz", "Santa Fe Springs", "Santa Maria", "Santa Monica", "Santa Paula", "Santa Rosa", "Santee", "Saratoga", "Sausalito", "Scotts Valley", "Seal Beach", "Seaside", "Sebastopol", "Selma", "Shafter", "Shasta Lake", "Sierra Madre", "Signal Hill", "Simi Valley", "Solana Beach", "Soledad", "Solvang", "Sonoma", "Sonora", "South El Monte", "South Gate", "South Lake Tahoe", "South Pasadena", "South San Francisco", "Stanton", "Stockton", "Suisun City", "Sunnyvale", "Susanville", "Sutter Creek", "Taft", "Tehachapi", "Tehama", "Temecula", "Temple City", "Thousand Oaks", "Tiburon", "Torrance", "Tracy", "Trinidad", "Truckee", "Tulare", "Tulelake", "Turlock", "Tustin", "Twentynine Palms", "Ukiah", "Union City", "Upland", "Vacaville", "Vallejo", "Ventura", "Vernon", "Victorville", "Villa Park", "Visalia", "Vista", "Walnut", "Walnut Creek", "Wasco", "Waterford", "Watsonville", "Weed", "West Covina", "West Hollywood", "West Sacramento", "Westlake Village", "Westminster", "Westmorland", "Wheatland", "Whittier", "Wildomar", "Williams", "Willits", "Willows", "Windsor", "Winters", "Woodlake", "Woodland", "Woodside", "Yorba Linda", "Yountville", "Yreka", "Yuba City", "Yucaipa", "Yucca Valley"]

countyNameStr =["San Bernardino", "Los Angeles", "Alameda", "Alameda", "Los Angeles", "Orange", "Modoc", "Amador", "Napa", "Orange", "Shasta", "Calaveras", "Contra Costa", "San Bernardino", "Los Angeles", "Humboldt", "San Luis Obispo", "Los Angeles", "Kern", "San Luis Obispo", "San Mateo", "Merced", "Placer", "Los Angeles", "Kings", "Los Angeles", "Kern", "Los Angeles", "Riverside", "San Bernardino", "Riverside", "Los Angeles", "Los Angeles", "Los Angeles", "San Mateo", "Marin", "Solano", "Alameda", "Los Angeles", "San Bernardino", "Butte", "Inyo", "Humboldt", "Riverside", "Los Angeles", "Imperial", "Orange", "Contra Costa", "San Mateo", "Santa Barbara", "Orange", "Los Angeles", "San Mateo", "Los Angeles", "Imperial", "Kern", "Riverside", "Imperial", "Napa", "Ventura", "Santa Clara", "Riverside", "Santa Cruz", "San Diego", "Monterey", "Santa Barbara", "Los Angeles", "Riverside", "Stanislaus", "Los Angeles", "Butte", "San Bernardino", "San Bernardino", "Madera", "San Diego", "Sacramento", "Los Angeles", "Contra Costa", "Lake", "Sonoma", "Fresno", "Riverside", "Fresno", "Placer", "San Mateo", "San Bernardino", "Colusa", "Los Angeles", "Los Angeles", "Contra Costa", "Kings", "Tehama", "Riverside", "San Diego", "Marin", "Orange", "Sonoma", "Los Angeles", "Del Norte", "Los Angeles", "Los Angeles", "Santa Clara", "Orange", "San Mateo", "Orange", "Contra Costa", "Yolo", "San Diego", "Monterey", "Kern", "Riverside", "Los Angeles", "Tulare", "Solano", "Siskiyou", "Merced", "Los Angeles", "Los Angeles", "Alameda", "Siskiyou", "San Mateo", "Riverside", "San Diego", "Imperial", "Contra Costa", "Los Angeles", "Los Angeles", "Sacramento", "Alameda", "San Diego", "San Joaquin", "San Diego", "Siskiyou", "Humboldt", "Tulare", "Marin", "Solano", "Tulare", "Humboldt", "Ventura", "Fresno", "Sacramento", "San Bernardino", "Mendocino", "Siskiyou", "Humboldt", "San Mateo", "Orange", "Fresno", "Alameda", "Fresno", "Orange", "Sacramento", "Orange", "Los Angeles", "Santa Clara", "Los Angeles", "Los Angeles", "Santa Barbara", "Monterey", "San Bernardino", "Nevada", "Monterey", "Butte", "San Luis Obispo", "Santa Barbara", "Merced", "San Mateo", "Kings", "Los Angeles", "Los Angeles", "Alameda", "Sonoma", "Riverside", "Contra Costa", "Los Angeles", "San Bernardino", "Los Angeles", "San Bernardino", "San Mateo", "San Benito", "Imperial", "Stanislaus", "Orange", "Los Angeles", "Fresno", "Imperial", "San Diego", "Riverside", "Riverside", "Los Angeles", "Los Angeles", "Amador", "Orange", "Los Angeles", "Sacramento", "Amador", "Riverside", "Fresno", "Monterey", "Fresno", "Los Angeles", "Orange", "Los Angeles", "San Diego", "Los Angeles", "Orange", "Los Angeles", "Riverside", "Los Angeles", "Contra Costa", "Orange", "Orange", "Orange", "Orange", "Riverside", "Orange", "Lake", "Los Angeles", "Los Angeles", "Marin", "San Joaquin", "Los Angeles", "San Diego", "Kings", "Placer", "Tulare", "Sutter", "Alameda", "Merced", "San Joaquin", "San Bernardino", "Los Angeles", "Santa Barbara", "Los Angeles", "Placer", "Orange", "Santa Clara", "Santa Clara", "Los Angeles", "Merced", "Santa Clara", "Sierra", "Los Angeles", "Madera", "Los Angeles", "Mono", "Los Angeles", "San Joaquin", "Kern", "Monterey", "Contra Costa", "Yuba", "Los Angeles", "Kern", "Fresno", "Riverside", "San Mateo", "Merced", "Marin", "San Mateo", "Santa Clara", "Orange", "Stanislaus", "Los Angeles", "Siskiyou", "San Bernardino", "Santa Clara", "Los Angeles", "Monterey", "Los Angeles", "Ventura", "Contra Costa", "Riverside", "Santa Clara", "San Luis Obispo", "Siskiyou", "Santa Clara", "Riverside", "Napa", "San Diego", "San Bernardino", "Nevada", "Alameda", "Stanislaus", "Orange", "Riverside", "Los Angeles", "Marin", "Stanislaus", "Alameda", "Contra Costa", "San Diego", "Ventura", "San Bernardino", "Orange", "Fresno", "Contra Costa", "Glenn", "Butte", "Ventura", "Monterey", "San Mateo", "Riverside", "Riverside", "Los Angeles", "Santa Clara", "Los Angeles", "Butte", "Los Angeles", "Fresno", "Los Angeles", "San Luis Obispo", "Stanislaus", "Riverside", "Sonoma", "Los Angeles", "Alameda", "Contra Costa", "San Luis Obispo", "Contra Costa", "Orange", "El Dorado", "Contra Costa", "Alameda", "Amador", "Mendocino", "Los Angeles", "Ventura", "Tulare", "Plumas", "San Mateo", "San Diego", "Sacramento", "San Bernardino", "Riverside", "Los Angeles", "Orange", "Tehama", "Shasta", "San Bernardino", "Los Angeles", "San Mateo", "Fresno", "San Bernardino", "Contra Costa", "Kern", "Humboldt", "Solano", "San Joaquin", "Stanislaus", "Riverside", "Placer", "Sonoma", "Los Angeles", "Los Angeles", "Los Angeles", "Placer", "Marin", "Sacramento", "Napa", "Monterey", "Marin", "San Bernardino", "San Mateo", "San Mateo", "Orange", "San Diego", "Los Angeles", "Los Angeles", "San Francisco", "Los Angeles", "Riverside", "Fresno", "Santa Clara", "San Benito", "Orange", "Alameda", "San Luis Obispo", "San Diego", "Los Angeles", "San Mateo", "Contra Costa", "Marin", "Contra Costa", "Monterey", "Fresno", "Orange", "Santa Barbara", "Santa Clara", "Los Angeles", "Santa Cruz", "Los Angeles", "Santa Barbara", "Los Angeles", "Ventura", "Sonoma", "San Diego", "Santa Clara", "Marin", "Santa Cruz", "Orange", "Monterey", "Sonoma", "Fresno", "Kern", "Shasta", "Los Angeles", "Los Angeles", "Ventura", "San Diego", "Monterey", "Santa Barbara", "Sonoma", "Tuolumne", "Los Angeles", "Los Angeles", "El Dorado", "Los Angeles", "San Mateo", "Orange", "San Joaquin", "Solano", "Santa Clara", "Lassen", "Amador", "Kern", "Kern", "Tehama", "Riverside", "Los Angeles", "Ventura", "Marin", "Los Angeles", "San Joaquin", "Humboldt", "Nevada", "Tulare", "Siskiyou", "Stanislaus", "Orange", "San Bernardino", "Mendocino", "Alameda", "San Bernardino", "Solano", "Solano", "Ventura", "Los Angeles", "San Bernardino", "Orange", "Tulare", "San Diego", "Los Angeles", "Contra Costa", "Kern", "Stanislaus", "Santa Cruz", "Siskiyou", "Los Angeles", "Los Angeles", "Yolo", "Los Angeles", "Orange", "Imperial", "Yuba", "Los Angeles", "Riverside", "Colusa", "Mendocino", "Glenn", "Sonoma", "Yolo", "Tulare", "Yolo", "San Mateo", "Orange", "Napa", "Siskiyou", "Sutter", "San Bernardino", "San Bernardino"]

cityDf=pd.DataFrame(cityNameStr)
cityDf["countyName"]=countyNameStr
cityDf.rename(columns={0:"CityName"},inplace=True)

sql_1 = """select ID,State,County,Population,BloodPressure,Cancer,Asthma,HeartDiseases,Diabetes,PhysicalHealth>14Days,MentalHealth>14Days,ClinicalPriventiveServices;""" 
#import pandas as pd
#import pandas.io.sql as sqlio
#import psycopg2 as pg

 

try:
    connection = pg.connect(
    user = "raghav_dap",password = "dap@123",
    host = "87.44.4.13",
    port = "5432",
    database = "dap_h")
    disease_df1 = sqlio.read_sql_query(sql_1, connection)
except (Exception , pg.Error) as dbError :
    print ("Error:", dbError)
finally:
    if(connection): connection.close()

####### Data Transformation

disease_df1= pd.merge(disease_df,cityDf,on=['CityName'],how="inner")


disease_df1.drop(["ID","State", "Population","BloodPressure", "HeartDiseases", "Diabetes","PhysicalHealth>14Days","ClinicalPriventiveServices"], axis = 1,inplace = True)

disease_df1.rename(columns={"MentalHealth>14Days": "Mental_health"})

disease_df1.to_csv('diseases_updated.csv', sep=',')


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
CREATE TABLE Updated_diseases(
County                text PRIMARY KEY,
Cancer                 integer,
Asthma              integer(20),
Mental_Health           integer(20),
""")
    dbCursor.close()
except (Exception , pg.Error) as dbError :
    print ("Error while connecting to PostgreSQL", dbError)
finally:
    if(connection): connection.close()


###### Inserting the final data to posgresql

#import csv
file = input(diseases_updated.csv)
sql_insert = """INSERT INTO Updated_diseases(County,Cancer,Asthma,Mental_Health)
                VALUES(%s, %s, %s, %s, %s)"""
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



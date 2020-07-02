#### Retriving the 4 datasets from the postgresql to perform data integration

## uvirradiation

sql_1 = """select StateFips,countyfips,Irradationvalue,State from Updated_uvirradation;""" 
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2 as pg

 

try:
    connection = pg.connect(
    user = "raghav_dap",password = "dap@123",
    host = "87.44.4.13",
    port = "5432",
    database = "dap_h")
    irradation_final_df = sqlio.read_sql_query(sql_1, connection)
except (Exception , pg.Error) as dbError :
    print ("Error:", dbError)
finally:
    if(connection): connection.close()
	
## ozone

sql_1 = """select statefips,countyfips,ozone_con,county from Updated_ozone;""" 
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2 as pg

 

try:
    connection = pg.connect(
    user = "raghav_dap",password = "dap@123",
    host = "87.44.4.13",
    port = "5432",
    database = "dap_h")
    ozone_final_df = sqlio.read_sql_query(sql_1, connection)
except (Exception , pg.Error) as dbError :
    print ("Error:", dbError)
finally:
    if(connection): connection.close()
	
## diseases 

sql_1 = """select County,Cancer,Asthma,Mental_Health from Updated_diseases;""" 
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2 as pg

 

try:
    connection = pg.connect(
    user = "raghav_dap",password = "dap@123",
    host = "87.44.4.13",
    port = "5432",
    database = "dap_h")
    diseases_final_df = sqlio.read_sql_query(sql_1, connection)
except (Exception , pg.Error) as dbError :
    print ("Error:", dbError)
finally:
    if(connection): connection.close()

## air pollution

sql_1 = """select StateFips,countyfips,AirPollutionLevel from Updated_airpollution;""" 
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2 as pg

 

try:
    connection = pg.connect(
    user = "raghav_dap",password = "dap@123",
    host = "87.44.4.13",
    port = "5432",
    database = "dap_h")
    airpollution_final_df = sqlio.read_sql_query(sql_1, connection)
except (Exception , pg.Error) as dbError :
    print ("Error:", dbError)
finally:
    if(connection): connection.close()
	
###### Aggregation  has been performed on the state column

aggData_st_irradation = irradation_final_df.groupby(['StateFips']).mean()

aggData_st_ozone = ozone_final_df.groupby(['statefips']).mean()

aggData_st_airpollution = airpollution_final_df.groupby(['StateFips']).mean()

###### Left outer joined has been performed on the ozone,airpollution and irradiation dataset and  merged as single one 

df_st = aggData_st_irradation.merge(aggData_st_ozone[['statefips','ozone_con']], how='left', on=['statefips']).merge(aggData_st_airpollution[['StateFips', 'AirPollutionLevel']], how='left', on=['StateFips'])


###### Filter is performed to extract the state data 

df_filtered_ozone1 = ozone_final_df.query('statefips==6')
df_filtered_radiation1 = irradation_final_df.query('statefips==6')
df_filtered_airpollution1 = airpollution_final_df.query('statefips==6')

###### Aggregation is performed on county  data 

aggData_ct_irradation = df_filtered_radiation1.groupby(['countyfips']).mean()

aggData_ct_ozone = df_filtered_ozone1.groupby(['countyfips']).mean()

aggData_ct_airpollution = df_filtered_airpollution1.groupby(['countyfips']).mean()
	
##### Data has been joined on county wise

df_ct = aggData_irradation.merge(aggData_ozone[['countyfips','ozone_con']], how='left', on=['countyfips']).merge(aggData_airpollution[['countyfips', 'AirPollutionLevel']], how='left', on=['countyfips'])

aggData_ct_diseases = diseases_final_df.groupby(['County']).mean()

###### Data has been joined on county and diseses wise

df_ct1 = df_ct.merge(aggData_ct_diseases[['County','Cancer','Asthma','Mental_Health']], how='left', on=['County'])


###### Dropping the unneccessary column

df_ct1_pollution = df_ct1.drop(["Cancer", "Asthma","Mental_Health"], axis = 1,inplace = True)

df_ct1_diseases = df_ct1.drop(["Irradationvalue", "ozone_con","AirPollutionLevel"], axis = 1,inplace = True)


###### Melting the column to give as an input to the visualization

melteddata_pol = pd.melt(df_ct1_pollution, id_vars =['countyfips','County'], value_vars =['Irradationvalue', 'ozone_con','AirPollutionLevel'])

melteddata_diseases = pd.melt(df_ct1_diseases, id_vars =['countyfips','County'], value_vars =['Cancer','Asthma','Mental_Health'])

melteddata = pd.melt(df_ct1, id_vars =['countyfips','County'], value_vars =['Irradationvalue', 'ozone_con','AirPollutionLevel','Cancer','Asthma','Mental_Health'])

#############################visualisation####################################################

# Ozone geomap

import chart_studio.plotly as py
from plotly import graph_objs as go
from plotly.offline import download_plotlyjs,plot,iplot

data = dict (type ='choropleth',
             colorscale='ylorrd',
             locations =df_st['statefips'],
             z = df_st['ozone_con'],
             locationmode ='USA-states',
             text=df_st['text'],
             marker =dict(line =dict(color ='rgb(255,255,255)',width=5)),
             colorbar={'title':"Scale"}
            )

layout =dict(title=' USA state research',
             geo =dict(scope='usa'))

choromap = go.Figure(data =[data],layout=layout)

from chart_studio.plotly import iplot
from  plotly.offline import plot
import plotly
plotly.offline.plot(choromap,filename='geoozonemap.html')

##############################################################################################

#Airpollution geomap

import chart_studio.plotly as py
from plotly import graph_objs as go
from plotly.offline import download_plotlyjs,plot,iplot

data = dict (type ='choropleth',
             colorscale='ylorrd',
             locations =df_st['statefips'],
             z = df_st['AirPollutionLevel'],
             locationmode ='USA-states',
             text=df_st['text'],
             marker =dict(line =dict(color ='rgb(255,255,255)',width=5)),
             colorbar={'title':"Scale"}
            )

layout =dict(title=' USA state research',
             geo =dict(scope='usa'))

choromap = go.Figure(data =[data],layout=layout)

from chart_studio.plotly import iplot
from  plotly.offline import plot
import plotly
plotly.offline.plot(choromap,filename='geoairpollutionmap.html')

################################################################################################

#Irradiation geomap

import chart_studio.plotly as py
from plotly import graph_objs as go
from plotly.offline import download_plotlyjs,plot,iplot

data = dict (type ='choropleth',
             colorscale='ylorrd',
             locations =df_st['statefips'],
             z = df_st['Irradationvalue'],
             locationmode ='USA-states',
             text=df_st['text'],
             marker =dict(line =dict(color ='rgb(255,255,255)',width=5)),
             colorbar={'title':"Scale"}
            )

layout =dict(title=' USA state research',
             geo =dict(scope='usa'))

choromap = go.Figure(data =[data],layout=layout)

from chart_studio.plotly import iplot
from  plotly.offline import plot
import plotly
plotly.offline.plot(choromap,filename='geoirradationmap.html')

##################################################################################################
#Bar plot

import plotly.express as px

fig1 = px.bar(melteddata_pol,x="County",y="value",color="variable",labels={'y':'Value'}, title='CALIFORNIA COUNTY LEVEL UV Irradiance,Ozone concentration,PM 2.5 density')

plotly.offline.plot(fig3,filename='geoairpollutionmapsfinal.html')

##################################################################################################
#Scatter  plot

import plotly.express as px

df = pd.read_csv('finalradiationcc.csv')

fig = px.scatter(df,x="County name",y="Concentration level",color="variable",facet_col="variable",
           color_continuous_scale=px.colors.sequential.Viridis)
fig.update_xaxes(showgrid=False)
plotly.offline.plot(fig,filename='geodiseasesfinal.html')












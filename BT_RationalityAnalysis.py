
# coding: utf-8

# In[1]:


#before using this you need to make a link-breaked version of the roads in yoru database with the osm2pgrourting tool. 
#before use it requires a security setup: (for security reasons it wqill only run on a database which has a password setup):
#$ sudo -u postgres psql
# \password
# (enter 'postgres' for the password twice)
# Ctrl-D (to exit psql)
#Then run the link-breaking with:
#$ osm2pgrouting -f data/dcc.osm -d mydatabasename -W postgres
#you also need to do
# $ psql -d mydatabasename
# CREATE EXTENSION pgrouting
#in psql, to enable postgres routing extension.

##This code has OD rationality plots , but not map visualisations

import psycopg2
import pandas as pd
import geopandas as gpd
import pyproj
import os,re,datetime
from matplotlib.pyplot import *
con = psycopg2.connect(database='mydatabasename', user='root')
cur = con.cursor()
wgs84  = pyproj.Proj(init='epsg:4326')  #WGS84
bng    = pyproj.Proj(init='epsg:27700') #british national grid
import pandas.io.sql as psql # To read sql files
get_ipython().run_line_magic('matplotlib', 'inline')
import matplotlib.pyplot as plt
#import random
#from random import randint
print ("import complete")


# In[3]:


##Importing BTsite detection site CSVs and cleaning the repetitive counts for each site, 
##with some exceptions of alternate detections

#Cleaning CSV while importing
#like a wrapper
class currentUnique():
    lastN = [] #Empty array
    N = 60 #Timestamp difference set

    def calc(self,id,time): #Function inside class , id = mac id , timestamp = time
        # print "last", self.lastN
        idx = [index for index,item in enumerate(self.lastN) if item[0] == id] 
        # print sum(idx)
        self.lastN.append((id,time))
        if idx:
            # print 'present'
            old_time = self.lastN[idx[0]][1]
            del self.lastN[idx[0]]
            if (time - old_time).seconds < self.N:
                return False
            else:
                return True
        else:
            # print 'not present'
            return True

def importDetections():
    print("importing detections...")
    sql = "DROP TABLE IF EXISTS detection_clean;"
    cur.execute(sql)    
    sql = "CREATE TABLE detection_clean ( id serial, siteID text, mac text,     timestamp timestamp );"
    cur.execute(sql)    
    dir_detections = "/headless/data/dcc/bluetooth/"
    for fn in sorted(os.listdir(dir_detections)):  #importing all BT sensor files
        print("processing file: " +fn)
        m = re.match("vdFeb14_(.+).csv", fn)  #extracting CSVs matching the file names
        if m is None:  #if there was no regex match then continue
            continue   
        siteID = m.groups()[0]
        fn_detections = dir_detections+fn
        
        #dataframe.header is which row to use for field names-fn
        df_detections = pd.read_csv(fn_detections, header=0)
        
        #Cleaning CSV files through Class defined
        cu = currentUnique()#new class
        cu.lastN = []
        print (cu.lastN)
        for i in range(0, df_detections.shape[0]): 
            #here we use Python's DateTime library to store times properly
            datetime_text = df_detections.iloc[i]['Unnamed: 0']
            #proper Python datetime  
            dt = datetime.datetime.strptime(datetime_text, "%d/%m/%Y %H:%M:%S") 
            cur_mac = df_detections.iloc[i]['Number Plate']
            newDetection = cu.calc(cur_mac, dt)
            if not newDetection:
                continue
            #Insert Cleaned detections into the detection table
            sql = "INSERT INTO detection_clean (siteID, timestamp, mac) VALUES ('%s', '%s', '%s');"            %(siteID, dt, df_detections.iloc[i]['Number Plate'])
            cur.execute(sql)
      
        table = psql.read_sql("SELECT * FROM detection_clean", con)
        print ("Length of  table: ", len(table), df_detections.shape[0])
    con.commit()
    
    print("Cleaning done")
    
importDetections()

####Alternative method for Cleaning Detection table using PSQL, here The CSVs have to imported raw into the PSQL
#The raw tables can then be cleaned using the following code
"""def cleanedDetections(con,cur):
    print("cleaning detections...")
    sql="DROP TABLE IF EXISTS CleanedDetection;"
    cur.execute(sql)
    sql = "CREATE TABLE CleanedDetection AS SELECT * FROM Detection;"
    cur.execute(sql)
    sql="DELETE FROM CleanedDetection d1 USING CleanedDetection d2 WHERE \
            d1.timestamp<d2.timestamp + interval '1 minute' \
            AND d2.mac=d1.mac AND d2.siteid=d1.siteid \
             AND d2.id<d1.id;"
    cur.execute(sql)          
    con.commit() 
cleanedDetections(con,cur)"""



# In[4]:


###Routes and Vehicle Counts between selected OD pairs 

###Say for example the OD pair is A-B and the routes between them are A-C-B and A-D-B

#Table for total OD route eg from A-to-B
def createTotalRouteCount(con,cur):
    sql = "DROP TABLE IF EXISTS TotalRouteCount;"
    cur.execute(sql)          
    sql="CREATE TABLE TotalRouteCount(         emp_no SERIAL PRIMARY KEY,         ODrouteID text, OriginSiteID text, DestSiteID text,         timestamp timestamp,         winlenseconds integer,         count integer);"
    cur.execute(sql)          
    con.commit() 

createTotalRouteCount(con,cur)

def makeTotalRouteCounts(con,cur):
    print("define Total measureable route and count, from origin sensor to destination sensor")

    ODRoutes = {"MeasureableRouteID":["MAC000010119>MAC000010130",                                     "MAC000010102>MAC000010104",                                     "MAC000010121>MAC000010124",                                     "MAC000010101>MAC000010119",                                     "MAC000010123>MAC000010120"],               "OriginSiteID":["MAC000010119","MAC000010102",                               "MAC000010121",                               "MAC000010101","MAC000010123"],

                "DestSiteID":["MAC000010130","MAC000010104",\
                              "MAC000010124",\
                              "MAC000010119","MAC000010120"]}
    df=pd.DataFrame.from_dict(ODRoutes)
    for i in range(0,df.shape[0]):    #each route
        routeID = df['MeasureableRouteID'][i]
        oSiteID = df['OriginSiteID'][i]
        dSiteID = df['DestSiteID'][i]
        
        #MAC matching with an assumption that,---
        #every vehicle takes atleast 60 minutes to from A to B through any route
        sql = "SELECT d.siteID AS dSiteID,  d.mac as dmac,         d.timestamp as dtimestamp, o.siteID AS oSiteID,          o.mac as omac, o.timestamp as otimestamp            FROM Detection_clean AS d, Detection_clean AS o          WHERE d.timestamp>o.timestamp AND d.timestamp-o.timestamp<interval'60 minutes'         AND o.mac=d.mac AND o.siteID='%s'        AND d.siteID='%s';"%(oSiteID, dSiteID)
        print(sql)
        df_matches = pd.read_sql_query(sql,con)
        count = df_matches.shape[0]  #count number of bluetooth matches
        #these two variables allow us to compute flows for differnt time windows. Here we just take one whole day.
        winlenseconds = 99999999.9
        timestamp = "2015-02-14 09:00:00"
        sql = "INSERT INTO TotalRouteCount (ODrouteID, OriginSiteID, DestSiteID, timestamp,        winlenseconds, count) VALUES ('%s', '%s', '%s', '%s', %f, %i)"        %(routeID,oSiteID, dSiteID,timestamp, winlenseconds, count)
        cur.execute(sql)
        print (sql)
    con.commit()
    print ("Total routes and counts created")
makeTotalRouteCounts(con,cur)

##Table for total OMD route eg from A-C-B and A-D-B
def createODRouteCounts(con,cur):
    sql = "DROP TABLE IF EXISTS ODRouteCount;"
    cur.execute(sql)          
    sql="CREATE TABLE ODRouteCount (             emp_no SERIAL PRIMARY KEY,             ODrouteID text,OriginSiteID text, MidSiteID text,             DestSiteID text,             timestamp timestamp,             winlenseconds integer,             count integer);"
    cur.execute(sql)          
    con.commit() 

createODRouteCounts(con,cur)

def makeODRouteCounts(con,cur):
    print("define OD measureable route and count, from each sensor to each other")
    
    Routes = {"MeasureableRouteID":["MAC000010119>MAC000010104>MAC000010130",                                     "MAC000010119>MAC000010109>MAC000010130",                                     "MAC000010102>MAC000010119>MAC000010104",                                     "MAC000010102>MAC000010118>MAC000010104",                                     "MAC000010121>MAC000010113>MAC000010124",                                     "MAC000010121>MAC000010112>MAC000010124",                                     "MAC000010101>MAC000010104>MAC000010119",                                     "MAC000010101>MAC000010118>MAC000010119",                                     "MAC000010123>MAC000010114>MAC000010120",                                     "MAC000010123>MAC000010112>MAC000010120"],               "OriginSiteID":["MAC000010119","MAC000010119","MAC000010102",                               "MAC000010102","MAC000010121","MAC000010121",                               "MAC000010101","MAC000010101","MAC000010123",                               "MAC000010123"],
                "MidSiteID": ["MAC000010104","MAC000010109","MAC000010119",\
                              "MAC000010118","MAC000010113","MAC000010112",\
                              "MAC000010104","MAC000010118","MAC000010114",\
                              "MAC000010112"],
                "DestSiteID":["MAC000010130","MAC000010130","MAC000010104",\
                              "MAC000010104","MAC000010124","MAC000010124",\
                              "MAC000010119","MAC000010119","MAC000010120",\
                              "MAC000010120"]}
    df=pd.DataFrame.from_dict(Routes)
# or enter for i in range(0,df.shape[0])    
    for i in range(0,df.shape[0]):    #each route
        routeID = df['MeasureableRouteID'][i]
        oSiteID = df['OriginSiteID'][i]
        mSiteID = df['MidSiteID'][i]
        dSiteID = df['DestSiteID'][i]
         #MAC matching with an assumption that,---
        #every vehicle takes atleast 30 minutes to  from A to C and then,---
        # C to B through any route
        sql = "SELECT d.siteID AS dSiteID,  d.mac as dmac,         d.timestamp as dtimestamp  , m.siteID AS mSiteID,  m.mac as mmac,         m.timestamp as mtimestamp,  o.siteID AS oSiteID,          o.mac as omac, o.timestamp as otimestamp            FROM Detection_clean AS d, Detection_clean AS m, Detection_clean AS o          WHERE d.timestamp>m.timestamp AND m.timestamp>o.timestamp AND d.timestamp>o.timestamp        AND d.timestamp-m.timestamp<interval'30 minutes' AND m.timestamp-o.timestamp<interval '30minutes' AND        o.mac=m.mac AND m.mac=d.mac AND d.mac=m.mac AND o.siteID='%s'        AND m.siteID = '%s' AND d.siteID='%s';" %(oSiteID, mSiteID, dSiteID)
        print(sql)
        df_matches = pd.read_sql_query(sql,con)
        #print(df_matches)
        count = df_matches.shape[0]  #count number of bluetooth matches
        #these two variables allow us to compute flows for differnt time windows. Here we just take one whole day.
        winlenseconds = 99999999.9
        timestamp = "2015-02-14 09:00:00"
        sql = "INSERT INTO ODRouteCount (ODrouteID, OriginSiteID, MidSiteID, DestSiteID, timestamp,        winlenseconds, count) VALUES ('%s', '%s','%s','%s','%s', %f, %i);"        %(routeID,oSiteID, mSiteID, dSiteID,timestamp, winlenseconds, count)
        cur.execute(sql)
        print (sql)
    con.commit()
print ("OD routes  and counts created")
makeODRouteCounts(con,cur)


# In[5]:


##Calculating Distance between the chosen set of Sensors

#The table "Shortest Route" has the routes for Origin to Mid to Destination sensor points

def createSensorDistances(con,cur):
    sql = "DROP TABLE IF EXISTS SensorDistancesRaw;" #Raw  calculation table
    cur.execute(sql)          
    sql = "CREATE TABLE SensorDistancesRaw (             emp_no SERIAL PRIMARY KEY,             ODRouteID text, gid_link text,             om_length integer, md_length integer            );"
    cur.execute(sql) 
    sql = "DROP TABLE IF EXISTS ShortestRoute;"
    cur.execute(sql)        
    sql = "CREATE TABLE ShortestRoute (             emp_no SERIAL PRIMARY KEY,             ODrouteID text,             om_length integer,             md_length integer,             total_length integer             );"
    cur.execute(sql)      
createSensorDistances(con,cur)

def SensorDistances(con,cur):#Raw  calculation table
    print ("computing lengths ..")
    sql = "SELECT ODRouteID, OriginSiteID, MidSiteID,             DestSiteID, ST_X(orig.geom) AS ox, ST_Y(orig.geom)             AS oy, ST_X(mid.geom) AS mx, ST_Y(mid.geom) AS my, ST_X(dest.geom) AS dx,            ST_Y(dest.geom) AS dy from ODRouteCount,             BluetoothSite AS orig, BluetoothSite AS mid, BluetoothSite AS dest             WHERE OriginSiteID=orig.siteID AND MidSiteID=mid.siteID AND             DestSiteID=dest.siteID;"
    #Read bulk of db in psql and enter into DF
    df_dist = pd.read_sql_query(sql,con)
    print (df_dist)
    N = df_dist.shape[:]   
    print (N, "shape of DF")
# for i in range(0, df_dist) or  i in range (10,7) - Error fixed
    for i in range (0, df_dist.shape[0]): 
        OMDrouteID = df_dist['odrouteid'][i]
        origsiteID = df_dist['originsiteid'][i]
        midsiteID  = df_dist['midsiteid'][i]
        destsiteID = df_dist['destsiteid'][i]
        o_easting  = df_dist['ox'].iloc[i]#link-broken table is stored as latlon so convert on the fly
        o_northing = df_dist['oy'].iloc[i]
        m_easting  = df_dist['mx'].iloc[i]
        m_northing = df_dist['my'].iloc[i]
        d_easting  = df_dist['dx'].iloc[i]
        d_northing = df_dist['dy'].iloc[i]
        
        (o_lon,o_lat) = pyproj.transform(bng, wgs84, o_easting, o_northing) #project uses nonISO lonlat convention
        (m_lon,m_lat) = pyproj.transform(bng, wgs84, m_easting, m_northing)
        (d_lon,d_lat) = pyproj.transform(bng, wgs84, d_easting, d_northing) 

        #Origin gid
        sql = "SELECT id,ST_Distance(ways_vertices_pgr.the_geom,        ST_SetSRID(ST_MakePoint(%f, %f),4326)) FROM ways_vertices_pgr         ORDER BY st_distance ASC LIMIT 1;"%(o_lon,o_lat)    #4326=SRID code for WGS84
        df_on = pd.read_sql_query(sql,con)
        o_vertex_gid = df_on['id'][0] #get the vertexID of its source
        print ("Origin GIDs selected")

        #Mid gid
        sql = "SELECT id,ST_Distance(ways_vertices_pgr.the_geom,        ST_SetSRID(ST_MakePoint(%f, %f),4326)) FROM ways_vertices_pgr         ORDER BY st_distance ASC LIMIT 1;"%(m_lon,m_lat)    #4326=SRID code for WGS84
        df_mn = pd.read_sql_query(sql,con)
        m_vertex_gid = df_mn['id'][0] #get the vertexID of its source
        print ("Mid GIDs selected")
    
        #Dest gid
        sql = "SELECT id,ST_Distance(ways_vertices_pgr.the_geom,        ST_SetSRID(ST_MakePoint(%f, %f),4326)) FROM ways_vertices_pgr         ORDER BY st_distance ASC LIMIT 1;"%(d_lon,d_lat)    #4326=SRID code for WGS84
        df_dn = pd.read_sql_query(sql,con)
        d_vertex_gid = df_dn['id'][0] #get the vertexID of its source
        print ("Dest GIDs selected")
        
        #pgr_dijkstra(o_vertex_gid, m_vertex_gid)
        sql = "SELECT * FROM pgr_dijkstra('SELECT gid AS id, source, target,         length_m AS cost FROM ways', %d,%d, directed := false),         ways  WHERE ways.gid=pgr_dijkstra.edge;"%(o_vertex_gid, m_vertex_gid)
        print ("Shortest path between origin and mid sensors found")
        df_route_1 = gpd.GeoDataFrame.from_postgis(sql, con, geom_col='the_geom')
        OM_route   = pd.read_sql_query(sql,con) #Origin to Mid sites DF

        #pgr_dijkstra(m_vertex_gid, d_vertex_gid)        
        sql = "SELECT * FROM pgr_dijkstra('SELECT gid AS id, source, target,         length_m AS cost FROM ways', %d,%d, directed := false),         ways  WHERE ways.gid=pgr_dijkstra.edge;"%(m_vertex_gid, d_vertex_gid)
        print ("Shortest path between mid and destination sensors found")
        df_route_2 = gpd.GeoDataFrame.from_postgis(sql, con, geom_col='the_geom')
        MD_route   = pd.read_sql_query(sql,con) #Mid to Destination Site DF

        for i in range(0,df_route_1.shape[0]):
            rc = df_route_1.iloc[i]  #route component
            sql = "INSERT INTO SensorDistancesRaw(ODrouteID, gid_link, om_length)                 VALUES ('%s', %f, %f);" %(OMDrouteID, rc['gid'],rc['length_m'])
                #print(sql)
            cur.execute(sql)
        for i in range(0,df_route_2.shape[0]):
            rc = df_route_2.iloc[i]  #route component
            sql = "INSERT INTO SensorDistancesRaw(ODrouteID, gid_link, md_length)                 VALUES ('%s', %f, %f);" %(OMDrouteID, rc['gid'],rc['length_m'])
                #print(sql)
            cur.execute(sql)
    sql_om = "SELECT distinct odrouteid, sum(om_length) as om from sensordistancesraw    group by odrouteid;"
    df_om = pd.read_sql_query(sql_om,con)
    print(df_om)

    sql_md = "SELECT distinct odrouteid, sum(md_length) as md from sensordistancesraw    group by odrouteid;"
    df_md = pd.read_sql_query(sql_md,con)
    print(df_md)
    
    df_total= pd.merge(df_om,df_md, how ='left', on =['odrouteid'])
    print (df_total)
    #Length tables
    
  #The final table with distances between the routes  
    for i in range(0,df_total.shape[0]):
        rl = df_total.iloc[i]
        sql = "INSERT INTO ShortestRoute(ODrouteID, om_length, md_length, total_length)         VALUES ('%s', %f, %f, %f);" %(rl['odrouteid'], rl['om'],rl['md'],                                      rl['om']+rl['md'])
        print(sql)
        cur.execute(sql)            
    con.commit()
    print ('Sensor to sensor distances calculated' )
       
SensorDistances(con,cur)




# In[36]:


#plot of Flows vs route id 
get_ipython().run_line_magic('matplotlib', 'inline')
import matplotlib.pyplot as plt

def plotFlowsvsOD(con,cur):     
    sql = "SELECT odrouteid, count FROM odroutecount;" 
    print(sql)
    #fig, ax = subplots()
    df = pd.read_sql_query(sql,con)
    print (df)
    ax= df.plot(kind ='bar')
   
    plt.legend(["count"])
    plt.suptitle = "Flows at chosen OD pairs"
    
    plt.xticks(rotation=25)
    plt.xlabel("OD Route")
    plt.ylabel("Flows")
    

plotFlowsvsOD(con,cur)
print ("Flows Vs OD plotted")



# In[8]:


#plot of Flows vs od distances
get_ipython().run_line_magic('matplotlib', 'inline')
import matplotlib.pyplot as plt

def plotFlowsvsODdist(con,cur):     
    sql_count = "SELECT odrouteid, count FROM odroutecount order by odrouteid;"
    sql_id = "SELECT odrouteid, total_length     from shortestroute order by odrouteid;" 
    df = pd.read_sql_query(sql_count,con)
    df_1 = pd.read_sql_query(sql_id,con)
    df_new = pd.merge(df,df_1, how ='left', on =['odrouteid'])
    print(df_new)
    
    x1 = df_new['total_length'][0:2]
    y1 = df_new['count'][0:2]
    #figure(1)
    f1=plot(x1, y1)
    #xlabel('Distance (m)'); ylabel('Traffic flow'); title("Relation of Traffic flow-Distance on different routes between MAC000010101-MAC000010119")
    x2 = df_new['total_length'][2:4]
    y2 = df_new['count'][2:4]
    #figure(2)
    f2=plot(x2, y2)
    #xlabel('Distance (m)'); ylabel('Traffic flow'); title("Relation of Traffic flow-Distance on different routes between MAC000010102-MAC000010104")
    x3 = df_new['total_length'][4:6]
    y3 = df_new['count'][4:6]
    #figure(3)
    f3=plot(x3, y3)
    #xlabel('Distance (m)'); ylabel('Traffic flow'); title("Relation of Traffic flow-Distance on different routes between MAC000010119-MAC000010130")
    x4 = df_new['total_length'][6:8]
    y4 = df_new['count'][6:8]
    #figure(4)
    f4=plot(x4, y4)
    #xlabel('Distance (m)'); ylabel('Traffic flow'); title("Relation of Traffic flow-Distance on different routes between MAC000010121-MAC000010124")
    x5 = df_new['total_length'][8:10]
    y5 = df_new['count'][8:10]
    #figure(5)
    f5=plot(x5, y5)
    #xlabel('Distance (m)'); ylabel('Traffic flow'); title("Relation of Traffic flow-Distance on different routes between MAC000010123-MAC000010120")
    xlabel('Distance (m)'); ylabel('Traffic flow'); title("Relation of Traffic Flow & Distance on Different Routes")
    


''' fig = plt.figure()
    print(df.shape[0])
    #print(df)
    for i in range(0,df.shape[0]): 
        print(df_new.iloc[i]['total_length'],df.iloc[i]['count'])   
        plt.scatter(df_new.iloc[i]['total_length'],df.iloc[i]['count'])
        plt.xlim(2500,5000) 
        plt.ylim(0,425) 
        plt.plot(marker =".", markersize =10)
        plt.title = ("Flows at chosen OD pairs")
        plt.xlabel("OD Route Distance")
        plt.ylabel("Traffic Flow")
    plt.show()'''


''''def plotRationality(con,cur):
    sql="SELECT * FROM ODRouteCountDist;"
    df = pd.read_sql_query(sql,con)
    x1 = df['distance'][0:2]
    y1 = df['count'][0:2]
    #figure(1)
    f1=plot(x1, y1)
    xlabel('Distance (m)'); ylabel('Traffic flow'); title("Relation of Traffic flow-Distance on different routes between MAC000010101-MAC000010119")
    x2 = df['distance'][2:4]
    y2 = df['count'][2:4]
    #figure(2)
    f2=plot(x2, y2)
    xlabel('Distance (m)'); ylabel('Traffic flow'); title("Relation of Traffic flow-Distance on different routes between MAC000010102-MAC000010104")
    x3 = df['distance'][4:6]
    y3 = df['count'][4:6]
    #figure(3)
    f3=plot(x3, y3)
    xlabel('Distance (m)'); ylabel('Traffic flow'); title("Relation of Traffic flow-Distance on different routes between MAC000010119-MAC000010130")
    x4 = df['distance'][6:8]
    y4 = df['count'][6:8]
    #figure(4)
    f4=plot(x4, y4)
    xlabel('Distance (m)'); ylabel('Traffic flow'); title("Relation of Traffic flow-Distance on different routes between MAC000010121-MAC000010124")
    x5 = df['distance'][8:10]
    y5 = df['count'][8:10]
    #figure(5)
    f5=plot(x5, y5)
    xlabel('Distance (m)'); ylabel('Traffic flow'); title("Relation of Traffic flow-Distance on different routes between MAC000010123-MAC000010120")
plotRationality(con,cur)'''


plotFlowsvsODdist(con,cur)
print ("Flows Vs OD Distances plotted")


# In[2]:



#Vehicles on other paths - Section 2 

# Load total OD flows and put them into a dafaframe
sql_od = "Select OriginSiteID as O, DestSiteID as D, count as flow from Totalroutecount order by O;"
df_od = pd.read_sql_query(sql_od,con) #df of OD flow

# Load total OmD routes, flows and their respective distance. Then select 
# only shortest paths from each OD pair
sql_count = "SELECT odrouteid, originsiteid as o, midsiteid as m,             destsiteid as d, count FROM odroutecount order by odrouteid;"
sql_d = "SELECT odrouteid, total_length     from shortestroute order by odrouteid;" 
df_1 = pd.read_sql_query(sql_count,con)
df_2 = pd.read_sql_query(sql_d,con)
df_new = pd.merge(df_1,df_2, how ='left', on =['odrouteid'])
# Here the filtering of shortest paths take place
df_new=df_new.sort_values(['o', 'total_length'], ascending=[True,True])
df_new['dist_comparison']=df_new['total_length'].shift(-1)
df_new['boolean_check']=df_new['total_length']<df_new['dist_comparison']
newDF = df_new[(df_new['boolean_check']==True)]

print (df_od)
print(newDF)

# Merge two dfs
df = pd.merge(df_od, newDF, how ='left', on =['o'])

df["flow_alt"] = df["flow"] - df["count"] #People taking other routes
df["flow_alt"] = df["flow_alt"].abs()
print(df)


#%drivers on alternative routes
df["%Alternate_Path"] = df["flow_alt"]/ df["flow"] *100
print(df)

#%drivers on shortest path

df["%Shortest_Path"] = df["count"]/df["flow"] *100

#Histogram with %flows on routes

df_2 = pd.concat([df["%Shortest_Path"], df["%Alternate_Path"]], axis = 1,                  keys = ["%Shortest_Path", "%Alternate_Path"])
print (df_2)
axes = df_2.plot(kind ='bar',stacked =True)
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
labels = axes.get_xticks().tolist()
labels =["MAC000010101", "MAC000010102", "MAC000010119", "MAC000010121",         "MAC000010123"]
axes.set_xticklabels(labels)
plt.xticks(rotation=25)
plt.suptitle ("Comparison of Routes Taken")
plt.xlabel("OD pairs")
plt.ylabel("Percentage of Vehicles")
plt.savefig('StackGraph.png')


# In[9]:



#Vehicles on other paths 

##This can be used to see the difference between measured paths and alternate paths without sensors.

sql_od = "Select OriginSiteID as O, DestSiteID as D, count as flow from Totalroutecount;"
sql_omd = "Select OriginSiteID as Om, DestSiteID as Dm, count as flow_m from ODroutecount;"
df_od = pd.read_sql_query(sql_od,con) #df of OD flow
df_omd = pd.read_sql_query(sql_omd,con) #df for OMD flows
df_sum = df_omd.groupby("om")["flow_m"].sum().reset_index() #sum of omd route flows
merge_df = df_od.merge(df_sum, left_on= 'o', right_on = 'om', how ='outer') #merge tables
df = merge_df.drop(columns=['om'])


print (df_od)
print (df_omd)
print(df_sum)
print(merge_df)
print (df)

df["flow_alt"] = df["flow"] - df["flow_m"] #People taking other routes
df["flow_alt"] = df["flow_alt"].abs()
print(df)


#%drivers on alternative routes
df["%Alternate_Path"] = df["flow_alt"]/ df["flow"] *100
print(df)

#%drivers on shortest path

df["%Shortest_Path"] = df["flow_m"]/df["flow"] *100

#Histogram with %flows on routes

df_2 = pd.concat([df["%Shortest_Path"], df["%Alternate_Path"]], axis = 1,                  keys = ["%Shortest_Path", "%Alternate_Path"])
print (df_2)
axes = df_2.plot(kind ='bar',stacked =True)
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
labels = axes.get_xticks().tolist()
labels =["MAC000010119", "MAC000010102","MAC000010121","MAC000010101", "MAC000010123"]
axes.set_xticklabels(labels)
plt.xticks(rotation=25)
plt.suptitle ("Comparison of Routes Taken")
plt.xlabel("OD pairs")
plt.ylabel("Percentage of Vehicles")


''''#%Flows on alternative routes Vs OD
fig2, ax = subplots()
df.plot(ax=ax,x="o", y = "%Alternate_Path",  marker =".", markersize =10, \
        title = "Percentage of drivers on other routes at chosen OD pairs")
ax.legend(["%drivers"])
ax.set_xlabel("ID")
ax.set_ylabel("%Flows on Alternative Routes per Route")

#plot on Shortest Path
fig3, ax = subplots()
df.plot(ax=ax,x="flow_m", y = "%Shortest_Path",  marker =".", markersize =10, \
        title = "Percentage of drivers on Shortest routes at chosen OD pairs")
ax.legend(["%drivers"])
ax.set_xlabel("Total Flows")
ax.set_ylabel("%Flows on Shortest routes")

print ("%Flows on Shortest Routes Vs Total Flows plotted")

#plot on Alternate Path
fig1, ax = subplots()
df.plot(ax=ax,x="flow_m", y = "%Alternate_Path",  marker =".", markersize =10, \
        title = "Percentage of drivers on other routes at chosen OD pairs")
ax.legend(["%drivers"])
ax.set_xlabel("Total Flows")
ax.set_ylabel("%Flows on alternative routes")

print ("%Flows on Alternate rotes Vs Total Flows plotted")

#%Flows on alternative routes Vs OD
fig4, ax = subplots()
df.plot(ax=ax,x="o", y = "%Shortest_Path",  marker =".", markersize =10, \
        title = "Percentage of drivers on Shortest routes at chosen OD pairs")
ax.legend(["%drivers"])
ax.set_xlabel("ID")
ax.set_ylabel("%Flows on Shortest Routes per Route")'''


# In[ ]:


#############################End of Analysis#####################################
##################################################################################


# In[ ]:


###Except Visualisations#####Look in Visualisations Code
get_ipython().run_line_magic('matplotlib', 'inline')
#Importing Geometries

def importRoads():     #(data has come from openstreetmap, then ogr2ogr )
    print("importing roads...")
    sql = "DROP TABLE IF EXISTS Road;"
    cur.execute(sql)
    sql = "CREATE TABLE Road (name text, geom geometry, highway text);"
    cur.execute(sql)
    fn_osm_shp = "/headless/data/dcc.osm.shp/lines.shp"
    df_roads = gpd.GeoDataFrame.from_file(fn_osm_shp)
    df_roads = df_roads.to_crs({'init': 'epsg:27700'})
    for index, row in df_roads.iterrows():
        sql="INSERT INTO Road VALUES ('%s', '%s', '%s');"        %(row.name, row.geometry, row.highway )
        cur.execute(sql)
    con.commit()
    print ("Roads imported")

importRoads()

##Creating list of BT sites table
def importBluetoothSites():
    print("importing sites...")
    sql = "DROP TABLE IF EXISTS BluetoothSite;"
    cur.execute(sql)
    sql = "CREATE TABLE BluetoothSite ( id serial PRIMARY KEY, siteID text,    geom geometry);"
    cur.execute(sql)
    con.commit()    
    fn_sites = "/headless/data/sites.csv"
    df_sites = pd.read_csv(fn_sites, header=1)   #dataframe. header is which row to use for the field names.
    for i in range(0, df_sites.shape[0]):      #munging to extract the coordinates - the arrive in National Grid
        locationstr = str(df_sites.iloc[i]['Grid'])
        bng_east  = locationstr[0:6]
        bng_north = locationstr[6:12]
        sql = "INSERT INTO BluetoothSite (siteID, geom)         VALUES ('%s', 'POINT(%s %s)');"%(df_sites.iloc[i]['Site ID'],         bng_east, bng_north )
        cur.execute(sql)
    con.commit()        
    print ("BT Sites imported")
importBluetoothSites()

def plotRoads():   
    print("plotting roads...")
    sql = "SELECT * FROM Road;"
    df_roads = gpd.GeoDataFrame.from_postgis(sql,con,geom_col='geom')
    for index, row in df_roads.iterrows():
        (xs,ys) = row['geom'].coords.xy
        color='y'
        #road colour by type
        if row['highway']=="motorway":
            color = 'b'
        if row['highway']=="trunk":
            color = 'g'
        #if not color=='y':  #only plot major roads
        plot(xs, ys, color)
    print("Roads plotted")
plotRoads()

def plotBluetoothSites():    
    sql = "SELECT siteID, geom FROM BluetoothSite;"
    df_sites = gpd.GeoDataFrame.from_postgis(sql,con,geom_col='geom') #
    for index, row in df_sites.iterrows():
        (xs,ys) = row['geom'].coords.xy
        plot(xs, ys, 'bo')
    print ("BT sites plotted")  
plotBluetoothSites()
def plotFlows(con,cur):     
    dt_start  = datetime.datetime.strptime('2015-01-05_00:00:00' ,                                            "%Y-%m-%d_%H:%M:%S" )
    dt_end    = datetime.datetime.strptime('2016-12-10_00:00:00' ,                                            "%Y-%m-%d_%H:%M:%S" )
    sql = "SELECT ways.gid, SUM(ODroutecount.count), ways.the_geom FROM ways,    ODroutecount  WHERE ODroutecount.gid::int=ways.gid AND ODroutecount.timestamp>'%s'    AND ODroutecount.timestamp<'%s'  GROUP BY ways.gid;"%(dt_start,dt_end)
    print(sql) 
    df_links = gpd.GeoDataFrame.from_postgis(sql,con,geom_col='the_geom')
    for i in range(0,df_links.shape[0]): 
        link = df_links.iloc[i]
        lons = link['the_geom'].coords.xy[0] #coordinates in latlon
        lats = link['the_geom'].coords.xy[1]
        gid = int(link.gid) 
        xs=[];ys=[]
        n_segments = len(lons)
        for j in range(0, n_segments):
            (x,y) = pyproj.transform(wgs84, bng, lons[j], lats[j]) #project to BNG -- uses nonISO lonlat convention  #TODO faster to cache this! 
            xs.append(x)
            ys.append(y)
        color='r'
        lw = int(link['sum']/10000)
        plot(xs, ys, color, linewidth=lw)  
plotFlows(con,cur)
print ("Flows plotted")


# In[ ]:


import plotly.plotly as py
import plotly.graph_objs as go

import pandas as pd

mapbox_access_token = "pk.eyJ1Ijoic3JlZXNvbWFkYXMiLCJhIjoiY2poZXY5aDNoMDNjNDM3bnJhaXZneHF5YyJ9.5io2Q2NCWyJTE82MSK5PdA"


df = pd.read_csv("/headless/data/BTsites.csv")
print (df)
site_lat = df.Latitude
site_lon = df.Longitude
locations_name = df.Description

data = [
    go.Scattermapbox(
        lat=site_lat,
        lon=site_lon,
        mode='markers',
        marker=dict(
            size=17,
            color='rgb(255, 0, 0)',
            opacity=0.7
        ),
        text=locations_name,
        hoverinfo='text'
    ),
    go.Scattermapbox(
        lat=site_lat,
        lon=site_lon,
        mode='markers',
        marker=dict(
            size=8,
            color='rgb(242, 177, 172)',
            opacity=0.7
        ),
        hoverinfo='none'
    )]


layout = go.Layout(
    title='Bluetooth Sites',
    autosize=True,
    hovermode='closest',
    showlegend=False,
    mapbox=dict(
        accesstoken=mapbox_access_token,
        bearing=0,
        center=dict(
            lat=38,
            lon=-94
        ),
        pitch=0,
        zoom=3,
        style='light'
    ),
)

fig = dict(data=data, layout=layout)

py.iplot(fig, filename='Bluetooth Sites')



# coding: utf-8

# In[4]:


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

##This code has cleaned detection table with exceptions and it can find the OD flow-OD distance relation ##Task 1

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
#import random
#from random import randint


# In[5]:


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

importRoads()


# In[6]:


##Creating list of BT sites table
def importBluetoothSites():
    print("importing sites...")
    sql = "DROP TABLE IF EXISTS BluetoothSite;"
    cur.execute(sql)
    sql = "CREATE TABLE BluetoothSite ( id serial PRIMARY KEY, siteID text,    geom geometry);"
    cur.execute(sql)
    con.commit()    
    fn_sites = "/headless/data/dcc/web_bluetooth_sites.csv"
    df_sites = pd.read_csv(fn_sites, header=1)   #dataframe. header is which row to use for the field names.
    for i in range(0, df_sites.shape[0]):      #munging to extract the coordinates - the arrive in National Grid
        locationstr = str(df_sites.iloc[i]['Grid'])
        bng_east  = locationstr[0:6]
        bng_north = locationstr[6:12]
        sql = "INSERT INTO BluetoothSite (siteID, geom)         VALUES ('%s', 'POINT(%s %s)');"%(df_sites.iloc[i]['Site ID'],         bng_east, bng_north )
        cur.execute(sql)
    con.commit()        

importBluetoothSites()


# In[8]:


##Importing BTsite detection site CSVs and cleaning the repetitive counts, 
##with some exceptions of alternate detections


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
        prev_ts = "" #empty string for timestamp
        prev_mac = ""#empty string for mac
        for i in range(0, df_detections.shape[0]): 
            #here we use Python's DateTime library to store times properly
            datetime_text = df_detections.iloc[i]['Unnamed: 0']
            #proper Python datetime  
            dt = datetime.datetime.strptime(datetime_text, "%d/%m/%Y %H:%M:%S") 
            
            if prev_ts == "":
                prev_ts = dt
                prev_mac = df_detections.iloc[i]['Number Plate']
                #continue
            else:
                cur_ts = dt
                cur_mac = df_detections.iloc[i]['Number Plate']
                #print (cur_ts)
                diff = cur_ts - prev_ts #difference in timestamps
                #print (cur_ts, prev_ts, cur_mac, prev_mac, diff)
                if cur_mac == prev_mac and diff.seconds<60:
                    continue
                else:
                    prev_mac = df_detections.iloc[i]['Number Plate']
                    prev_ts = dt
                    
            sql = "INSERT INTO detection_clean (siteID, timestamp, mac) VALUES ('%s', '%s', '%s');"            %(siteID, dt, df_detections.iloc[i]['Number Plate'])
            cur.execute(sql)
            #print(siteID, dt, df_detections.iloc[i]['Number Plate'])
        
        table = psql.read_sql("SELECT * FROM detection_clean", con)
        # print(table)
        print ("Length of  table: ", len(table), df_detections.shape[0])
    con.commit()
    
    print("Cleaning done")
    
importDetections()

###############Take Only till Here#####


# In[9]:


#########Yet to be cleaned################

def createODRoute(con,cur):
    sql = "DROP TABLE IF EXISTS ODRoute;"
    cur.execute(sql)          
    sql="CREATE TABLE ODRoute (             emp_no SERIAL PRIMARY KEY,             ODrouteID text,             timestamp timestamp,             winlenseconds integer,             count integer, OriginSiteID text, MidSiteID text,             DestSiteID text);"
    cur.execute(sql)          
    con.commit() 
createODRouteCount(con,cur)
def createSensorDistances (con,cur):
    sql = "DROP TABLE IF EXISTS SensorDistances;"
    cur.execute(sql)          
    sql="CREATE TABLE SensorDistances (             emp_no SERIAL PRIMARY KEY,             ODRouteID text, gid_link text,             om_length integer, md_length integer,            total_length integer            );"
    cur.execute(sql)          
    con.commit()

def createODRouteCount(con, cur):
    sql = "DROP TABLE IF EXISTS ODroutecount;"
    cur.execute(sql)          
    sql="CREATE TABLE ODroutecount (             emp_no SERIAL PRIMARY KEY,             ODrouteID text,             timestamp timestamp,             winlenseconds integer,             count integer             );"
    cur.execute(sql)          
    con.commit()
createODRouteCount(con, cur)

def createODLinkCount(con, cur):
    sql = "DROP TABLE IF EXISTS ODlinkcount;"
    cur.execute(sql)          
    sql="CREATE TABLE ODlinkcount (             emp_no SERIAL PRIMARY KEY,             gid text,             timestamp timestamp,             winlenseconds integer,             count integer             );"
    cur.execute(sql)          
    con.commit()
createODLinkCount(con, cur)

def ODrouteCounts(con,cur):   #count number of matching Bluetooth detections between origins and destinations
    sql = "SELECT * FROM ODRoute;"
    df_odroute = pd.read_sql_query(sql,con)
    for i in range(0, df_odroute.shape[0]):    #each route
        oSiteID = df_odroute['originsiteid'][i]
        mSiteID = df_odroute['midsiteid'][i]
        dSiteID = df_odroute['destsiteid'][i]
        #MAC matching
        sql = "SELECT d.siteID AS dSiteID,  d.mac as dmac,         d.timestamp as dtimestamp  , m.siteID AS mSiteID,  m.mac as mmac,         m.timestamp as mtimestamp,  o.siteID AS oSiteID,          o.mac as omac, o.timestamp as otimestamp            FROM Detection AS d, Detection AS m, Detection AS o          WHERE d.timestamp>m.timestamp AND m.timestamp>o.timestamp        AND o.mac=d.mac AND o.siteID='%s' AND        AND m.siteID = '%s' AND d.siteID='%s'"%(oSiteID, mSiteID, dSiteID)
        print(sql)
        df_matches = pd.read_sql_query(sql,con)
        count = df_matches.shape[0]  #count number of bluetooth matches
        #these two variables allow us to compute flows for differnt time windows. Here we just take one whole day.
        winlenseconds = 99999999.9
        timestamp = "2015-02-14 09:00:00"
        sql = "INSERT INTO ODRouteCount (ODrouteID, timestamp,        winlenseconds, count) VALUES ('%s', '%s', %f, %i)"        %(df_odroute['ODrouteid'].iloc[i], timestamp, winlenseconds, count)
        cur.execute(sql)
        print (sql)
        con.commit()
ODrouteCounts(con,cur)

def ODlinkCounts(con, cur):
    sql = "SELECT * FROM ODRouteCount;"
    df_rc = pd.read_sql_query(sql,con)
    for i in range(0, df_rc.shape[0]):    #each routecount
        row  = df_rc.iloc[i]
        count         = row['count']
        winlenseconds = row['winlenseconds']
        timestamp     = row['timestamp']
        if count>0:
            sql = "SELECT * FROM ODRouteLink WHERE ODrouteid='%s'"%row['ODrouteid']
            df_rl = pd.read_sql_query(sql,con)
            for j in range(0, df_rl.shape[0]): #add counts for each link
                link = df_rl.iloc[j]                   #route component
                gid_link = link['link_gid']
                sql = "INSERT INTO ODlinkcount (gid, timestamp, winlenseconds,                count) VALUES ('%s', '%s', %d, %d);"%(gid_link,                 timestamp, winlenseconds, count)
                print(sql)
                cur.execute(sql)
            con.commit()
ODlinkCounts(con, cur)
'''def ODLengths(con,cur):
    
    
def ODFlowsLengths(con,cur):
    sql = "SELECT * FROM O'''

def plotFlows(con,cur):     
    dt_start  = datetime.datetime.strptime('2015-01-05_00:00:00' ,                                            "%Y-%m-%d_%H:%M:%S" )
    dt_end    = datetime.datetime.strptime('2016-12-10_00:00:00' ,                                            "%Y-%m-%d_%H:%M:%S" )
    sql = "SELECT ways.gid, SUM(ODlinkcount.count), ways.the_geom FROM ways,    ODlinkcount  WHERE ODlinkcount.gid::int=ways.gid AND ODlinkcount.timestamp>'%s'    AND ODlinkcount.timestamp<'%s'  GROUP BY ways.gid;"%(dt_start,dt_end)
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
'''def plotGraphs(con,cur):     
    dt_start  = datetime.datetime.strptime('2015-01-05_00:00:00' , \
                                           "%Y-%m-%d_%H:%M:%S" )
    dt_end    = datetime.datetime.strptime('2016-12-10_00:00:00' , \
                                           "%Y-%m-%d_%H:%M:%S" )
    sql = "SELECT ways.gid, SUM(linkcount.count), ways.the_geom FROM ways,\
    linkcount  WHERE linkcount.gid::int=ways.gid AND linkcount.timestamp>'%s'\
    AND linkcount.timestamp<'%s'  GROUP BY ways.gid;"%(dt_start,dt_end)
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
        plot(xs, ys, color, linewidth=lw)'''  
#import psycopg2
#import pandas as pd
#import geopandas as gpd
#import pyproj
#import os,re,datetime
#from matplotlib.pyplot import *
#con = psycopg2.connect(database='mydatabasename', user='root')
#cur = con.cursor()
#wgs84  = pyproj.Proj(init='epsg:4326')  #WGS84
#bng    = pyproj.Proj(init='epsg:27700') 
#import pandas.io.sql as psql


# In[ ]:


def plotRoads():   
    print("plotting roads...")
    sql = "SELECT * FROM Road;"
    df_roads = gpd.GeoDataFrame.from_postgis(sql,con,geom_col='geom') #
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
plotRoads()

def plotBluetoothSites():    
    sql = "SELECT siteID, geom FROM BluetoothSite;"
    df_sites = gpd.GeoDataFrame.from_postgis(sql,con,geom_col='geom') #
    for index, row in df_sites.iterrows():
        (xs,ys) = row['geom'].coords.xy
        plot(xs, ys, 'bo')    
plotBluetoothSites()


# In[ ]:





# In[1]:


def makeODRoutes(con,cur):
    print("define OD measureable route from each sensor to each other")
    #sql = "SELECT * FROM MeasureableRoute;"
    #df_ODRoutes = pd.read_sql_query(sql,con)
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
    #print(df.shape)
    #print(df.shape[0])
    #print("asdf")
# or enter for i in range(0,df.shape[0])    
    for i in range(0,df.shape[0]):    #each route
        routeID = df['MeasureableRouteID'][i]
        oSiteID = df['OriginSiteID'][i]
        mSiteID = df['MidSiteID'][i]
        dSiteID = df['DestSiteID'][i]
        #MAC matching
        sql = "SELECT d.siteID AS dSiteID,  d.mac as dmac, d.timestamp as dtimestamp  ,        m.siteID AS mSiteID,  m.mac as mmac, m.timestamp as mtimestamp ,        o.siteID AS oSiteID,  o.mac as omac, o.timestamp as otimestamp          FROM Detection AS d,Detection AS m, Detection AS o          WHERE d.timestamp>m.timestamp AND m.timestamp>o.timestamp        AND o.mac=m.mac AND m.mac=d.mac  AND o.siteID='%s'AND m.siteID='%s'        AND d.siteID='%s'"%(oSiteID, mSiteID, dSiteID)
        #print(sql)
        df_matches = pd.read_sql_query(sql,con)
        count = df_matches.shape[0]  #count number of bluetooth matches
        #these two variables allow us to compute flows for differnt time windows. Here we just take one whole day.
        winlenseconds = 99999999.9
        timestamp = "2015-02-14 09:00:00"
        sql = "INSERT INTO ODRoute (ODrouteID, timestamp, winlenseconds, count,        OriginSiteID, MidSiteID, DestSiteID)        VALUES ('%s', '%s', %f, %i,'%s','%s','%s')"%(routeID,timestamp,        winlenseconds, count, oSiteID, mSiteID, dSiteID)
        cur.execute(sql)
        
    con.commit()  

makeODRoutes(con,cur)


# In[ ]:


def SensorDistances (con,cur):
    print ("computing lengths ..")
    sql = "SELECT ODRouteID from ODRoute"
    sql = "SELECT ODRouteID, ST_X(orig.geom) AS ox, ST_Y(orig.geom)             AS oy, ST_X(mid.geom) AS mx, ST_Y(mid.geom) AS my, ST_X(dest.geom) AS dx,            ST_Y(dest.geom) AS dy from ODRoute,             BluetoothSite AS orig, BluetoothSite AS mid, BluetoothSite AS dest             WHERE OriginSiteID=orig.siteID AND MidSiteID=mid.siteID AND             DestSiteID=dest.siteID;"
    #Read bulk of db in psql and enter into DF
    df_dist = pd.read_sql_query(sql,con)
    #print (df_dist)
    N= df_dist.shape[:]   
    #N = len(df_dist)
    print (N)
# for i in range(0, df_dist) or  i in range (10,7) - Error fixed
    for i in range (0, df_dist.shape[0]):      
        OMDrouteID = df_dist['odrouteid'][i]
   # for i in range(df_dist)
        o_easting  = df_dist['ox'][i]#link-broken table is stored as latlon so convert on the fly
        #print (o_easting)
        #print(df_dist['ox'][i])
        #print(df_dist['ox'])
        o_northing = df_dist['oy'][i]
        m_easting  = df_dist['mx'][i]
        m_northing = df_dist['my'][i]
        d_easting  = df_dist['dx'][i]
        d_northing = df_dist['dy'][i]
        #print(type(o_easting))
        (o_lon,o_lat) = pyproj.transform(bng, wgs84, o_easting, o_northing) #project uses nonISO lonlat convention
        (m_lon,m_lat) = pyproj.transform(bng, wgs84, m_easting, m_northing)
        (d_lon,d_lat) = pyproj.transform(bng, wgs84, d_easting, d_northing) #project uses nonISO lonlat convention 

    #Origin gid
        sql = "SELECT id,ST_Distance(ways_vertices_pgr.the_geom,        ST_SetSRID(ST_MakePoint(%f, %f),4326)) FROM ways_vertices_pgr         ORDER BY st_distance ASC LIMIT 1;"%(o_lon,o_lat)    #4326=SRID code for WGS84
        df_on = pd.read_sql_query(sql,con)
        o_vertex_gid = df_on['id'][0] #get the vertexID of its source
    
    #Mid gid
        sql = "SELECT id,ST_Distance(ways_vertices_pgr.the_geom,        ST_SetSRID(ST_MakePoint(%f, %f),4326)) FROM ways_vertices_pgr         ORDER BY st_distance ASC LIMIT 1;"%(m_lon,m_lat)    #4326=SRID code for WGS84
        df_mn = pd.read_sql_query(sql,con)
        m_vertex_gid = df_mn['id'][0] #get the vertexID of its source
    
    #Dest gid
        sql = "SELECT id,ST_Distance(ways_vertices_pgr.the_geom,        ST_SetSRID(ST_MakePoint(%f, %f),4326)) FROM ways_vertices_pgr         ORDER BY st_distance ASC LIMIT 1;"%(d_lon,d_lat)    #4326=SRID code for WGS84
        df_dn = pd.read_sql_query(sql,con)
        d_vertex_gid = df_dn['id'][0] #get the vertexID of its source
        
        #pgr_dijkstra(o_vertex_gid, m_vertex_gid, d_vertex_gid)
        sql = "SELECT * FROM pgr_dijkstra('SELECT gid AS id, source, target,         length_m AS cost FROM ways', %d,%d, directed := false),         ways  WHERE ways.gid=pgr_dijkstra.edge;"%(o_vertex_gid, m_vertex_gid)
        
        sql = "SELECT * FROM pgr_dijkstra('SELECT gid AS id, source, target,         length_m AS cost FROM ways', %d,%d, directed := false),         ways  WHERE ways.gid=pgr_dijkstra.edge;"%(m_vertex_gid, d_vertex_gid)        

        df_route = gpd.GeoDataFrame.from_postgis(sql, con, geom_col='the_geom')
        M = df_route.shape[:]
        print (M)
        #store which links belong to this route
        for i in range(0,df_route.shape[0]):
            rc = df_route.iloc[i]                   #route component
            #sql = "INSERT INTO SensorDistances(ODrouteID, gid_link, \
            #om_length, md_length, total_length) \
            #VALUES (%i,%i,%i,%i);"%(OMDrouteID, rc['gid'], \
            #rc['length_m'],rc['length_m'],rc['length_m'])
            sql = "INSERT INTO SensorDistances(ODrouteID,gid_link) values(%d, '%s');"%(route['odrouteid'],rc['gid'])
            print(sql)
            cur.execute(sql)
    con.commit()
    


SensorDistances(con,cur)


# In[ ]:



ODrouteCounts(con,cur)
ODlinkCounts(con,cur)
plotRoads()
plotFlows(con,cur)  
plotBluetoothSites()


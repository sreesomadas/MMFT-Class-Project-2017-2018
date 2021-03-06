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
import psycopg2
import pandas as pd
import geopandas as gpd
import pyproj
import os,re,datetime
from matplotlib.pyplot import *
import mplleaflet # pip install mplleaflet
import folium # pip install folium
import shapely
from shapely.geometry import Point
con = psycopg2.connect(database='mydatabasename', user='root')
cur = con.cursor()
wgs84  = pyproj.Proj(init='epsg:4326')  #WGS84
bng    = pyproj.Proj(init='epsg:27700') #british national grid
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
        sql="INSERT INTO Road VALUES ('%s', '%s', '%s');"%(row.name, row.geometry, row.highway )
        cur.execute(sql)
    con.commit()  
importRoads()

# Importing BT site locations
def importBluetoothSites():
    print("importing sites...")
    sql = "DROP TABLE IF EXISTS BluetoothSite;"
    cur.execute(sql)
    sql = "CREATE TABLE BluetoothSite ( id serial PRIMARY KEY, siteID text, geom geometry);"
    cur.execute(sql)
    con.commit()    
    fn_sites = "/headless/data/dcc/web_bluetooth_sites.csv"
    df_sites = pd.read_csv(fn_sites, header=1)   #dataframe. header is which row to use for the field names.
    for i in range(0, df_sites.shape[0]):      #munging to extract the coordinates - the arrive in National Grid
        locationstr = str(df_sites.iloc[i]['Grid'])
        bng_east  = locationstr[0:6]
        bng_north = locationstr[6:12]
        sql = "INSERT INTO BluetoothSite (siteID, geom) VALUES ('%s', 'POINT(%s %s)');"%(df_sites.iloc[i]['Site ID'], bng_east, bng_north )
        cur.execute(sql)
    con.commit()    
importBluetoothSites()

# Importing BT site detection csvs
def importDetections():
    print("importing detections...")
    sql = "DROP TABLE IF EXISTS Detection;"
    cur.execute(sql)    
    sql = "CREATE TABLE Detection ( id serial, siteID text, mac text, timestamp timestamp );"
    cur.execute(sql)    
    dir_detections = "/headless/data/dcc/bluetooth/"
    for fn in sorted(os.listdir(dir_detections)):  #import ALL sensor files
        print("processing file: "+fn)
        m = re.match("vdFeb14_(.+).csv", fn)  #use regex to extract the sensor ID
        if m is None:  #if there was no regex match
            continue   #ignore any non detection files
        siteID = m.groups()[0]
        fn_detections = dir_detections+fn
        df_detections = pd.read_csv(fn_detections, header=0)   #dataframe. header is which row to use for the field names.
        for i in range(0, df_detections.shape[0]):   #here we use Python's DateTime library to store times properly
            datetime_text = df_detections.iloc[i]['Unnamed: 0']
            dt = datetime.datetime.strptime(datetime_text ,  "%d/%m/%Y %H:%M:%S" ) #proper Python datetime   
            sql = "INSERT INTO Detection (siteID, timestamp, mac) VALUES ('%s', '%s', '%s');"%(siteID, dt, df_detections.iloc[i]['Number Plate'])
            cur.execute(sql)
    con.commit() 
importDetections()

# Cleaning previously imported detections
def cleanedDetections(con,cur):
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
cleanedDetections(con,cur)

def createRoute(con, cur):
    sql = "DROP TABLE IF EXISTS Route;"
    cur.execute(sql)          
    sql="CREATE TABLE Route ( \
            emp_no SERIAL PRIMARY KEY, \
            routeID text, \
            originSiteID text, \
            destSiteID text \
            );"
    cur.execute(sql)          
    con.commit() 
createRoute(con,cur)

def createRouteLink(con, cur):
    sql = "DROP TABLE IF EXISTS routelink;"
    cur.execute(sql)          
    sql="CREATE TABLE routelink ( \
            emp_no SERIAL PRIMARY KEY, \
            routeID text, \
            link_gid bigint \
            );"
    cur.execute(sql)          
    con.commit()
createRouteLink(con,cur)

def createRouteCount(con, cur):
    sql = "DROP TABLE IF EXISTS routecount;"
    cur.execute(sql)          
    sql="CREATE TABLE routecount ( \
            emp_no SERIAL PRIMARY KEY, \
            routeID text, \
            timestamp timestamp, \
            winlenseconds integer, \
            count integer \
            );"
    cur.execute(sql)          
    con.commit()
createRouteCount(con,cur)

def createLinkCount(con, cur):
    sql = "DROP TABLE IF EXISTS linkcount;"
    cur.execute(sql)          
    sql="CREATE TABLE linkcount ( \
            emp_no SERIAL PRIMARY KEY, \
            gid text, \
            timestamp timestamp, \
            winlenseconds integer, \
            count integer \
            );"
    cur.execute(sql)          
    con.commit()
createLinkCount(con,cur)

def makeRoutes(con,cur):
    print("define a route from each sensor to each other")
    sql = "SELECT * FROM BluetoothSite;"
    df_sites = gpd.GeoDataFrame.from_postgis(sql,con,geom_col='geom')
    N = df_sites.shape[0]
    for i_orig in range(0,N):
        for i_dest in range(0,N):
            if (i_orig==i_dest):  #no route from self to self
                continue
            originSiteID = df_sites.iloc[i_orig]['siteid']
            destSiteID = df_sites.iloc[i_dest]['siteid']
            routeID = originSiteID+">"+destSiteID
            sql = "INSERT INTO Route (routeID, originSiteID, destSiteID ) VALUES ('%s', '%s', '%s')"%(routeID, originSiteID, destSiteID)
            print(sql)
            cur.execute(sql)    
    con.commit() 
makeRoutes(con,cur)

def routeLinks(con,cur): 
    print("computing links on routes")
    sql = "SELECT routeID, ST_X(orig.geom) AS ox, ST_Y(orig.geom) AS oy, \
    ST_X(dest.geom) AS dx, ST_Y(dest.geom) AS dy  FROM Route, BluetoothSite \
    AS orig, BluetoothSite AS dest WHERE originSiteID=orig.siteID AND \
    destSiteID=dest.siteID;"  #join useful data
    df_od = pd.read_sql_query(sql,con)
    N = df_od.shape[0]
    for i in range(0, N):
        routeid=df_od['routeid'][i]
        o_easting  = df_od['ox'].iloc[i] #link-broken table is stored as latlon so convert on the fly
        o_northing = df_od['oy'].iloc[i]
        d_easting  = df_od['dx'].iloc[i]
        d_northing = df_od['dy'].iloc[i]
        (o_lon,o_lat) = pyproj.transform(bng, wgs84, o_easting, o_northing) #project uses nonISO lonlat convention 
        (d_lon,d_lat) = pyproj.transform(bng, wgs84, d_easting, d_northing) #project uses nonISO lonlat convention 
        #find closest VERTICES to sensors  
        sql = "SELECT id,ST_Distance(ways_vertices_pgr.the_geom,ST_SetSRID(ST_MakePoint(%f, %f),4326)) FROM ways_vertices_pgr ORDER BY st_distance ASC LIMIT 1;"%(o_lon,o_lat)    #4326=SRID code for WGS84
        df_on = pd.read_sql_query(sql,con)
        o_vertex_gid = df_on['id'][0] #get the vertexID of its source
        sql = "SELECT id,ST_Distance(ways_vertices_pgr.the_geom,ST_SetSRID(ST_MakePoint(%f, %f),4326)) FROM ways_vertices_pgr ORDER BY st_distance ASC LIMIT 1;"%(d_lon,d_lat) 
        df_dn = pd.read_sql_query(sql,con)  #dest nearest link
        d_vertex_gid = df_dn['id'][0]   #get vertexID of way target 
        #find (physically) shortest route between them. NB. pgr_dijkstra takes VERTEX ID's not WAY IDs as start and ends.
        sql = "SELECT * FROM pgr_dijkstra('SELECT gid AS id, source, target, length AS cost FROM ways', %d,%d, directed := false), ways  WHERE ways.gid=pgr_dijkstra.edge;"%(o_vertex_gid, d_vertex_gid)
        df_route = gpd.GeoDataFrame.from_postgis(sql,con,geom_col='the_geom')
        #store which links belong to this route
        for i in range(0,df_route.shape[0]):
            rc = df_route.iloc[i]                   #route component
            sql = "INSERT INTO routelink (routeid, link_gid) VALUES ('%s', %s);"%(routeid, rc['gid'])
            print(sql)
            cur.execute(sql)
        con.commit()
routeLinks(con,cur) 

def routeCounts(con,cur):   #count number of matching Bluetooth detections between origins and destinations
    sql = "SELECT * FROM Route;"
    df_route = pd.read_sql_query(sql,con)
    for i in range(0, df_route.shape[0]):    #each route
        oSiteID = df_route['originsiteid'][i]
        dSiteID = df_route['destsiteid'][i]
        #MAC matching
        sql = "SELECT d.siteID AS dSiteID, d.mac as dmac, d.timestamp \
        as dtimestamp, o.siteID AS oSiteID, o.mac as omac, o.timestamp \
        as otimestamp FROM CleanedDetection AS d, CleanedDetection AS o WHERE \
        d.timestamp>o.timestamp AND o.mac=d.mac AND o.siteID='%s' \
        AND d.siteID='%s'"%(oSiteID, dSiteID)
        print(sql)
        df_matches = pd.read_sql_query(sql,con)
        count = df_matches.shape[0]  #count number of bluetooth matches
        #these two variables allow us to compute flows for differnt time windows. Here we just take one whole day.
        winlenseconds = 99999999.9
        timestamp = "2015-02-14 09:00:00"
        sql = "INSERT INTO routecount (routeID, timestamp, \
        winlenseconds, count) VALUES ('%s', '%s', %f, %i)"%(df_route['routeid'].iloc[i], timestamp, winlenseconds, count)
        cur.execute(sql)
        con.commit()
routeCounts(con,cur)

def linkCounts(con, cur):
    sql = "SELECT * FROM RouteCount;"
    df_rc = pd.read_sql_query(sql,con)
    for i in range(0, df_rc.shape[0]):    #each routecount
        row  = df_rc.iloc[i]
        count         = row['count']
        winlenseconds = row['winlenseconds']
        timestamp     = row['timestamp']
        if count>0:
            sql = "SELECT * FROM RouteLink WHERE routeid='%s'"%row['routeid']
            df_rl = pd.read_sql_query(sql,con)
            for j in range(0, df_rl.shape[0]): #add counts for each link
                link = df_rl.iloc[j]                   #route component
                gid_link = link['link_gid']
                sql = "INSERT INTO linkcount (gid, timestamp, winlenseconds, count) VALUES ('%s', '%s', %d, %d);"%(gid_link, timestamp, winlenseconds, count)
                print(sql)
                cur.execute(sql)
            con.commit()
linkCounts(con,cur)

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
    sql = "SELECT siteID, geom FROM BluetoothSite WHERE NOT \
    (siteid='MAC000010122' OR siteid='MAC000010128');"
    df_sites = gpd.GeoDataFrame.from_postgis(sql,con,geom_col='geom') #
    for index, row in df_sites.iterrows():
        (xs,ys) = row['geom'].coords.xy
        plot(xs, ys, 'bo')
plotBluetoothSites()

def plotFlows(con,cur):     
    dt_start  = datetime.datetime.strptime('2015-01-05_00:00:00' , "%Y-%m-%d_%H:%M:%S" )
    dt_end    = datetime.datetime.strptime('2016-12-10_00:00:00' , "%Y-%m-%d_%H:%M:%S" )
    sql = "SELECT ways.gid, SUM(linkcount.count), ways.the_geom FROM ways, linkcount  WHERE linkcount.gid::int=ways.gid AND linkcount.timestamp>'%s' AND linkcount.timestamp<'%s'  GROUP BY ways.gid;"%(dt_start,dt_end)
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
        lw = int(link['sum']/5000)
        plot(xs, ys, color, linewidth=lw)  
plotFlows(con,cur)  


############################## End of last year's code#########################


def createODRoute(con,cur):
    sql = "DROP TABLE IF EXISTS ODRoute;"
    cur.execute(sql)          
    sql="CREATE TABLE ODRoute ( \
            emp_no SERIAL PRIMARY KEY, \
            ODrouteID text, \
            timestamp timestamp, \
            winlenseconds integer, \
            count integer, OriginSiteID text, MidSiteID text, DestSIteID text \
            );"
    cur.execute(sql)          
    con.commit() 
createODRoute(con,cur)

def makeODRoutes(con,cur):
    print("define OD measureable route from each sensor to each other")
    Routes = {"MeasureableRouteID":["MAC000010119>MAC000010104>MAC000010130",\
                                    "MAC000010119>MAC000010109>MAC000010130",\
                                    "MAC000010102>MAC000010119>MAC000010104",\
                                    "MAC000010102>MAC000010118>MAC000010104",\
                                    "MAC000010121>MAC000010113>MAC000010124",\
                                    "MAC000010121>MAC000010112>MAC000010124",\
                                    "MAC000010101>MAC000010104>MAC000010119",\
                                    "MAC000010101>MAC000010118>MAC000010119",\
                                    "MAC000010123>MAC000010114>MAC000010120",\
                                    "MAC000010123>MAC000010112>MAC000010120"],\
               "OriginSiteID":["MAC000010119","MAC000010119","MAC000010102",\
                               "MAC000010102","MAC000010121","MAC000010121",\
                               "MAC000010101","MAC000010101","MAC000010123",\
                               "MAC000010123"],
                "MidSiteID": ["MAC000010104","MAC000010109","MAC000010119",\
                              "MAC000010118","MAC000010113","MAC000010112",\
                              "MAC000010104","MAC000010118","MAC000010114",\
                              "MAC000010112"],
                "DestSiteID":["MAC000010130","MAC000010130","MAC000010104",\
                              "MAC000010104","MAC000010124","MAC000010124",\
                              "MAC000010119","MAC000010119","MAC000010120",\
                              "MAC000010120"]}
    df=pd.DataFrame.from_dict(Routes)
    for i in range(0, df.shape[0]):    #each route
        routeID = df['MeasureableRouteID'][i]
        oSiteID = df['OriginSiteID'][i]
        mSiteID = df['MidSiteID'][i]
        dSiteID = df['DestSiteID'][i]
        #MAC matching
        sql = "SELECT d.siteID, d.mac, d.timestamp, m.siteID, m.mac, m.timestamp, \
        o.siteID, o.mac, o.timestamp FROM CleanedDetection AS d, \
        CleanedDetection AS m, CleanedDetection AS o WHERE \
        d.timestamp>m.timestamp AND m.timestamp>o.timestamp \
        AND d.timestamp-m.timestamp<interval '30 minutes' AND \
        m.timestamp-o.timestamp<interval '30 minutes' AND o.mac=m.mac \
        AND m.mac=d.mac AND o.siteID='%s'AND m.siteID='%s' \
        AND d.siteID='%s'"%(oSiteID, mSiteID, dSiteID)
        print(sql)
        df_matches = pd.read_sql_query(sql,con)
        count = df_matches.shape[0]  #count number of bluetooth matches
        #these two variables allow us to compute flows for differnt time windows. Here we just take one whole day.
        winlenseconds = 99999999.9
        timestamp = "2015-02-14 09:00:00"
        sql = "INSERT INTO ODRoute (ODrouteID, timestamp, winlenseconds, count, \
        OriginSiteID, MidSiteID, DestSiteID) VALUES ('%s', '%s', %f, %i, '%s', '%s', '%s')"\
        %(routeID,timestamp, winlenseconds, count, oSiteID, mSiteID, dSiteID)
        cur.execute(sql)
    con.commit() 
makeODRoutes(con,cur)

def createODRouteLinkOrigMid(con, cur):
    sql = "DROP TABLE IF EXISTS ODroutelinkOrigMid;"
    cur.execute(sql)          
    sql="CREATE TABLE ODroutelinkOrigMid ( \
            emp_no SERIAL PRIMARY KEY, \
            odrouteid text, start text, finish text,\
            link_gid bigint, length_m integer \
            );"
    cur.execute(sql)          
    con.commit()
createODRouteLinkOrigMid(con,cur)

def createODRouteLinkMidDest(con, cur):
    sql = "DROP TABLE IF EXISTS ODroutelinkMidDest;"
    cur.execute(sql)          
    sql="CREATE TABLE ODroutelinkMidDest ( \
            emp_no SERIAL PRIMARY KEY, \
            odrouteid text, start text, finish text,\
            link_gid bigint, length_m integer \
            );"
    cur.execute(sql)          
    con.commit()
createODRouteLinkMidDest(con,cur)

def ODRouteLinkOrigMid(con,cur): 
    print("computing links on ODroutes")
    sql = "SELECT ODrouteID, OriginSiteID, MidSiteID, DestSiteID, \
    ST_X(orig.geom) AS ox, ST_Y(orig.geom) AS oy, ST_X(mid.geom) \
    AS mx, ST_Y(mid.geom) AS my, ST_X(dest.geom) AS dx, \
    ST_Y(dest.geom) AS dy FROM ODRoute, BluetoothSite AS orig, \
    BluetoothSite AS mid, BluetoothSite AS dest WHERE \
    originSiteID=orig.siteID AND midSiteID=mid.siteID AND \
    destSiteID=dest.siteID;"  #join useful data
    df_od = pd.read_sql_query(sql,con)
    N = df_od.shape[0]
    for i in range(0, N):
        omdrouteid = df_od['odrouteid'][i]
        originsiteid = df_od['originsiteid'][i]
        midsiteid = df_od['midsiteid'][i]
        #destsiteid = df_od['destsiteid'][i]
        o_easting  = df_od['ox'].iloc[i]
        #print (o_easting)#link-broken table is stored as latlon so convert on the fly
        o_northing = df_od['oy'].iloc[i]
        m_easting  = df_od['mx'].iloc[i] #link-broken table is stored as latlon so convert on the fly
        m_northing = df_od['my'].iloc[i]
        #d_easting  = df_od['dx'].iloc[i]
        #d_northing = df_od['dy'].iloc[i]
        (o_lon,o_lat) = pyproj.transform(bng, wgs84, o_easting, o_northing)
        (m_lon,m_lat) = pyproj.transform(bng, wgs84, m_easting, m_northing) #project uses nonISO lonlat convention 
        #(d_lon,d_lat) = pyproj.transform(bng, wgs84, d_easting, d_northing) #project uses nonISO lonlat convention 
        #find closest VERTICES to sensors  
        # Origin vertices
        sql = "SELECT id,ST_Distance(ways_vertices_pgr.the_geom,\
        ST_SetSRID(ST_MakePoint(%f, %f),4326)) FROM ways_vertices_pgr \
        ORDER BY st_distance ASC LIMIT 1;"%(o_lon,o_lat)    #4326=SRID code for WGS84
        df_on = pd.read_sql_query(sql,con)
        o_vertex_gid = df_on['id'][0] #get the vertexID of its source
        # Midpoint vertices
        sql = "SELECT id,ST_Distance(ways_vertices_pgr.the_geom,\
        ST_SetSRID(ST_MakePoint(%f, %f),4326)) FROM ways_vertices_pgr \
        ORDER BY st_distance ASC LIMIT 1;"%(m_lon,m_lat)    #4326=SRID code for WGS84
        df_mn = pd.read_sql_query(sql,con)
        m_vertex_gid = df_mn['id'][0] #get the vertexID of its source
        #find (physically) shortest route between Origin-Midpoint. NB. pgr_dijkstra takes VERTEX ID's not WAY IDs as start and ends.
        sql = "SELECT * FROM pgr_dijkstra('SELECT gid AS id, source, \
        target, length AS cost, length_m FROM ways', %d,%d, directed := false), \
        ways  WHERE ways.gid=pgr_dijkstra.edge;"\
        %(o_vertex_gid, m_vertex_gid)
        df_route = gpd.GeoDataFrame.from_postgis(sql,con,geom_col='the_geom')
        #store which links belong to this route
        for i in range(0,df_route.shape[0]):
            rc = df_route.iloc[i]                   #route component
            sql = "INSERT INTO ODroutelinkOrigMid (odrouteid, start, \
            finish, link_gid, length_m) VALUES ('%s', '%s','%s', %s, %s);"%\
            (omdrouteid, originsiteid, midsiteid, rc['gid'], rc['length_m'])
            print(sql)
            cur.execute(sql)
        con.commit() 
ODRouteLinkOrigMid(con,cur)

def ODRouteLinkMidDest(con,cur): 
    print("computing links on ODroutes")
    sql = "SELECT ODrouteID, OriginSiteID, MidSiteID, DestSiteID, \
    ST_X(orig.geom) AS ox, ST_Y(orig.geom) AS oy, ST_X(mid.geom) \
    AS mx, ST_Y(mid.geom) AS my, ST_X(dest.geom) AS dx, \
    ST_Y(dest.geom) AS dy FROM ODRoute, BluetoothSite AS orig, \
    BluetoothSite AS mid, BluetoothSite AS dest WHERE \
    originSiteID=orig.siteID AND midSiteID=mid.siteID AND \
    destSiteID=dest.siteID;"  #join useful data
    df_od = pd.read_sql_query(sql,con)
    N = df_od.shape[0]
    for i in range(0, N):
        omdrouteid = df_od['odrouteid'][i]
        #originsiteid = df_od['originsiteid'][i]
        midsiteid = df_od['midsiteid'][i]
        destsiteid = df_od['destsiteid'][i]
        #o_easting  = df_od['ox'].iloc[i] #link-broken table is stored as latlon so convert on the fly
        #o_northing = df_od['oy'].iloc[i]
        m_easting  = df_od['mx'].iloc[i] #link-broken table is stored as latlon so convert on the fly
        m_northing = df_od['my'].iloc[i]
        d_easting  = df_od['dx'].iloc[i]
        d_northing = df_od['dy'].iloc[i]
        #(o_lon,o_lat) = pyproj.transform(bng, wgs84, o_easting, o_northing)
        (m_lon,m_lat) = pyproj.transform(bng, wgs84, m_easting, m_northing) #project uses nonISO lonlat convention 
        (d_lon,d_lat) = pyproj.transform(bng, wgs84, d_easting, d_northing) #project uses nonISO lonlat convention 
        #find closest VERTICES to sensors  
        # Midpoint vertices
        sql = "SELECT id,ST_Distance(ways_vertices_pgr.the_geom,\
        ST_SetSRID(ST_MakePoint(%f, %f),4326)) FROM ways_vertices_pgr \
        ORDER BY st_distance ASC LIMIT 1;"%(m_lon,m_lat)    #4326=SRID code for WGS84
        df_mn = pd.read_sql_query(sql,con)
        m_vertex_gid = df_mn['id'][0] #get the vertexID of its source
        # Destination vertices
        sql = "SELECT id,ST_Distance(ways_vertices_pgr.the_geom,\
        ST_SetSRID(ST_MakePoint(%f, %f),4326)) FROM ways_vertices_pgr \
        ORDER BY st_distance ASC LIMIT 1;"%(d_lon,d_lat) 
        df_dn = pd.read_sql_query(sql,con)  #dest nearest link
        d_vertex_gid = df_dn['id'][0]   #get vertexID of way target """
        #find (physically) shortest route between Midpoint-Destination. NB. pgr_dijkstra takes VERTEX ID's not WAY IDs as start and ends.
        sql = "SELECT * FROM pgr_dijkstra('SELECT gid AS id, source, target, length AS cost \
        FROM ways', %d,%d, directed := false), ways  WHERE ways.gid=pgr_dijkstra.edge;"\
        %(m_vertex_gid, d_vertex_gid)
        df_route = gpd.GeoDataFrame.from_postgis(sql,con,geom_col='the_geom')
        for i in range(0,df_route.shape[0]):
            rc1 = df_route.iloc[i]                   #route component
            sql = "INSERT INTO ODroutelinkMidDest (odrouteid, start, \
            finish, link_gid, length_m) VALUES ('%s', '%s','%s', %s, %s);"%\
            (omdrouteid, midsiteid, destsiteid, rc1['gid'], rc1['length_m'])
            print(sql)
            cur.execute(sql)
        con.commit() 
ODRouteLinkMidDest(con,cur)

def ODRouteLinkOrigMidAgg(con,cur):
    sql="DROP TABLE IF EXISTS ODRouteLinkOrigMidAgg;"
    cur.execute(sql)
    sql="CREATE TABLE ODRouteLinkOrigMidAgg (emp_no serial, odrouteid text, \
    start text, finish text, TotalLength_m int);"
    cur.execute(sql)    
    sql="INSERT INTO ODRouteLinkOrigMidAgg (odrouteid, start, finish, \
    TotalLength_m) SELECT odrouteid, start, finish, SUM(Length_m) \
    FROM ODRouteLinkOrigMid GROUP BY odrouteid, start, finish ORDER BY \
    odrouteid;"
    cur.execute(sql) 
    con.commit()
ODRouteLinkOrigMidAgg(con,cur)

def ODRouteLinkMidDestAgg(con,cur):
    sql="DROP TABLE IF EXISTS ODRouteLinkMidDestAgg;"
    cur.execute(sql)
    sql="CREATE TABLE ODRouteLinkMidDestAgg (emp_no1 serial, odrouteid text, \
    start text, finish text, TotalLength_m1 int);"
    cur.execute(sql)    
    sql="INSERT INTO ODRouteLinkMidDestAgg (odrouteid, start, finish, \
    TotalLength_m1) SELECT odrouteid, start, finish, SUM(Length_m) FROM \
    ODRouteLinkMidDest GROUP BY odrouteid, start, finish ORDER BY odrouteid;"
    cur.execute(sql) 
    con.commit()
ODRouteLinkMidDestAgg(con,cur)  
 
def ODRouteDist(con,cur):
    sql="DROP TABLE IF EXISTS ODRouteDist;"
    cur.execute(sql)
    sql="CREATE TABLE ODRouteDist AS SELECT odrouteid, sum(TotalLength_m)\
    distance FROM (SELECT odrouteid, totallength_m from odroutelinkorigmidagg\
    UNION ALL SELECT odrouteid, totallength_m1 from odroutelinkmiddestagg) \
    t GROUP BY odrouteid ORDER BY odrouteid;"
    cur.execute(sql)
    con.commit()
ODRouteDist(con,cur)

def ODRouteCountDist(con,cur):
    sql="DROP TABLE IF EXISTS ODRouteCountDist;"
    cur.execute(sql)
    sql="CREATE TABLE ODRouteCountDist (emp_no serial, odrouteid text, \
    count int, distance int);"
    cur.execute(sql)
    sql="INSERT INTO ODRouteCountDist (odrouteid, count, distance) SELECT \
    a.odrouteid, a.count, b.distance from odroute a JOIN odroutedist b \
    ON a.odrouteid=b.odrouteid;"
    cur.execute(sql)
    con.commit()
ODRouteCountDist(con,cur)

def plotRationality(con,cur):
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
plotRationality(con,cur)
################################# Testing 1: Only Os and Ds ##################
def createIndividualRoute(con,cur):
    sql = "DROP TABLE IF EXISTS IndividualRoute;"
    cur.execute(sql)          
    sql="CREATE TABLE individualRoute ( \
            emp_no SERIAL PRIMARY KEY, \
            ODrouteID text, \
            count integer, OriginSiteID text, DestSiteID text);"
    cur.execute(sql)          
    con.commit() 
createIndividualRoute(con,cur)
def makeIndividualRoute(con,cur):
    print("define OD route from each sensor to each other")
    Routes = {"MeasureableRouteID":["MAC000010119>MAC000010130",\
                                    "MAC000010102>MAC000010104",\
                                    "MAC000010121>MAC000010124",\
                                    "MAC000010101>MAC000010119",\
                                    "MAC000010123>MAC000010120"],\
               "OriginSiteID":["MAC000010119","MAC000010102",\
                               "MAC000010121","MAC000010101","MAC000010123"],
                "DestSiteID":["MAC000010130","MAC000010104",\
                              "MAC000010124","MAC000010119","MAC000010120"]}
    df=pd.DataFrame.from_dict(Routes)
    for i in range(0, df.shape[0]):    #each route
        routeID = df['MeasureableRouteID'][i]
        oSiteID = df['OriginSiteID'][i]
        dSiteID = df['DestSiteID'][i]
        #MAC matching
        sql = "SELECT d.siteID AS dSiteID,  d.mac as dmac, d.timestamp as dtimestamp  ,\
        o.siteID AS oSiteID,  o.mac as omac, o.timestamp as otimestamp  \
        FROM Detection1 AS d, Detection1 AS o  \
        WHERE d.timestamp>o.timestamp AND d.timestamp-o.timestamp<interval '1 hour' \
        AND o.mac=d.mac  AND o.siteID='%s' AND d.siteID='%s'"%(oSiteID, dSiteID)
        print(sql)
        df_matches = pd.read_sql_query(sql,con)
        count = df_matches.shape[0]  #count number of bluetooth matches
        #these two variables allow us to compute flows for differnt time windows. Here we just take one whole day.
        sql = "INSERT INTO IndividualRoute (ODrouteID, count, OriginSiteID, DestSiteID) \
        VALUES ('%s', %i, '%s', '%s')"\
        %(routeID, count, oSiteID, dSiteID)
        cur.execute(sql)
    con.commit() 
makeIndividualRoute(con,cur) 

################################ Testing 2: Only one OMD ####################
def createIndividualRoute1(con,cur):
    sql = "DROP TABLE IF EXISTS IndividualRoute1;"
    cur.execute(sql)          
    sql="CREATE TABLE individualRoute1 (emp_no SERIAL PRIMARY KEY, \
            ODrouteID text, count integer, OriginSiteID text, \
            MidSiteID text, DestSiteID text);"
    cur.execute(sql)          
    con.commit() 
createIndividualRoute1(con,cur)
def makeIndividualRoute1(con,cur):
    print("define OD route from each sensor to each other")
    Routes = {"MeasureableRouteID":["MAC000010119>MAC000010104>MAC000010130",\
                                    "MAC000010119>MAC000010109>MAC000010130"],\
               "OriginSiteID":["MAC000010119","MAC000010119"],\
               "MidSiteID":["MAC000010104","MAC000010109"],\
                "DestSiteID":["MAC000010130","MAC000010130"]}
    df=pd.DataFrame.from_dict(Routes)
    for i in range(0, df.shape[0]):    #each route
        routeID = df['MeasureableRouteID'][i]
        oSiteID = df['OriginSiteID'][i]
        mSiteID = df["MidSiteID"][i]
        dSiteID = df['DestSiteID'][i]
        #MAC matching
        sql = "SELECT d.siteID AS dSiteID,  d.mac as dmac, d.timestamp as dtimestamp  ,\
        m.siteID AS mSiteID,  m.mac as mmac, m.timestamp as mtimestamp, \
        o.siteID AS oSiteID,  o.mac as omac, o.timestamp as otimestamp  \
        FROM Detection1 AS d, Detection1 AS m, Detection1 AS o  \
        WHERE (((o.mac=m.mac) AND (m.mac=d.mac)) AND \
        ((d.timestamp>m.timestamp) AND (m.timestamp>o.timestamp))) \
        AND (o.siteID='%s') AND (m.SiteID='%s')\
        AND (d.siteID='%s')"%(oSiteID, mSiteID, dSiteID)
        print(sql)
        df_matches = pd.read_sql_query(sql,con)
        count = df_matches.shape[0]  #count number of bluetooth matches
        #these two variables allow us to compute flows for differnt time windows. Here we just take one whole day.
        sql = "INSERT INTO IndividualRoute1 (ODrouteID, count, OriginSiteID, MidSiteID, DestSiteID) \
        VALUES ('%s', %i, '%s', '%s', '%s')"\
        %(routeID, count, oSiteID, mSiteID, dSiteID)
        cur.execute(sql)
    con.commit() 
makeIndividualRoute1(con,cur) 

# Plot selected OD routes


def makeOtoM(con,cur):
    sql="DROP TABLE IF EXISTS OtoM;"
    cur.execute(sql)
    sql="CREATE TABLE OtoM AS SELECT ODroutelinkOrigMid.odrouteid, \
    ODroutelinkOrigMid.start, ODroutelinkOrigMid.finish, \
    ODroutelinkOrigMid.link_gid, ways.the_geom FROM \
    ODRouteLinkOrigMid, ways WHERE ODroutelinkOrigMid.link_gid=ways.gid;"
    cur.execute(sql)
    con.commit()
makeOtoM(con,cur)
def makeMtoD(con,cur):
    sql="DROP TABLE IF EXISTS MtoD;"
    cur.execute(sql)
    sql="CREATE TABLE MtoD AS SELECT ODroutelinkMidDest.odrouteid, \
    ODroutelinkMidDest.start, ODroutelinkMidDest.finish, \
    ODroutelinkMidDest.link_gid, ways.the_geom FROM \
    ODroutelinkMidDest, ways WHERE ODroutelinkMidDest.link_gid=ways.gid;"
    cur.execute(sql)
    con.commit()
makeMtoD(con,cur)
def plotSelectedODRoutes():
    s=folium.Map(location=[53.235048, -1.421629], zoom_start=13)
    
    folium.CircleMarker(location=[53.233640,-1.397630], radius=10, color='blue',\
                           fill=True, fill_opacity=0.6, \
                           opacity=1, popup='MAC000010101').add_to(s)
    # Plot MAC000010119
    folium.CircleMarker(location=[53.221055,-1.449628], radius=10, color='red',\
                           fill=True, fill_opacity=0.6, \
                           opacity=1, popup='MAC000010119').add_to(s)
    # Plot MAC000010104
    folium.CircleMarker(location=[53.223355,-1.426487], radius=5, color='green',\
                           fill=True, fill_opacity=0.6, \
                           opacity=1, popup='MAC000010104').add_to(s)
    # Plot MAC000010118
    folium.CircleMarker(location=[53.232450,-1.452898], radius=5, color='green',\
                           fill=True, fill_opacity=0.6, \
                           opacity=1, popup='MAC000010118').add_to(s)
    
    sql="SELECT * FROM OtoM WHERE odrouteid='MAC000010101>MAC000010104>MAC000010119' \
        OR odrouteid='MAC000010101>MAC000010118>MAC000010119';"
    df_links = gpd.GeoDataFrame.from_postgis(sql,con,geom_col='the_geom') 
    df_links.to_csv('df_links1.csv')
    loc=[] # initialize an empty list to create a tuple with lat-lon
    for i in range(0,df_links.shape[0]): 
        link = df_links.iloc[i]
        lons = link['the_geom'].coords.xy[0] #coordinates in latlon
        lats = link['the_geom'].coords.xy[1]
        #gid = int(link.gid) 
        n_segments = len(lons)
        for j in range(0, n_segments):
            loc.append(tuple([lats[j],lons[j]])) # parse the latlon pairs for each segment
    print(loc) # print the tuple to see if we are correct
    folium.PolyLine(locations=loc, color='red', \
                        weight=2, opacity=1).add_to(s)
    
    sql="SELECT * FROM MtoD WHERE odrouteid='MAC000010101>MAC000010104>MAC000010119' \
        OR odrouteid='MAC000010101>MAC000010118>MAC000010119';"
    df_linksMD = gpd.GeoDataFrame.from_postgis(sql,con,geom_col='the_geom') 
    loc1=[] # initialize an empty list to create a tuple with lat-lon
    for i in range(0,df_linksMD.shape[0]): 
        link = df_linksMD.iloc[i]
        lons = link['the_geom'].coords.xy[0] #coordinates in latlon
        lats = link['the_geom'].coords.xy[1]
        #gid = int(link.gid) 
        n_segments = len(lons)
        for j in range(0, n_segments):
            loc1.append(tuple([lats[j],lons[j]])) # parse the latlon pairs for each segment
    print(loc1) # print the tuple to see if we are correct
    folium.PolyLine(locations=loc1, color='red', \
                        weight=2, opacity=1).add_to(s)
        
    s.save("chesterfield66.html")
plotSelectedODRoutes()


            



def createomdSites(con,cur):
    sql = "DROP TABLE IF EXISTS omdSite;"
    cur.execute(sql) 
    # Use St_AsText(geom) to see x, y in a readable format         
    sql="CREATE TABLE omdSite AS SELECT siteID, geom FROM BluetoothSite WHERE siteid='MAC000010101' OR\
            siteid='MAC000010102' OR siteid='MAC000010104' OR siteid='MAC000010109' \
            OR siteid='MAC000010112' OR siteid='MAC000010113' OR siteid='MAC000010114' \
            OR siteid='MAC000010118' OR siteid='MAC000010119' OR siteid='MAC000010120'\
            OR siteid='MAC000010121' OR siteid='MAC000010123' OR siteid='MAC000010124'\
            OR siteid='MAC000010130';"
    cur.execute(sql)          
    con.commit() 
createomdSites(con,cur) 

def importRoads1():     #(data has come from openstreetmap, then ogr2ogr )
    print("importing roads...")
    sql = "DROP TABLE IF EXISTS Road1;"
    cur.execute(sql)
    sql = "CREATE TABLE Road1 (name text, geom geometry, highway text);"
    cur.execute(sql)
    fn_osm_shp = "/headless/data/dcc1.osm.shp/lines.shp"
    df_roads = gpd.GeoDataFrame.from_file(fn_osm_shp)
    df_roads = df_roads.to_crs({'init': 'epsg:27700'})
    for index, row in df_roads.iterrows():
        sql="INSERT INTO Road1 VALUES ('%s', '%s', '%s');"%(row.name, row.geometry, row.highway )
        cur.execute(sql)
    con.commit() 
importRoads1()
def plotRoads1():   
    print("plotting roads...")
    sql = "SELECT * FROM Road1;"
    df_roads = gpd.GeoDataFrame.from_postgis(sql,con,geom_col='geom') #
    for index, row in df_roads.iterrows():
        (xs,ys) = row['geom'].coords.xy
        (lon,lat)=pyproj.transform(bng, wgs84, xs, ys) #transform bng to wgs84
        color='gray'
        #road colour by type
        if row['highway']=="motorway":
            color = 'b'
        if row['highway']=="trunk":
            color = 'g'
        #if not color=='y':  #only plot major roads
        plot(lon, lat, color)
def omdSiteCRS(con,cur):  
    sql = "DROP TABLE IF EXISTS omdSiteCRS;"
    cur.execute(sql)
    sql = "CREATE TABLE omdSiteCRS AS SELECT siteid, ST_AsText(geom) \
            FROM omdSite;"
    cur.execute(sql)
    con.commit()
omdSiteCRS(con,cur)
def plotBluetoothSites1():
    m=folium.Map(location=[53.235048, -1.421629], zoom_start=13)
    sql="SELECT * FROM omdSite;"
    df_sites = gpd.GeoDataFrame.from_postgis(sql,con,geom_col='geom') #
    for index, row in df_sites.iterrows():
        (xs,ys) = row['geom'].coords.xy
        (lon,lat)=pyproj.transform(bng, wgs84, xs, ys) #transform bng to wgs84
       # plot(lon, lat, 'bo')
    # Plot MAC000010101
    folium.CircleMarker(location=[53.233640,-1.397630], radius=10, color='blue',\
                           fill=True, fill_opacity=0.6, \
                           opacity=1, popup='MAC000010101').add_to(m)
    # Plot MAC000010119
    folium.CircleMarker(location=[53.221055,-1.449628], radius=10, color='red',\
                           fill=True, fill_opacity=0.6, \
                           opacity=1, popup='MAC000010119').add_to(m)
    # Plot MAC000010104
    folium.CircleMarker(location=[53.223355,-1.426487], radius=5, color='green',\
                           fill=True, fill_opacity=0.6, \
                           opacity=1, popup='MAC000010104').add_to(m)
    # Plot MAC000010118
    folium.CircleMarker(location=[53.232450,-1.452898], radius=5, color='green',\
                           fill=True, fill_opacity=0.6, \
                           opacity=1, popup='MAC000010118').add_to(m)
    #df_sites=df_sites.to_json()
    #folium.CircleMarker(location=[53.235048, -1.421629], radius=5, color='blue',\
     #                   fill=True, fill_opacity=0.6, \
      #                  opacity=1).add_to(m)
    #BTsites=folium.FeatureGroup(name='BTsites')
    #BTsites.add_child(df_sites)
    #m.add_child(BTsites)
    m.save("chesterfieldOD1.html")
plotRoads1()
plotBluetoothSites1()

df_sites = pd.read_csv('df_sites.csv')
for index, row in df_sites.iterrows():
        (xs,ys) = row['geom'].coords.xy




m=folium.Map(location=[53.235048, -1.421629], zoom_start=13)

folium.CircleMarker(location=[53.233640,-1.397630], radius=10, color='blue',\
                           fill=True, fill_opacity=0.6, \
                           opacity=1, popup='MAC000010101').add_to(m)
    # Plot MAC000010119
folium.CircleMarker(location=[53.221055,-1.449628], radius=10, color='red',\
                           fill=True, fill_opacity=0.6, \
                           opacity=1, popup='MAC000010119').add_to(m)
    # Plot MAC000010104
folium.CircleMarker(location=[53.223355,-1.426487], radius=5, color='green',\
                           fill=True, fill_opacity=0.6, \
                           opacity=1, popup='MAC000010104').add_to(m)
    # Plot MAC000010118
folium.CircleMarker(location=[53.232450,-1.452898], radius=5, color='green',\
                           fill=True, fill_opacity=0.6, \
                           opacity=1, popup='MAC000010118').add_to(m)

df_locOD1 = pd.read_csv('locOD1.csv') # load df with latlon from OSMnX
locOD1=df_locOD1.values.tolist() # convert df to list

pp=folium.PolyLine(locations=locOD1, color='red', \
                        weight=2, opacity=1)
m.add_children(pp)
m.save("chesterfield100.html")
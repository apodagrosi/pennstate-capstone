#==============================================================================
# MIT License
# 
# Copyright (c) 2017 Angelo Podagrosi
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
# 
#==============================================================================
# -*- coding: utf-8 -*-

"""
SCRIPT OVERVIEW AND CODE TO BE CHANGED BY USER

1) Script creates database table to contain polygons of farm fields.
2) Populates this new table based on polygons from existing shapefile.

Main components to be changed by user:
1) Postgres database connection
2) File path to folder of shapefiles (variable 'srcFile')

Code created spring 2017 by Angelo Podagrosi
Questions to angelo.podagrosi@gmail.com

"""

#########################
import sys
import datetime
import psycopg2

try:
    from osgeo import ogr, gdal
except:
    sys.exit('ERROR: cannot find GDAL/OGR modules')

# Print current time to assist in tracking total processing time
print("Current time: " + str(datetime.datetime.now()))

# Connect to database
try:
    connection = psycopg2.connect("dbname='PrecisionAg_v1' user='postgres' host='localhost' password='PASSWORD'")
    print("I am able to connect to the database! :)")
except:
    print("I am unable to connect to the database.")

# Establish cursor connection to database; necessary to begin providing commands/queries to database
cursor = connection.cursor()

###############################################################################
# Create FIELD polygon table

#commands_createFieldTable = ("""
cursor.execute("""
-- Create field polygon table
CREATE TABLE field_polygons_v1(id serial,
field_ID smallint NOT NULL,
final_farm VARCHAR NOT NULL,
finalfield VARCHAR NOT NULL,
owner_ID smallint NOT NULL,
CONSTRAINT fieldpolygon_pkey PRIMARY KEY (id),
FOREIGN KEY (owner_ID) REFERENCES owner (owner_id),
FOREIGN KEY (field_ID) REFERENCES field (field_ID)
);""")
connection.commit()
print("Created field polygon table with geography field.")

# Create geometry/geography field
#cursor.execute("""SELECT AddGeometryColumn('public', 'field_polygons_v1', 'geog', 4326, 'POLYGON', 2);""")
# http://postgis.17.x6.nabble.com/How-to-add-a-geography-column-td5002073.html
#cursor.execute("""ALTER TABLE field_polygons_v1
#               ADD COLUMN geography geography(Polygon,4326);""")
# Decided to utilize 'geometry' (http://workshops.boundlessgeo.com/postgis-intro/geography.html#why-not-use-geography)
cursor.execute("""ALTER TABLE field_polygons_v1
               ADD COLUMN geom geometry(Polygon,4326);""")
connection.commit()

# Helpful links:
# http://andrewgaidus.com/Build_Query_Spatial_Database/

srcFile = r'FILE PATH TO FOLDER OF FINAL FARM FIELD POLYGON SHAPEFILE\Final_Fields_v1.shp'
#shapefile = osgeo.ogr.Open(srcFile)
#layer = shapefile.GetLayer(0)

driver = ogr.GetDriverByName("ESRI Shapefile")
dataSource = driver.Open(srcFile, 0)     # 0 means read-only; 1 means writeable.
# .GetLayer allows access to features in a layer - http://pcjericks.github.io/py-gdalogr-cookbook/layers.html#iterate-over-features
layer = dataSource.GetLayer()
# For instance could access attributes of a specific field using: for feature in layer: print feature.GetField("Elevation_")

#for feature in layer:
for i in range(layer.GetFeatureCount()):   
    # Identify each feature and their attributes
    #attributes = feature.items()
    #print(attributes)
    
    # Identify each of the individual attributes for each feature
        # Helpful link to identify each attribute: https://pcjericks.github.io/py-gdalogr-cookbook/vector_layers.html#iterate-over-features
        # Also helpful: http://andrewgaidus.com/Build_Query_Spatial_Database/
    feature = layer.GetFeature(i)
    recordID = feature.GetField("id")
    fieldID = feature.GetField("field_id")
    finalFarm = feature.GetField("Final_Farm")
    finalField = feature.GetField("FinalField")
    ownerID = feature.GetField("owner_id")
    print("FieldID: " + str(fieldID) + ", " + "Final_Farm: " + finalFarm + ", " + "FinalField: " + finalField + ", " + "Owner_ID: " + str(ownerID))
    geometry = feature.GetGeometryRef()
    wkt = geometry.ExportToWkt()

    #Insert data into database, converting WKT geometry to a PostGIS geography
    # Struggled with ‘INSERT INTO…VALUES’ because script initially was unable to read strings of farm/field names. Needed to differentiate the values as string with single quotes (‘ ‘)
    cursor.execute("INSERT INTO field_polygons_v1 (id, field_id, final_farm, finalfield, owner_ID, geom) VALUES ({},{},'{}','{}',{}, ST_GeomFromText('{}','4326'))".format(recordID, fieldID, finalFarm, finalField, ownerID, wkt))

# Commit the changes to the database    
connection.commit()

############################
# Close communication with the Postgres database server
cursor.close()
connection.close()
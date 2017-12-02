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

1) Code will add the appopriate geometry field to selected database table and then 
spatially-enable (to Web Mercator in this case) using latitude and longitude fields.
2) Spatial index is created.

Main components to be changed by user:
1) Postgres database connection
2) Relevant database tables

Code created spring 2017 by Angelo Podagrosi
Questions to angelo.podagrosi@gmail.com

"""

# Import necessary Python packages and libraries
import datetime
import psycopg2


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
## Adding and populating geometry fields (either WGS84 (EPSG: 4326) or 'Web Mercator' (EPSG: 3857)) for final point files

## Reference:
# AddGeometryColumn(<schema_name>, <table_name>, <column_name>, <srid>, <type>, <dimension>)
# Sources: https://postgis.net/docs/using_postgis_dbmanagement.html ; 

commands_addGEOMFields_points = (
"""
SELECT AddGeometryColumn('public', 'yield_point', 'geom_3857', 3857, 'POINT', 2);
""")
    # Appears to be case-sensitive (case of file name needed to match case of table name in dbase)

cursor.execute(commands_addGEOMFields_points)

print("Added 'geom_3857' field to final point files.")
connection.commit()

# Populate the 'geom_3857' geometry field based on the latitude and longitude fields, 
# imported in datum WGS84.
commands_populateGEOMFields_points = (
"""
UPDATE yield_point SET geom_3857 = ST_Transform((ST_SetSRID(ST_MakePoint(yield_point.longitude, yield_point.latitude), 4326)), 3857);
""")

print("...Populating geometry field to final point files beginning at " + str(datetime.datetime.now()) + "...")
cursor.execute(commands_populateGEOMFields_points)

print("Populated geometry fields to final point files.")
connection.commit()
print("Current time: " + str(datetime.datetime.now()))


## Creating spatial indexes on geometry columns
commands_createSpatialIndex = (
"""
CREATE INDEX yield_point_geom3857 ON yield_point USING gist(geom_3857);
""")

print("...Creating spatial index on geometry columns " + str(datetime.datetime.now()) + "...")

cursor.execute(commands_createSpatialIndex)

print("Created spatial indexes on geometry columns")
connection.commit()
print("Current time: " + str(datetime.datetime.now()))

# Close communication with the Postgres database server
cursor.close()
connection.close()
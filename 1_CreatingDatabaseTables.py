# -*- coding: utf-8 -*-
"""

This code creates tables within an existing PostgreSQL database.
1) Import Python packages
2) Test connection to database; print reply
3) Sample scripts to create tables in database:
        -- Sample farmer and owner tables
        -- Sample scratch tables to contain data from CSVs
        -- Final table to contain yield/harvest data from multiple sources
            from above scratch tables
            Note that currently 'field' table is not created and therefore the
            appropriate primary key on this table is commented out.

"""

# Import necessary Python packages and libraries
import datetime
import psycopg2
    # Note psycopg2 was 'conda' installed - https://anaconda.org/anaconda/psycopg2

# Print current time to assist in tracking total processing time
print("Current time: " + str(datetime.datetime.now()))

#connection = psycopg2.connect("dbname='PrecisionAg_v1' user='postgres' host='localhost' password='postgis'")

# Connect to database
try:
    connection = psycopg2.connect("dbname='scratch' user='postgres' host='localhost' password='postgis'")
    print("I am able to connect to the database! :)")
except:
    print("I am unable to connect to the database.")

# Establish cursor connection to database; necessary to begin providing commands/queries to database
cursor = connection.cursor()

###############################################################################
"""

--> Brief overview of how Python can interact with Postgres database using 'psycopg2' <--
Multiple SQL statement (queries or adjustments to tables) can be created within 
one large Python command. Each SQL statement is contains a semicolon to complete
the SQL and is surrounded by a set of 3 doublequotes. Each SQL statement is then
separated by a command.

For instance, the 'CREATE TABLE' SQL statement examples below include three 
double quotes to identify that the syntax below will be a SQL statement, followed
by the table name ('farmer' or 'owner'), open parenthesis, each field and datatype
of the table, close parenthesis, semi colon, and three double quotes to complete 
the SQL statement. This entire SQL statement inside of the 2 sets of triple double 
quotes can be used directly within the standard SQL window of Postgres.
    --> It is recommended to experiment with the SQL statements directly within 
    Postgres in order to verify the syntax before using in Python.

In the examples below, these different components (the SQL, semicolons, 3 doublequotes)
are clearly identified on separate lines.

The separate SQL statements are executed individually by looping through the 
entire Python command of SQL utilizing a 'for' loop.

SQL comments are identified by two dashes ( -- ).

"""
###############################################################################
# Create FARMER and OWNER tables

print("...Creating farmer and owner tables...")
# Create SQL commands separated by commas
commands_createFarmerOwnerTable = (
"""
CREATE TABLE farmer(
farmer_id serial,
first_name VARCHAR (30) NOT NULL,
middle_name VARCHAR (30) NULL,
last_name VARCHAR (100) NOT NULL,
address1 VARCHAR (50) NULL,
address2 VARCHAR (50) NULL,
city VARCHAR (30) NULL,
state VARCHAR (2) NULL,
zip VARCHAR (12) NULL,
phone_cell SMALLINT NULL,
phone_home SMALLINT NULL,
CONSTRAINT farmer_pkey PRIMARY KEY (farmer_id)  -- Example of how a primary key (identified as 'farmer_pkey') is created on field 'farmer_id' 
);
""",
"""
CREATE TABLE owner(
owner_id serial,
first_name VARCHAR (30) NOT NULL,
middle_name VARCHAR (30) NULL,
last_name VARCHAR (100) NOT NULL,
address1 VARCHAR (50) NULL,
address2 VARCHAR (50) NULL,
city VARCHAR (30) NULL,
state VARCHAR (2) NULL,
zip VARCHAR (12) NULL,
phone_cell SMALLINT NULL,
phone_home SMALLINT NULL,
CONSTRAINT owner_pkey PRIMARY KEY (owner_id)  -- Example of how a primary key (identified as 'owner_pkey') is created on field 'owner_id' 
);
""")

# Loop through SQL commands using 'for' loop, executing each individually
for command in commands_createFarmerOwnerTable:
    cursor.execute(command)
print("Completed: Created farmer and owner tables.")

# Commit the changes to the database
connection.commit()
print("Current time: " + str(datetime.datetime.now()))

##################################################################
# Create FIELD table - both temporary ('_CSVimport_field') and final ('field')
commands_createFieldTable = ("""
-- Create temporary field table
CREATE TABLE _CSVimport_field(id serial NOT NULL,
jd_farm VARCHAR(30) NULL,
jd_field VARCHAR(30) NULL,
fv_farm VARCHAR(30) NULL,
fv_field VARCHAR(30) NULL,
agf_farm VARCHAR(30) NULL,
agf_field VARCHAR(30) NULL,
final_farm VARCHAR(30) NULL,
finalfield VARCHAR(40) NULL,
field_id smallint NOT NULL,
owner_id smallint NOT NULL,
CONSTRAINT csvFieldID_pkey PRIMARY KEY (id)
);""",
"""
-- Create final field table
CREATE TABLE field(field_ID serial,
farm_name VARCHAR(30) NOT NULL,
field_name VARCHAR(40) NOT NULL,
owner_ID smallint NOT NULL,
CONSTRAINT fieldID_pkey PRIMARY KEY (field_ID),
FOREIGN KEY (owner_ID) REFERENCES owner (owner_id)
);"""
)

for cursorCommand in commands_createFieldTable:
    cursor.execute(cursorCommand)
print("Created field table.")

# Adding field-keytable to database and populating with CSV 
# This table will contain the unique file names of the raw CSV files. This table was used to identify 
# the event (planting/harvesting), year, the participating farmer (farmerID), and the appropriate unqiue farm/field name.
commands_createFieldKeyTable = ("""
-- Create temporary field table
CREATE TABLE _CSVimport_field_key(
id serial,
file_name VARCHAR(100) NULL,
source VARCHAR(30) NULL,
event VARCHAR(30) NULL,
year smallint NULL,
farmerID smallint NULL,
final_farm VARCHAR(30) NOT NULL,
final_field VARCHAR(30) NOT NULL,
notes VARCHAR(150),
CONSTRAINT fieldkey_id_pkey PRIMARY KEY (ID)
);"""
)

cursor.execute(commands_createFieldKeyTable)
print("Created field-key table.")

connection.commit()

###############################################################################

# Create table to contain product identifiers in order to identify if whether corn or soybean

commands_createProductsTable = (
"""
CREATE TABLE products(
id serial,
productname VARCHAR (30) NOT NULL,
count INTEGER NOT NULL,
source VARCHAR (20) NOT NULL,
product VARCHAR (20) NOT NULL,
corn SMALLINT NOT NULL,
soybean SMALLINT NOT NULL,
company VARCHAR (30) NULL,
document1 VARCHAR (100) NULL,
document2 VARCHAR (100) NULL,
CONSTRAINT products_unique PRIMARY KEY (ID)
);
""")

cursor.execute(commands_createProductsTable)
print("Created products table.")

connection.commit()

###############################################################################

# Below is an example of creating the scratch tables to contain the raw precision 
# agriculture data from the CSVs

print("...Creating scratch tables to contain raw data from CSVs...")
# Create SQL commands separated by commas
commands_createPointScratchTablesForCSV = (
"""
-- Creating scratch John Deere yield table
CREATE TABLE _CSVimport_yield_point_JD(
id serial,
id_pd integer NULL,
longitude numeric(12,8) NOT NULL,
latitude numeric(12,8) NOT NULL,
field VARCHAR(30) NOT NULL,
dataset VARCHAR(100) NULL,
product VARCHAR(50) NULL,
obj__id numeric(12,4) NULL,
distance_f double precision NULL,
track_deg_ double precision NULL,
duration_s double precision NULL,
elevation_ numeric(12,4) NULL,
"time" date NULL,
area_count VARCHAR(12) NULL,
swth_wdth numeric(12,4) NULL,
y_offset_f numeric(12,4) NULL,
crop_flw_m numeric(12,4) NULL,
moisture__ numeric(12,4) NULL,
yld_mass_w numeric(18,6) NOT NULL,
yld_vol_we numeric(18,6) NOT NULL,
yld_mass_d numeric(18,6) NOT NULL,
yld_vol_dr numeric(18,6) NOT NULL,
humidity__ numeric(12,4) NULL,
air_temp__ numeric(12,4) NULL,
wind_speed numeric(12,4) NULL,
soil_temp_ numeric(24,16) NULL,
pass_num numeric(12,4) NULL,
speed_mph_ numeric(12,4) NULL,
prod_ac_h_ numeric(18,6) NOT NULL,
crop_flw_v numeric(18,6) NOT NULL,
date date NOT NULL,
org_file VARCHAR(100) NULL,
file_source VARCHAR(20) NULL,
CONSTRAINT yieldpointjd_id_pkey PRIMARY KEY (ID)
);""",
"""
-- Creating scratch AgFiniti yield table
CREATE TABLE _CSVimport_yield_point_AgFiniti(
id serial,
id_pd integer NULL,
longitude numeric(12,8) NOT NULL,
latitude numeric(12,8) NOT NULL,
field VARCHAR(30) NOT NULL,
dataset VARCHAR(100) NULL,
product VARCHAR(50) NULL,
obj__id numeric(12,4) NULL,
track_deg_ double precision NULL,
swth_wdth numeric(12,4) NULL,
distance_f double precision NULL,
duration_s double precision NULL,
elevation_ numeric(12,4) NULL,
area_count VARCHAR(12) NULL,
diff_statu VARCHAR(12) NULL,
"time" date NULL,
x_offset_f numeric(12,4) NULL,
y_offset_f numeric(12,4) NULL,
satellites smallint NULL,
hding_veh_ numeric(12,4) NULL,
diff_statu_1 varchar(12) NULL,
active_row numeric(12,4) null,
vdop numeric(12,4) NULL,
hdop numeric(12,4) NULL,
pdop numeric(12,4) NULL,
crop_flw_m numeric(18,6) NULL,
moisture__ numeric(12,4) NULL,
grain_temp numeric(12,4) NULL,
pass_num numeric(12,4) NULL,
yld_mass_d numeric(18,6) NOT NULL,
yld_vol_dr numeric(18,6) NOT NULL,
yld_mass_w numeric(18,6) NOT NULL,
yld_vol_we numeric(18,6) NOT NULL,
speed_mph_ numeric(12,4) NULL,
prod_ac_h_ numeric(18,6) NOT NULL,
crop_flw_v numeric(18,6) NOT NULL,
date date NOT NULL,
org_file VARCHAR(100) NULL,
file_source VARCHAR(20) NULL,
CONSTRAINT yieldpointagfiniti_id_pkey PRIMARY KEY (ID)
);"""
)

# Loop through SQL commands using 'for' loop, executing each individually
for command2 in commands_createPointScratchTablesForCSV:
    cursor.execute(command2)
print("Completed: Created scratch tables to contain separate CSV point data.")

# Commit the changes to the database
connection.commit()
print("Current time: " + str(datetime.datetime.now()))


###############################################################################

# Below is an example of creating the final table to contain the raw precision 
# agriculture data appended from the CSVs

print("...Creating scratch table to contain final yield/harvest data from raw data received in CSV files...")

# Create SQL command
commands_createFinalYieldPointTables = (
"""
-- Creating final yield point table to contain all records from CSV table
CREATE TABLE yield_point(
id serial,
longitude numeric(12,8) NOT NULL,
latitude numeric(12,8) NOT NULL,
field VARCHAR(30) NOT NULL,
dataset VARCHAR(100) NULL,
product VARCHAR(50) NULL,
obj__id numeric(12,4) NULL,
track_deg_ double precision NULL,
swth_wdth numeric(12,4) NULL,
distance_f double precision NULL,
duration_s double precision NULL,
elevation_ numeric(12,4) NULL,
area_count VARCHAR(12) NULL,
diff_statu VARCHAR(12) NULL,
"time" date NULL,
x_offset_f numeric(12,4) NULL,
y_offset_f numeric(12,4) NULL,
satellites smallint NULL,
hding_veh_ numeric(12,4) NULL,
diff_statu_1 varchar(12) NULL,
active_row numeric(12,4) null,
vdop numeric(12,4) NULL,
hdop numeric(12,4) NULL,
pdop numeric(12,4) NULL,
crop_flw_m numeric(18,6) NULL,
moisture__ numeric(12,4) NULL,
humidity__ numeric(12,4) NULL,
air_temp__ numeric(12,4) NULL,
grain_temp numeric(12,4) NULL,
soil_temp__ numeric(24,16) NULL,
wind_speed numeric(12,4) NULL,
pass_num numeric(12,4) NULL,
yld_mass_d numeric(18,6) NOT NULL,
yld_vol_dr numeric(18,6) NOT NULL,
yld_mass_w numeric(18,6) NOT NULL,
yld_vol_we numeric(18,6) NOT NULL,
speed_mph_ numeric(12,4) NULL,
prod_ac_h_ numeric(18,6) NOT NULL,
crop_flw_v numeric(18,6) NOT NULL,
date date NOT NULL,
org_file VARCHAR(100) NULL,
file_source VARCHAR(20) NULL,
field_ID smallint NULL REFERENCES field (field_id),   
farmer_id smallint NULL REFERENCES farmer (farmer_ID),
CONSTRAINT yield_point_id_pkey PRIMARY KEY (ID)
);"""
)

cursor.execute(commands_createFinalYieldPointTables)

print("Created final table to contain yield/harvest point data from all CSV files.")

# Commit the changes to the database
connection.commit()
print("All database changes committed.")
print("Current time: " + str(datetime.datetime.now()))

###############################################################################


# Close communication with the Postgres database server
cursor.close()
connection.close()
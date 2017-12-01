# -*- coding: utf-8 -*-
"""

This code populates the created tables within the existing PostgreSQL database.
1) Import Python packages
2) Test connection to database; print reply
3) Sample scripts to populate existing tables in database from CSVs:
        -- Sample farmer, owner, and products tables
        -- Sample table of unique farm/field names ("Farm_Fields.csv")
            -- Also to identify the ownerID
        -- Sample of every imported precision ag files, in order to identify farmer ID
            and appropriate farm/field name combinations ("AllFiles_Farm_Fields.csv")
        -- Sample scratch tables to contain data from CSVs, by vendor
            -- Add and populate 'field_ID' and 'farmer_ID' based on original file name from "AllFiles_Farm_Fields.csv"
        -- Combine scratch tables of CSVs by vendor into one file - "yield_point"
        -- Add flag for corn or soybean

TO BE ADJUSTED BY THE USER:
-- Folder paths of raw CSV files
-- Field headings for each of the raw files

"""

# Import necessary Python packages and libraries
import os
import datetime
import psycopg2
import pandas as pd

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

# Identify folders containing the raw CSV precision agriculture data
directory_yieldJD = r'C:\GIS\CapstoneReport\Yield_JD_Point'
directory_yieldAgFiniti = r'C:\GIS\CapstoneReport\Yield_AgFiniti_Point'

###############################################################################
# Populate FARMER and OWNER tables

print("...Populating farmer and owner tables...")
# Populate ('COPY') SQL commands separated by commas
commands_populateFarmerOwnerTable = (
"""
COPY farmer(first_name, middle_name, last_name, address1, address2, city, state, zip, phone_cell, phone_home)
FROM 'C:\GIS\CapstoneReport\_AdditionalTables\Table_Farmers.csv' DELIMITER ',' CSV HEADER;
""",
"""
COPY owner(first_name, middle_name, last_name, address1, address2, city, state, zip, phone_cell, phone_home)
FROM 'C:\GIS\CapstoneReport\_AdditionalTables\Table_Owners.csv' DELIMITER ',' CSV HEADER;
"""
)

# Loop through SQL commands using 'for' loop, executing each individually
for command in commands_populateFarmerOwnerTable:
    cursor.execute(command)
print("Completed: Populated farmer and owner tables.")

# Commit the changes to the database
connection.commit()
print("Current time: " + str(datetime.datetime.now()))


# Populate 'products' table with information as to whether corn or soybean
products_copy_command = (
"""
COPY products(productname, count, source, product, corn, soybean, company, document1, document2)
FROM 'C:\GIS\CapstoneReport\_AdditionalTables\products_combined.csv' DELIMITER ',' CSV HEADER;
"""
)
cursor.execute(products_copy_command)
print("Copied records from CSV to new table: Products (corn or soybean) (products)")
# Commit the changes
connection.commit()

###############################################################################
# Populate temporary ('_CSVimport_field') and then 'INSERT INTO' same information into final 'field' table
# 'Farm_Fields' is used to identify the unique farm/field name combinations based on original data
                     
fields_copy_command = (
"""
COPY _CSVimport_field(jd_farm, jd_field, fv_farm, fv_field, agf_farm, agf_field, final_farm, finalfield, field_id, owner_id)
FROM 'C:\GIS\CapstoneReport\_AdditionalTables\Farm_Fields.csv' DELIMITER ',' CSV HEADER;
"""
)
cursor.execute(fields_copy_command)
print("Copied records from CSV to new table: Fields (_CSVimport_field)")
# Commit the changes
connection.commit()

field_insertinto_command = (
"""
INSERT INTO field(field_id, farm_name, field_name, owner_id)
SELECT field_id, final_farm, finalfield, owner_id
FROM _CSVimport_field;
"""
)

cursor.execute(field_insertinto_command)
print("Moved records from temporary table (from CSV) to new final table: Field")
# Commit the changes
connection.commit()


# Populate temporary ('_CSVimport_field_key') and then 'INSERT INTO' same information into final 'field' table
# 'AllFiles_Farm_Fields.csv' is a list of every file name and used to identify the event (planting/harvesting), year, farmerID and farm/field
fieldskey_copy_command = (
"""
COPY _CSVimport_field_key(file_name, source, event, year, farmerid, final_farm, final_field, notes)
FROM 'C:\GIS\CapstoneReport\_AdditionalTables\AllFiles_Farm_Fields.csv' DELIMITER ',' CSV HEADER;
"""
)
cursor.execute(fieldskey_copy_command)
print("Copied records from CSV to new table: Field's Key (_CSVimport_field_key)")
# Commit the changes
connection.commit()

# Adding 'field_id' field to "_CSVimport_field_key" and then populate based using 'inner join update'
_csvimport_fieldkey_fieldID_command = ("""
ALTER TABLE _csvimport_field_key
ADD COLUMN field_ID smallint;""",
"""
UPDATE _csvimport_field_key
SET field_id = f.field_id
FROM field as f
WHERE _csvimport_field_key.final_farm = f.farm_name AND _csvimport_field_key.final_field = f.field_name;"""
)

for cursorCommand in _csvimport_fieldkey_fieldID_command:
    cursor.execute(cursorCommand)
print("Added 'field_ID' column to _CSVimport_field_key and populated with 'inner join update' on 'field' table")
connection.commit()

###############################################################################
###############################################################################
"""
--> Process original raw CSVs and copy into existing database table <--
The below Python code loops through the folder of raw CSVs of precision agriculture data,
combining the records into one raw CSV file.
The code then uses 'COPY...FROM' to copy these records from the compiled CSV
to the existing Postgres table to hold all of the raw CSV records.

Included in the code below are several 'print' statements, currently commented out,
that can be helpful when experimenting with the script or verifying the status during the processing.

"""
#########################################################
# Yield - John Deere
# Note that field 'id_pd' is being populated with the ID generated from the pandas dataframe.
# Field 'id' in the Postgres table is autogenerated as the primary key

print("...Copying records from CSV to new table: John Deere yield data...")
print("Current time: " + str(datetime.datetime.now()))

yield_JD_columns = ['longitude','latitude','field','dataset','product','obj__id','distance_f','track_deg_','duration_s','elevation_','time','area_count','swth_wdth','y_offset_f','crop_flw_m','moisture__','yld_mass_w','yld_vol_we','yld_mass_d','yld_vol_dr','humidity__','air_temp__','wind_speed','soil_temp_','pass_num','speed_mph_','prod_ac_h_','crop_flw_v', 'date']

# Create empty 'pandas' dataframe to contain dataframes of raw CSVs to be created
dfs_yieldJD = []

# 'for' loop through the files in the directory
for input_file in os.listdir(directory_yieldJD):
    # Print the file being processed
    #print (os.path.join(directory, input_file))    
    
    # Loop through only CSVs (file extension .csv)
    if input_file[-4:] == '.csv':
        print(input_file)
        #print(str(input_file[:-4]))                

        # Read the CSV into a 'pandas' dataframe using the 'yield_JD_columns' field headings
        df = pd.read_csv((os.path.join(directory_yieldJD,input_file)),header = 0, names = yield_JD_columns)

        # Add column to contain string of original CSV file name        
        df['org_file'] = str(input_file[:-4])
        df['file_source'] = "johndeere"
        
        # Append current 'pandas' data frame to existing 'dfs_yieldJD' dataframe
        dfs_yieldJD.append(df)

# Concatenate dataframes into new dataframe
df_1 = pd.concat(dfs_yieldJD)
# Output the final, appended dataframe ('dfs_yieldJD') to a CSV
df_1.to_csv(os.path.join(directory_yieldJD, "_csvAppend_yield_point_JohnDeere2.csv"), encoding='utf-8')

# Copy the CSV containing the appended raw CSV data into the existing Postgres table
jdYieldCopy_Command = (
"""
COPY _CSVimport_yield_point_JD(id_pd, longitude, latitude, field, dataset, product, obj__id, distance_f, track_deg_, duration_s, elevation_, time, area_count, swth_wdth, y_offset_f, crop_flw_m, moisture__, yld_mass_w, yld_vol_we, yld_mass_d, yld_vol_dr, humidity__, air_temp__, wind_speed, soil_temp_, pass_num, speed_mph_, prod_ac_h_, crop_flw_v, date, org_file, file_source)
FROM 'C:\GIS\CapstoneReport\Yield_JD_Point\_csvAppend_yield_point_JohnDeere2.csv' DELIMITER ',' CSV HEADER;
"""
)
cursor.execute(jdYieldCopy_Command)
print("Copied records from CSV to new table: John Deere yield data")

# Commit the changes
connection.commit()
print("Database changes committed: John Deere yield data.")
print("Current time: " + str(datetime.datetime.now()))

###############################################################################
#Yield - AgFiniti
# Note that field 'id_pd' is being populated with the ID generated from the pandas dataframe.
# Field 'id' in the Postgres table is autogenerated as the primary key

print("...Copying records from CSV to new table: AgFiniti yield data...")
print("Current time: " + str(datetime.datetime.now()))

yield_AgFiniti_columns = ['longitude','latitude','field','dataset','product','obj__id','track_deg_','swth_wdth','distance_f','duration_s','elevation_','area_count','diff_statu','time','x_offset_f','y_offset_f','satellites','hding_veh_','diff_statu_1','active_row','vdop','hdop','pdop','crop_flw_m','moisture__','grain_temp','pass_num','yld_mass_d','yld_vol_dr','yld_mass_w','yld_vol_we','speed_mph_','prod_ac_h_','crop_flw_v','date']

# Create empty 'pandas' dataframe to contain dataframes of raw CSVs to be created
dfs_yieldAF = []

# 'for' loop through the files in the directory
for input_file in os.listdir(directory_yieldAgFiniti):
    # Print the file being processed
    #print (os.path.join(directory, input_file))    
    
    # Loop through only CSVs (file extension .csv)
    if input_file[-4:] == '.csv':
        print(input_file)
        #print(str(input_file[:-4]))                
        
        # Read the CSV into a 'pandas' dataframe using the 'yield_AgFiniti_columns' field headings
        df = pd.read_csv((os.path.join(directory_yieldAgFiniti,input_file)),header = 0, names = yield_AgFiniti_columns)
        
        # Add column to contain string of original CSV file name        
        df['org_file'] = str(input_file[:-4])
        df['file_source'] = "agfiniti"
        
        # Append current 'pandas' data frame to existing 'dfs_yieldJD' dataframe
        dfs_yieldAF.append(df)

# Concatenate dataframes into new dataframe
df_1 = pd.concat(dfs_yieldAF)
# Output the final, appended dataframe ('dfs_yieldJD') to a CSV
df_1.to_csv(os.path.join(directory_yieldAgFiniti, "_csvAppend_yield_point_AgFiniti2.csv"), encoding='utf-8')

# Copy the CSV containing the appended raw CSV data into the existing Postgres table
yield_agfiniti_copy_command = (
"""
COPY _CSVimport_yield_point_AgFiniti(id_pd, longitude, latitude, field, dataset, product, obj__id, track_deg_, swth_wdth, distance_f, duration_s, elevation_, area_count, diff_statu, time, x_offset_f, y_offset_f, satellites, hding_veh_, diff_statu_1, active_row, vdop, hdop, pdop, crop_flw_m, moisture__, grain_temp, pass_num, yld_mass_d, yld_vol_dr, yld_mass_w, yld_vol_we, speed_mph_, prod_ac_h_, crop_flw_v, date, org_file, file_source)
FROM 'C:\GIS\CapstoneReport\Yield_AgFiniti_Point\_csvAppend_yield_point_AgFiniti2.csv' DELIMITER ',' CSV HEADER;
"""
)
cursor.execute(yield_agfiniti_copy_command)
print("Copied records from CSV to new table: Yield - AgFiniti data")

# Commit the changes
connection.commit()
print("Database changes committed: Yield - AgFiniti data.")
print("Current time: " + str(datetime.datetime.now()))

###############################################################################
# Add 'field_id' and 'farmer_id' to all the raw CSV point files and populate 
# using inner join to “_CSVimport_field_key” on file name field (‘file_name’)

print("""Adding 'field_id' and 'farmer_id' fields to CSV table and populating by joining to "field" table based on the original file name: _CSVimport_yield_point_agfiniti.""")
yield_agfiniti_point_addFields_cursorCommand = ("""
ALTER TABLE _CSVimport_yield_point_agfiniti
ADD COLUMN field_ID smallint NULL REFERENCES field (field_id),
ADD COLUMN farmer_id smallint NULL REFERENCES farmer (farmer_ID);""",
"""
UPDATE _CSVimport_yield_point_agfiniti
SET field_id = fk.field_id, farmer_id = fk.farmerid
FROM _CSVimport_field_key as fk
WHERE _CSVimport_yield_point_agfiniti.org_file = fk.file_name;"""
)
for command in yield_agfiniti_point_addFields_cursorCommand:
    cursor.execute(command)
print("""Completed populating 'field_id' and 'farmer_id' fields on CSV table: _CSVimport_yield_point_agfiniti.""")
connection.commit()
print("Current time: " + str(datetime.datetime.now()))

print("""Adding 'field_id' and 'farmer_id' fields to CSV table and populating by joining to "field" table based on the original file name: _CSVimport_yield_point_jd.""")
yield_jd_point_addFields_cursorCommand = ("""
ALTER TABLE _CSVimport_yield_point_jd
ADD COLUMN field_ID smallint NULL REFERENCES field (field_id),
ADD COLUMN farmer_id smallint NULL REFERENCES farmer (farmer_ID);""",
"""
UPDATE _CSVimport_yield_point_jd
SET field_id = fk.field_id, farmer_id = fk.farmerid
FROM _CSVimport_field_key as fk
WHERE _CSVimport_yield_point_jd.org_file = fk.file_name;"""
)
for command in yield_jd_point_addFields_cursorCommand:
    cursor.execute(command)
print("""Completed populating 'field_id' and 'farmer_id' fields on CSV table: _CSVimport_yield_point_jd.""")
connection.commit()
print("Current time: " + str(datetime.datetime.now()))

###############################################################################
# Copy/move the two separate, vendor-specific tables of from raw CSV yield data into a final "yield" table

print("""Copying/moving all records from raw CSV yield point files (both John Deere and AgFiniti) to "yield_point" table.""")
yield_point_finaltable_populate_cursorCommand = ("""
INSERT INTO yield_point(longitude,latitude,field,dataset,product,obj__id,track_deg_,swth_wdth,distance_f,duration_s,elevation_,area_count,diff_statu,"time",x_offset_f,y_offset_f,satellites,hding_veh_,diff_statu_1,active_row,vdop,hdop,pdop,crop_flw_m,moisture__,humidity__,air_temp__,grain_temp,soil_temp__,wind_speed,pass_num,yld_mass_d,yld_vol_dr,yld_mass_w,yld_vol_we,speed_mph_,prod_ac_h_,crop_flw_v,date,org_file,file_source,field_ID,farmer_id)
SELECT longitude,latitude,field,dataset,product,obj__id,track_deg_,swth_wdth,distance_f,duration_s,elevation_,area_count, NULL, "time", NULL, y_offset_f, NULL, NULL, NULL, NULL, NULL, NULL, NULL, crop_flw_m, moisture__, humidity__, air_temp__, NULL, soil_temp_, wind_speed, pass_num, yld_mass_d, yld_vol_dr, yld_mass_w, yld_vol_we, speed_mph_, prod_ac_h_, crop_flw_v, date, org_file, file_source, field_id, farmer_id
FROM _CSVimport_yield_point_jd;""",
"""
INSERT INTO yield_point(longitude,latitude,field,dataset,product,obj__id,track_deg_,swth_wdth,distance_f,duration_s,elevation_,area_count,diff_statu,"time",x_offset_f,y_offset_f,satellites,hding_veh_,diff_statu_1,active_row,vdop,hdop,pdop,crop_flw_m,moisture__,humidity__,air_temp__,grain_temp,soil_temp__,wind_speed,pass_num,yld_mass_d,yld_vol_dr,yld_mass_w,yld_vol_we,speed_mph_,prod_ac_h_,crop_flw_v,date,org_file,file_source,field_ID,farmer_id)
SELECT longitude,latitude,field,dataset,product,obj__id,track_deg_,swth_wdth,distance_f,duration_s,elevation_,area_count, diff_statu, "time", x_offset_f, y_offset_f, satellites, hding_veh_, diff_statu_1, active_row, vdop, hdop, pdop, crop_flw_m, moisture__, NULL, NULL, grain_temp, NULL, NULL, pass_num, yld_mass_d, yld_vol_dr, yld_mass_w, yld_vol_we, speed_mph_, prod_ac_h_, crop_flw_v, date, org_file, file_source, field_id, farmer_id
FROM _CSVimport_yield_point_AgFiniti;""")

for command in yield_point_finaltable_populate_cursorCommand:
    cursor.execute(command)
print("""Completed copying/moving all records from raw CSV yield point files (both John Deere and AgFiniti) to "yield_point" table.""")
connection.commit()
print("Current time: " + str(datetime.datetime.now()))

###############################################################################
# Add 'corn' and 'soybean' flags to the planting and yield tables and populate product "products" table populated manually based on unique product names

print("""Adding 'corn' and 'soybean' fields to final yield tables and populating by joining to "products" table based on 'product' and 'source' fields.""")
yield_addCornSoybeanFields_cursorCommand = ("""
ALTER TABLE yield_point
ADD COLUMN corn smallint NULL,
ADD COLUMN soybean smallint NULL;""",
"""
UPDATE yield_point
SET corn = products.corn, soybean = products.soybean
FROM products
WHERE yield_point.product = products.productname AND products.source='yield_point';"""
)
for command in yield_addCornSoybeanFields_cursorCommand:
    cursor.execute(command)
print("""Completed populating 'corn' and 'soybean' fields on yield tables.""")
connection.commit()
print("Current time: " + str(datetime.datetime.now()))


# Close communication with the Postgres database server
cursor.close()
connection.close()


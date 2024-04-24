import pandas as pd 
import numpy as np 
import time 
from dask import dataframe as df1 
  

s_time_dask = time.time() 
dask_df = df1.read_csv('US_Accidents_March23.csv') 
e_time_dask = time.time() 
  
print("Read with dask: ", (e_time_dask-s_time_dask), "seconds") 
  

# rows, columns = dask_df.shape
# rows_computed = rows.compute()
# print(f"Number of rows: {rows_computed}, Number of columns: {columns}")

# print(dask_df.head(10))

# dropped_df = dask_df.drop(['Civil_Twilight'], axis = 1)

###################
import dask.dataframe as dd

# Specify the columns to include
columns_to_include = ['Start_Lat', 'Start_Lng', 'Start_Time']

# Read the CSV file into a Dask DataFrame, including only the specified columns
dask_df = dd.read_csv('US_Accidents_March23.csv', usecols=columns_to_include)

# Filter the DataFrame to include only rows where the Start_Time starts with "2016"
# dask_df = dask_df[dask_df['Start_Time'].str.startswith('2016')]


# Display the first few rows of the DataFrame
# print(dask_df.head())

# Filter the DataFrame to include only rows with a start time before 9 am
dask_df_before_9am = dask_df[dask_df['Start_Time'].str.startswith('2016') & (dask_df['Start_Time'].str[11:13].astype(int) < 9)]

# Filter the DataFrame to include only rows with a start time after 5 pm
dask_df_after_5pm = dask_df[dask_df['Start_Time'].str.startswith('2016') & (dask_df['Start_Time'].str[11:13].astype(int) >= 17)]

# Display the first few rows of each filtered DataFrame
print("Rows with start time before 9 am:")
print(dask_df_before_9am.head())

print("\nRows with start time after 5 pm:")
print(dask_df_after_5pm.head())

#######################
import folium
from folium.plugins import HeatMap
# latitude = 39.865147
# longitude = -84.058723 
latitude = 34.0549
longitude = -118.2426
# Create a map centered around a specific location
m = folium.Map(location=[latitude, longitude], zoom_start=10)

# Access the 'Start_Lat' and 'Start_Lng' columns as Dask series
lat_series = dask_df['Start_Lat']
lon_series = dask_df['Start_Lng']

# Convert Dask series to Dask arrays
lat_array = lat_series.to_dask_array(lengths=True)
lon_array = lon_series.to_dask_array(lengths=True)

# Generate some sample data (latitude, longitude pairs)
data = [[lat_val, lon_val] for lat_val, lon_val in zip(lat_array.compute(), lon_array.compute())]

# Add heatmap layer to the map
HeatMap(data).add_to(m)

# Save the map as an HTML file
# m.save("heatmap.html")
# Output the map locally
m

###################
latitude = 34.0549
longitude = -118.2426
# Create a map centered around a specific location
m = folium.Map(location=[latitude, longitude], zoom_start=10)

# Access the 'Start_Lat' and 'Start_Lng' columns as Dask series
lat_series = dask_df['Start_Lat']
lon_series = dask_df['Start_Lng']

# Convert Dask series to Dask arrays
lat_array = lat_series.to_dask_array(lengths=True)
lon_array = lon_series.to_dask_array(lengths=True)

# Generate some sample data (latitude, longitude pairs)
data = [[lat_val, lon_val] for lat_val, lon_val in zip(lat_array.compute(), lon_array.compute())]

# Add heatmap layer to the map
HeatMap(data).add_to(m)

#####################
# Filter the DataFrame to include only rows with a start time within the 7 am to 9 am window
dask_df_morning = dask_df[
    (dask_df['Start_Time'].str.startswith('2016')) & 
    (dask_df['Start_Time'].str[11:13].astype(int) >= 7) & 
    (dask_df['Start_Time'].str[11:13].astype(int) < 9)
]

# Filter the DataFrame to include only rows with a start time within the 3 pm to 5 pm window
dask_df_afternoon = dask_df[
    (dask_df['Start_Time'].str.startswith('2016')) & 
    (dask_df['Start_Time'].str[11:13].astype(int) >= 15) & 
    (dask_df['Start_Time'].str[11:13].astype(int) < 17)
]

# Display the first few rows of each filtered DataFrame
print("Rows with start time from 7 am to 9 am:")
print(dask_df_morning.head())

print("\nRows with start time from 3 pm to 5 pm:")
print(dask_df_afternoon.head())

#####################
import folium
from folium.plugins import HeatMap

latitude = 34.0549
longitude = -118.2426
# Create a map centered around a specific location
m = folium.Map(location=[latitude, longitude], zoom_start=10)

# Access the 'Start_Lat' and 'Start_Lng' columns as Dask series
lat_series = dask_df['Start_Lat']
lon_series = dask_df['Start_Lng']

# Convert Dask series to Dask arrays
lat_array = lat_series.to_dask_array(lengths=True)
lon_array = lon_series.to_dask_array(lengths=True)

# Generate some sample data (latitude, longitude pairs)
data = [[lat_val, lon_val] for lat_val, lon_val in zip(lat_array.compute(), lon_array.compute())]

# Add heatmap layer to the map
HeatMap(data).add_to(m)
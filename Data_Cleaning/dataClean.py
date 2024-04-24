import pandas as pd 
import numpy as np 
import time 
from dask import dataframe as df1 
  

s_time_dask = time.time() 
dask_df = df1.read_csv('US_Accidents_March23.csv') 
e_time_dask = time.time() 
  
print("Read with dask: ", (e_time_dask-s_time_dask), "seconds") 







#These columns will be present in the new dataframe just add column titles to include more
selected_columns_cleaned_df = dask_df[['ID', 'Start_Lat', 'Start_Lng', 'Start_Time', 'Wind_Speed(mph)', 'Weather_Timestamp', 'Temperature(F)', 'Wind_Chill(F)', 'Humidity(%)', 'Pressure(in)','Visibility(mi)', 'Wind_Direction', 'Precipitation(in)','Weather_Condition', 'Sunrise_Sunset']]
print(selected_columns_cleaned_df.head(10))


#This line drops all NA values in the columns selected above
cleaned_df = selected_columns_cleaned_df.dropna()

nan_count = cleaned_df.isna().sum().compute()
print(f"Total NA Count: {nan_count}")


#uncomment the following lines to make new file
#output_csv_path = 'Cleaned_New_Dataset.csv'


#selected_columns_cleaned_df.to_csv(output_csv_path, single_file=True, index=False)

#print(f"Data written to {output_csv_path}")

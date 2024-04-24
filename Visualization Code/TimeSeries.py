import pandas as pd 
import numpy as np 
import time 
from dask import dataframe as ddf  
import matplotlib.pyplot as plt  


s_time_dask = time.time()
dask_df = ddf.read_csv('Cleaned_New_Dataset.csv')  #
e_time_dask = time.time()
print("Read with dask: ", (e_time_dask - s_time_dask), "seconds")

# Converting Dask DataFrame to Pandas DataFrame
pandas_df = dask_df.compute()

# Convert to datetime object
pandas_df['Start_Time'] = pd.to_datetime(pandas_df['Start_Time'], format='%Y-%m-%d %H:%M:%S', errors='coerce')


pandas_df.sort_values('Start_Time', inplace=True)


accidents_per_period = pandas_df.resample('M', on='Start_Time').size()
plt.figure(figsize=(10, 6)) 
accidents_per_period.plot()
plt.title('Number of Accidents Over Time')
plt.xlabel('Time')
plt.ylabel('Number of Accidents')
plt.xticks(rotation=45)  
plt.tight_layout() 
plt.show()

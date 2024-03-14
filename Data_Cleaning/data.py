import pandas as pd 
import numpy as np 
import time 
from dask import dataframe as df1 
  

s_time_dask = time.time() 
dask_df = df1.read_csv('US_Accidents_March23.csv') 
e_time_dask = time.time() 
  
print("Read with dask: ", (e_time_dask-s_time_dask), "seconds") 
  

rows, columns = dask_df.shape
rows_computed = rows.compute()
print(f"Number of rows: {rows_computed}, Number of columns: {columns}")

print(dask_df.head(10)) 
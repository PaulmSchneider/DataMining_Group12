
import pandas as pd

import datetime

import matplotlib.pyplot as plt

def check_weekday_or_weekend(date):

   try:

       given_date = datetime.datetime.strptime(date, '%Y-%m-%d')

        

       day_of_week = given_date.weekday()

        

       if day_of_week < 5:

           return 'weekday'

       else:

           return 'weekend'

        

   except ValueError as e:

       print(f"Error: {e}")

       return None






df = pd.read_csv('new_dataframe.csv')






wd_data = []

we_data = []




for index, row in df.iterrows():

   

   date = f"{row['Year']}-{row['Month']:02d}-{row['Day']:02d}"

   

   day_type = check_weekday_or_weekend(date)

   

  

   if day_type == 'weekday':

       wd_data.append(row.to_dict())

   elif day_type == 'weekend':

       we_data.append(row.to_dict())






wd_df = pd.DataFrame(wd_data)

we_df = pd.DataFrame(we_data)






print(wd_df.head())




print(we_df.head())




wd_hour_counts = wd_df['Hour'].value_counts().sort_index()

we_hour_counts = we_df['Hour'].value_counts().sort_index()

plt.figure(figsize=(10, 6))

plt.bar(wd_hour_counts.index - 0.2, wd_hour_counts.values/5, width=0.4, label='Weekdays')

plt.bar(we_hour_counts.index + 0.2, we_hour_counts.values/2, width=0.4, label='Weekends')

plt.title('Hourly Accident Count (Weekdays vs. Weekends)')

plt.xlabel('Hour')

plt.ylabel('Count')

plt.xticks(range(24))

plt.legend()

plt.tight_layout()

plt.show()

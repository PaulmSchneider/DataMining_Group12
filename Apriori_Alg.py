import numpy as np 
import pandas as pd


def mask_data(): 
    pd_df = pd.read_csv('Cleaned_New_Dataset.csv')
    new_df = pd_df.drop(['ID', 'Start_Lat', 'Start_Lng', 'Weather_Timestamp', 'Weather_Condition', 'Wind_Direction', 'Start_Time'], axis='columns')


    #https://www.rmets.org/metmatters/beaufort-wind-scale
    new_df['Wind_Speed(mph)'] = new_df['Wind_Speed(mph)'].mask(
            new_df['Wind_Speed(mph)'] <=  12, 'Light Breeze').mask(
                (new_df['Wind_Speed(mph)'] >=  12.1) & (new_df['Wind_Speed(mph)'] <=  18), 'Moderate Breeze').mask(
                    (new_df['Wind_Speed(mph)'] >= 18.1), 'Strong Breeze')  


    new_df['Temperature(F)'] = new_df['Temperature(F)'].mask(
            new_df['Temperature(F)'] <= 32, 'Freezing').mask(
                (new_df['Temperature(F)'] >= 32.1) & (new_df['Temperature(F)'] <= 50), "Cold").mask(
                    (new_df['Temperature(F)'] >= 50.1) & (new_df['Temperature(F)'] <= 75), "Warm").mask(
                        (new_df['Temperature(F)'] >= 75.1), 'Hot') 


    new_df['Wind_Chill(F)'] = new_df['Wind_Chill(F)'].mask(
        (new_df['Wind_Chill(F)'] >= 36.1), 'Safe Wind Chill').mask(
            (new_df['Wind_Chill(F)'] <= 36) & (new_df['Wind_Chill(F)'] >= -17), 'Dangerous Wind Chill').mask(
                (new_df['Wind_Chill(F)'] <= -17.1) & (new_df['Wind_Chill(F)'] >= -40), 'Severe Wind Chill').mask(
                    (new_df['Wind_Chill(F)'] <= -40.1), 'Deadly Wind Chill')  


    #https://www.weather.gov/arx/why_dewpoint_vs_humidity#:~:text=less%20than%20or%20equal%20to,in%20the%20air%2C%20becoming%20oppressive
    new_df['Humidity(%)'] = new_df['Humidity(%)'].mask(
        (new_df['Humidity(%)'] <= 55), 'Dry Humidity').mask(
            (new_df['Humidity(%)'] <= 55.1) & (new_df['Humidity(%)'] >= 65), 'Sticky Humidity').mask(
                (new_df['Humidity(%)'] >= 65.1), 'Wet Humidity') 


    #https://www.maximum-inc.com/learning-center/what-is-atmospheric-pressure-and-how-is-it-measured/
    new_df['Pressure(in)'] = new_df['Pressure(in)'].mask(
        (new_df['Pressure(in)'] <= 29.80), 'Low Pressure').mask(
            (new_df['Pressure(in)'] >= 29.81) & (new_df['Pressure(in)'] <= 30.20), 'Normal Pressure').mask(
                (new_df['Pressure(in)'] >= 30.21), 'High Pressure')


    #https://www.turkishstraits.com/info/visibilitytable
    new_df['Visibility(mi)'] = new_df['Visibility(mi)'].mask(
        (new_df['Visibility(mi)'] <= 2), 'Poor Visibility').mask(
            (new_df['Visibility(mi)'] >= 2.1) & (new_df['Visibility(mi)'] <= 10), 'Moderate Visibility').mask(
                (new_df['Visibility(mi)'] >= 10.1), 'Good Visibility') 

    #https://www.weathershack.com/static/ed-rain-measurement.html#:~:text=Moderate%20rainfall%20measures%200.10%20to,that%20is%20one%20inch%20deep.
    new_df['Precipitation(in)'] = new_df['Precipitation(in)'].mask(
        (new_df['Precipitation(in)'] < 0.10), 'Light Precipitation').mask(
            (new_df['Precipitation(in)'] >= 0.10) & (new_df['Precipitation(in)'] < 0.30), 'Moderate Precipitation').mask(
                (new_df['Precipitation(in)'] >= 0.30), 'Heavy Precipitation')  
    
    print(new_df.head(20))

    Aprioiri_List = new_df.values.tolist()
    
    return Aprioiri_List


from apriori_python import apriori
#source code taken from: https://github.com/chonyy/apriori_python

df_cat = mask_data() 

freqItemSet, rules = apriori(df_cat, minSup=0.6, minConf=0.7)
print("\n")
print(freqItemSet)
print("\n")
print(rules) 


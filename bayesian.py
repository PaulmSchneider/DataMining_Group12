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
    
    new_df['Distance(mi)'] = new_df['Distance(mi)'].mask(
        (new_df['Distance(mi)'] < .25), 'Minor Accident').mask(
            (new_df['Distance(mi)'] >= .25) & (new_df['Distance(mi)'] < 1), 'Major Accident').mask(
                (new_df['Distance(mi)'] >= 1), 'Severe Accident')
    
    print(new_df.head(20))

    #Aprioiri_List = new_df.values.tolist()
    
    return new_df

def bays():

    df_cat = mask_data() 
    # Lets predict the severity of an accident based on Distance(mi).
    # X = (Wind_Speed(mph)= "Strong Breeze", Temperature = "Freezing", "Wind_Chill(F) = "Danger Wind Chill", Humidity = "Wet Humidity", Visibilty = "Poor Visibilty", Sunrise_Sunset = "Night") 

    print("Strong Breeze Frequency: \n",df_cat['Wind_Speed(mph)'].value_counts()['Strong Breeze'])
    print("Freezing Frequency: \n", df_cat['Temperature(F)'].value_counts()['Freezing'])
    print("Dangerous Wind Chill Freqency: \n", df_cat['Wind_Chill(F)'].value_counts()['Dangerous Wind Chill'])
    print("Wet Humidity Freqency: \n",df_cat['Humidity(%)'].value_counts()['Wet Humidity'])
    print("High Pressure Freqency: \n",df_cat['Pressure(in)'].value_counts()['High Pressure'])
    print("Poor Visibility Freqency: \n",df_cat['Visibility(mi)'].value_counts()['Poor Visibility'])
    print("Night Freqency: \n",df_cat['Sunrise_Sunset'].value_counts()['Night']) 

    print("Distance Freqency 'Minor': \n", df_cat['Distance(mi)'].value_counts()['Minor Accident'])
    print("Distance Freqency 'Major': \n", df_cat['Distance(mi)'].value_counts()['Major Accident'])
    print("Distance Freqency 'Severe': \n",  df_cat['Distance(mi)'].value_counts()['Severe Accident'])
   
    print("Sum of Distance: \n", len(df_cat['Distance(mi)']))
    

    # Distance(mi)
    sum_distance = len(df_cat['Distance(mi)'])

    c1 = df_cat['Distance(mi)'].value_counts()['Minor Accident']
    c2 = df_cat['Distance(mi)'].value_counts()['Major Accident']
    c3 = df_cat['Distance(mi)'].value_counts()['Severe Accident']


    # P(Ci) 
    prob_c1 = c1 / sum_distance
    prob_c2 = c2 / sum_distance 
    prob_c3 = c3 / sum_distance
   
    
    #P(x_i | Distance = Minor Accident)
    strong_breeze_1 = len(df_cat[(df_cat['Distance(mi)'] == 'Minor Accident') & (df_cat['Wind_Speed(mph)'] == 'Strong Breeze')])
    freezing_1 = len(df_cat[(df_cat['Distance(mi)'] == 'Minor Accident') & (df_cat['Temperature(F)'] == 'Freezing')])
    dangerous_1 = len(df_cat[(df_cat['Distance(mi)'] == 'Minor Accident') & (df_cat['Wind_Chill(F)'] == 'Dangerous Wind Chill')])
    humidity_1 = len(df_cat[(df_cat['Distance(mi)'] == 'Minor Accident') & (df_cat['Humidity(%)'] == 'Wet Humidity')])
    visibility_1 = len(df_cat[(df_cat['Distance(mi)'] == 'Minor Accident') & (df_cat['Visibility(mi)'] == 'Poor Visibility')])
    night_1 = len(df_cat[(df_cat['Distance(mi)'] == 'Minor Accident') & (df_cat['Sunrise_Sunset'] == 'Night')])

    prob_sb1 = strong_breeze_1 / c1
    prob_f1 = freezing_1 / c1 
    prob_d1 = dangerous_1 / c1 
    prob_h1 = humidity_1 / c1 
    prob_v1 = visibility_1 / c1 
    prob_n1 = night_1 / c1


    #P(x_i | Distance = 'Major Accident')
    strong_breeze_2 = len(df_cat[(df_cat['Distance(mi)'] == 'Major Accident') & (df_cat['Wind_Speed(mph)'] == 'Strong Breeze')])
    freezing_2 = len(df_cat[(df_cat['Distance(mi)'] == 'Major Accident') & (df_cat['Temperature(F)'] == 'Freezing')])
    dangerous_2 = len(df_cat[(df_cat['Distance(mi)'] == 'Major Accident') & (df_cat['Wind_Chill(F)'] == 'Dangerous Wind Chill')])
    humidity_2 = len(df_cat[(df_cat['Distance(mi)'] == 'Major Accident') & (df_cat['Humidity(%)'] == 'Wet Humidity')])
    visibility_2 = len(df_cat[(df_cat['Distance(mi)'] == 'Major Accident') & (df_cat['Visibility(mi)'] == 'Poor Visibility')])
    night_2 = len(df_cat[(df_cat['Distance(mi)'] == 'Major Accident') & (df_cat['Sunrise_Sunset'] == 'Night')])

    prob_sb2 = strong_breeze_2 / c2 
    prob_f2 = freezing_2 / c2 
    prob_d2 = dangerous_2 / c2 
    prob_h2 = humidity_2 / c2
    prob_v2 = visibility_2 / c2
    prob_n2 = night_2 / c2 


    #P(x_i | Distance = 'Severe Accident')
    strong_breeze_3 = len(df_cat[(df_cat['Distance(mi)'] == 'Severe Accident') & (df_cat['Wind_Speed(mph)'] == 'Strong Breeze')])
    freezing_3 = len(df_cat[(df_cat['Distance(mi)'] == 'Severe Accident') & (df_cat['Temperature(F)'] == 'Freezing')])
    dangerous_3 = len(df_cat[(df_cat['Distance(mi)'] == 'Severe Accident') & (df_cat['Wind_Chill(F)'] == 'Dangerous Wind Chill')])
    humidity_3 = len(df_cat[(df_cat['Distance(mi)'] == 'Severe Accident') & (df_cat['Humidity(%)'] == 'Wet Humidity')])
    visibility_3 = len(df_cat[(df_cat['Distance(mi)'] == 'Severe Accident') & (df_cat['Visibility(mi)'] == 'Poor Visibility')])
    night_3 = len(df_cat[(df_cat['Distance(mi)'] == 'Severe Accident') & (df_cat['Sunrise_Sunset'] == 'Night')])

    prob_sb3 = strong_breeze_3 / c3
    prob_f3 = freezing_3 / c3
    prob_d3 = dangerous_3 / c3
    prob_h3 = humidity_3 / c3
    prob_v3 = visibility_3 / c3 
    prob_n3 = night_3 / c3  


    #P(X | Distance = 'Minor')
    prob_x1 = prob_v1 * prob_sb1 
    #prob_sb1 * prob_f1 * prob_d1 * prob_h1 * prob_v1 * prob_n1
    
    #P(X | Distance = 'Major')
    prob_x2 = prob_v2 * prob_sb2 
   # prob_sb2 * prob_f2 * prob_d2 * prob_h2 * prob_v2 * prob_n2

    #P(X | Distance = 'Severe')
    prob_x3 = prob_v3 * prob_sb3 
    #prob_sb3 * prob_f3 * prob_d3 * prob_h3 * prob_v3 * prob_n3 


    #P(X | Ci) P(Ci)

    #P(X | Distance = 'Minor') * P(Distance = 'Minor')
    prob_sev1 = prob_x1 * prob_c1

    #P(X | Distance = 'Major') * P(Distance = 'Major')
    prob_sev2 = prob_x2 * prob_c2

    #P(X | Distance = 'Severe') * P(Distance = 'Severe')
    prob_sev3 = prob_x3 * prob_c3




    print("The Probablity of condition X with a Distance(mi) of Minor Accident is: ", prob_sev1)
    print("The Probablity of condition X with a Distance(mi) of Major Accident is: ", prob_sev2)
    print("The Probablity of condition X with a Distance(mi) of Severe Accident is: ", prob_sev3)

bays() 
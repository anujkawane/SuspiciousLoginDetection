import random
import os, numpy as np
import socket, uuid, json, random, time, os, pyspark.pandas as ps
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import math

deviceTypes = ['Android', 'IOS', 'Windows', 'Mac OS']
PATH = os.getcwd()+ '/DataFiles'

def generateMainDF():
    spark = SparkSession.builder.appName('GenerateDF').getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('ERROR')

    ps.set_option('compute.ops_on_diff_frames', True)

    ip_df = ps.read_csv(PATH+'/geolite2-city-ipv4.csv')

    # Top cities in US
    cities = ps.read_csv(PATH+'/cities.csv')
    cities = cities.sort_values(by=['Population'], ascending=False).head(100)

    row_zero = ps.DataFrame(
        {'IP': ip_df.columns[0], 'IP_1': ip_df.columns[1], 'Country': ip_df.columns[2], 'State': ip_df.columns[3],
         'none1': ip_df.columns[4], 'city': ip_df.columns[5], '6': ip_df.columns[6], '7': ip_df.columns[7],
         '8': ip_df.columns[8], '9': ip_df.columns[9]}, index=[0])
    ip_df = ip_df.rename(
        columns={"1.0.0.0": "IP", "1.0.0.255": "IP_1", 'AU': 'Country', '_c3': 'State', '_c4': 'none1', '_c5': 'city',
                 '_c6': '6', '-33.4940': '7', '143.2104': '8', 'Australia/Sydney': '9'})
    ip_city_map = ps.concat([row_zero, ip_df])
    ip_city_map = ip_city_map[ip_city_map['Country'] == 'US']
    ip_city_map = ip_city_map.drop(columns=['IP_1', 'none1', '6', '7', '8', '9'])
    cities_list = cities['City'].to_list()

    ip_city_map = ip_city_map.loc[ip_city_map['city'].isin(cities_list)]
    ip_city_map = ip_city_map.groupby(['city']).head(10)

    locations = ip_city_map['city'].unique().to_list() # get locations from city list
    main_data = []
    for i in range(100):
        userId = str(random.randint(10 ** (len(str(34567)) - 1), 99999))
        location = locations[random.randint(0, len(locations) - 1)]
        random.shuffle(deviceTypes)
        device = ','.join(deviceTypes[0:2])
        curr_time = int(time.time())
        main_data.append([userId, location, device, curr_time])

    user_data_df = ps.DataFrame(np.array(main_data), columns=['UserID', 'Location', 'DeviceType', 'Time'])



    userDataFile = user_data_df.to_pandas()
    userDataFile.to_csv(PATH + '/User_login_history.csv', index=False)

    geoLoc = ip_city_map.to_pandas()
    geoLoc.to_csv(PATH+'/IPv4_Location_Mapping.csv', index = False)

generateMainDF()

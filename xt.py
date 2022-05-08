import json, time
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
import pyspark.pandas as ps
from pyspark.sql.functions import *

KAFKA_HOST = 'localhost:29092'
TOPIC_SERVER_LOGS = 'server_logs'

PANDA = ps.DataFrame({'USER_ID': ['91669', '35004', '83542', '95642'],
        'LOCATION': ['USA', 'USA', 'USA', 'INDIA'],
        'DEVICE_TYPE': ['Android', 'IOS', 'IOS', 'WINDOWS'],
        'TIMESTAMP': ['1651963313','1651897851', '1651897852', '1651897853'],
        })

geoLocationMap = ps.DataFrame({'IP': ['1', '2', '3', '4'],
        'LOCATION': ['USA', 'USA', 'USA', 'INDIA']
        })

def updateTimeStamp(user_details,current_logged_time):
    global PANDA
    PANDA = PANDA[PANDA['USER_ID'] != user_details['USER_ID'].to_list()[0] ]
    user_details = user_details.iloc[0,:].to_list()
    user_details[-1]  = str(current_logged_time)
    # user_details = ps.Series(user_details)
    user_details = ps.DataFrame({'USER_ID': [user_details[0]],
                  'LOCATION': [user_details[1]],
                  'DEVICE_TYPE': [user_details[2]],
                  'TIMESTAMP': [user_details[3]],
                  })
    PANDA = PANDA.append(user_details,ignore_index= True)


def getLocationFromIP(ip_address):
    locationIp = geoLocationMap[geoLocationMap['IP'] == 1]
    location = str(locationIp['LOCATION'].values[0])
    return location


def processData(message):
    list = message.split(",")
    user_login = PANDA[PANDA['USER_ID'].str.contains(list[-1])]

    if len(user_login) > 0:
        last_logged_time = int(user_login['TIMESTAMP'].values[0])
        current_logged_time = int(time.time())
        if current_logged_time - last_logged_time < 10 * 60:
            last_location = user_login['LOCATION'].values[0]
            current_logged_location = getLocationFromIP('1')
            if last_location == current_logged_location:
                last_Logged_device = user_login['DEVICE_TYPE'].values[0]
                current_logged_device = "Android"
                if last_Logged_device == current_logged_device:
                    updateTimeStamp(user_login,current_logged_time)

                    print("Valid Login")
                else:
                    print("Produced Device type sign in alert to topic")
            else:
                print("Produced Someone looged into your account from - Location ", current_logged_location)
        else:
            updateTimeStamp(user_login, current_logged_time)
            print("Login Done after required time limit update Time")


if __name__ == '__main__':
    # Kafka Consumer
    consumer = KafkaConsumer(
        TOPIC_SERVER_LOGS,
        bootstrap_servers=KAFKA_HOST,
        auto_offset_reset='earliest'
    )
    temp = 'a721eead-bccc-4b09-bf8a-46834f32515c,1651963313.443485,USA,login,91669'
    # for message in consumer:
    #     # print(json.loads(message.value))
    #     processData(temp)
    #     print ()
    processData(temp)



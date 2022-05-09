import os, pyspark.pandas as ps

PATH = os.getcwd()+ '/DataFiles'
User_Login_History = ps.read_csv(PATH + '/User_login_history.csv')
IPv4_Location_Mapping = ps.read_csv(PATH + '/IPv4_Location_Mapping.csv')

print(User_Login_History)
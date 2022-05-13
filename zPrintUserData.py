import sys, socket, json, os, time, pyspark.pandas as ps

PATH = os.getcwd()+ '/DataFiles'
User_Login_History = ps.read_csv(PATH + '/User_login_history.csv')

print(User_Login_History)
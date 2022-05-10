import os, pyspark.pandas as ps

PATH = os.getcwd()+ '/DataFiles'
User_Login_History = ps.read_csv(PATH + '/User_login_history.csv')
IPv4_Location_Mapping = ps.read_csv(PATH + '/IPv4_Location_Mapping.csv')

print(User_Login_History)


def writeLogs(request):
    if path.isfile('transactions/' + latest_file_datetime + ".json"):
        to_dump = json.loads(open('transactions/' + latest_file_datetime + '.json', "r+").read())
    else:
        to_dump = []

    to_dump.append(testing_set_results[index])

    f = open('transactions/' + latest_file_datetime + '.json', "w+")
    json.dump(to_dump, f)
    f.close()
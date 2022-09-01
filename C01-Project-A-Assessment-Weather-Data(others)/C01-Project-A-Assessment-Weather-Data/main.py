from model import UserModel, DeviceModel, WeatherDataModel, DailyReportModel
from datetime import datetime

# change these variable as per required input.
userName = 'admin'
userNameToSearch = 'user_1'
deviceIdToBeSearched = 'DT001'


# collections Data Model initialization
user_coll = UserModel(userName)
device_coll = DeviceModel()
weather_coll = WeatherDataModel()
reports_coll = DailyReportModel()


print("\nDoes "+userName+" have admin access?")
user_document = user_coll.find_if_asked_role_present(userName,'admin')
if (user_document):
    print(True)
else:
    print(False)


print("\n\nIs username based query possible for "+userName+"? Trying to search "+userNameToSearch)
user_document = user_coll.find_by_username(userNameToSearch)
if (user_document):
    print(user_document)
else: 
    print("Query failed, Admin access required!")    

print("\n\nCan "+userName+" add a new user? Trying to insert user_3")
user_document = user_coll.insert('user_3', 'user_3@example.com', 'default')
if(user_document == -1):
    print(user_coll.latest_error)
else:    
    print(user_document)


print("\n\nCan "+userName+" access device "+deviceIdToBeSearched+" ?")
if(user_coll.can_read_device_data(deviceIdToBeSearched)):
    device_doc = device_coll.find_by_device_id(deviceIdToBeSearched)
    if(device_doc):
       print(device_doc)
    else:
       print("device information for "+deviceIdToBeSearched+ " not present")    
else: 
    print("Read access not allowed to "+deviceIdToBeSearched)    

deviceToBeCreated = 'DT201'

print("\n\nCan "+userName+" create device "+deviceToBeCreated+" ?")
if(user_coll.find_if_asked_role_present(userName,'admin')):
    device_doc = device_coll.insert('DT201', 'Temperature Sensor', 'Temperature', 'Acme')
    if(device_doc != -1):
        print(device_doc)
    else:
        print(device_coll.latest_error)
else:
    print("Insert failed, Admin access required!")


print("\n\nCan "+userName+" read "+deviceIdToBeSearched+" device data?")
if(user_coll.can_read_device_data(deviceIdToBeSearched)):
    device_doc = device_coll.find_by_device_id(deviceIdToBeSearched)
    weather_doc = weather_coll.find_by_device_id_and_timestamp(deviceIdToBeSearched,datetime(2020, 12, 2, 13, 30, 0))
    if(weather_doc):
       print(weather_doc)
    else:
       print("device information for "+deviceIdToBeSearched+ " not present")    
else: 
    print("Read access not allowed for "+deviceIdToBeSearched)


print("\n\nCan "+userName+" write to "+deviceIdToBeSearched+" device data?")
if(user_coll.can_write_device_data(deviceIdToBeSearched)):
    print("Write access is allowed for "+deviceIdToBeSearched)    
else: 
    print("Write access not allowed for "+deviceIdToBeSearched)


print("\n\naggregated weather data")
result = weather_coll.get_aggregated_data()
if(result):
    print("BULK INSERTING THE SAME......")
    for row in result:
        data = reports_coll.insert(row.get('_id').get('device_id'),row.get('_id').get('timestamp'),row.get('min'),row.get('max'),row.get('avg'))
        if(data != -1):
            print(data)
        else:
            print(reports_coll.latest_error)
else:
    print("NO DATA FOUND in Weather DB for aggregation")


startDate = '2020-12-01'
endDate = '2020-12-05'

print("\n\nSearching reports for "+deviceIdToBeSearched+" between dates "+startDate+" and "+endDate)
result = reports_coll.get_all_data_for_device_id_between_dates(deviceIdToBeSearched, startDate, endDate)
if(result):
    for row in result:
        print(row)
else:
    print("NO DATA FOUND")
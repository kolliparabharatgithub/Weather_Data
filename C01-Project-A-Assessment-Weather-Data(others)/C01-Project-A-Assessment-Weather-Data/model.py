# Imports Database class from the project to provide basic functionality for database access
from database import Database
# Imports ObjectId to convert to the correct format before querying in the db
from bson.objectid import ObjectId


# User document contains username (String), email (String), and role (String) fields
class UserModel:
    USER_COLLECTION = 'users'

    def __init__(self, username):
        self._db = Database()
        self._username = username
        document = self.__find({'username':username})
        self._adminAccess = document['role'] == 'admin'
        self._latest_error = ''

    # Latest error is used to store the error string in case an issue. It's reset at the beginning of a new function call
    @property
    def latest_error(self):
        return self._latest_error
    
    def __checkAccess(self):
        if(self._adminAccess): 
            return True
        return False

    # Since username should be unique in users collection, this provides a way to fetch the user document based on the username
    def find_by_username(self, username):
        if(self.__checkAccess()):
           return self.__find({'username': username})
    
    # Finds a document based on the unique auto-generated MongoDB object id 
    def find_by_object_id(self, obj_id):
         if(self.__checkAccess()):
             return self.__find({'_id': ObjectId(obj_id)})
    
    # Private function (starting with __) to be used as the base for all find functions
    def __find(self, key):
        user_document = self._db.get_single_data(UserModel.USER_COLLECTION, key)
        return user_document

    # fun for getting all documents
    def get_all_users(self):
        if(self.__checkAccess()):
            return self._db.get_all_documents(UserModel.USER_COLLECTION)


    # Finds if a user has a role passed as paramter based on name
    def find_if_asked_role_present(self, username, role):
         if(self.__checkAccess()):        
             return self.__find({'username':username,'role':role})
   
    # Finds if user has access to device data for reading
    def can_read_device_data(self, deviceId):
        doc = self.__find({'username': self._username, 'alist.did': deviceId})
        if(doc):
            return True
        return False

    # Finds if user has access to device data for writing
    def can_write_device_data(self, deviceId):
        doc = self.__find({'username': self._username,'alist': { 'did':deviceId, 'atype': 'rw'}})
        if(doc):
            return True
        return False


    # This first checks if a user already exists with that username. If it does, it populates latest_error and returns -1
    # If a user doesn't already exist, it'll insert a new document and return the same to the caller
    def insert(self, username, email, role):
        if(self.__checkAccess()):
            self._latest_error = ''
            user_document = self.find_by_username(username)
            if (user_document):
               self._latest_error = f'Username {username} already exists'
               return -1
        
            user_data = {'username': username, 'email': email, 'role': role}
            user_obj_id = self._db.insert_single_data(UserModel.USER_COLLECTION, user_data)
            return self.find_by_object_id(user_obj_id)


# Device document contains device_id (String), desc (String), type (String - temperature/humidity) and manufacturer (String) fields
class DeviceModel:
    DEVICE_COLLECTION = 'devices'

    def __init__(self):
        self._db = Database()
        self._latest_error = ''
    
    # Latest error is used to store the error string in case an issue. It's reset at the beginning of a new function call
    @property
    def latest_error(self):
        return self._latest_error
    
    # Since device id should be unique in devices collection, this provides a way to fetch the device document based on the device id
    def find_by_device_id(self, device_id):
        key = {'device_id': device_id}
        return self.__find(key)
    
    # Finds a document based on the unique auto-generated MongoDB object id 
    def find_by_object_id(self, obj_id):
        key = {'_id': ObjectId(obj_id)}
        return self.__find(key)
    
    # Private function (starting with __) to be used as the base for all find functions
    def __find(self, key):
        device_document = self._db.get_single_data(DeviceModel.DEVICE_COLLECTION, key)
        return device_document
    
    # This first checks if a device already exists with that device id. If it does, it populates latest_error and returns -1
    # If a device doesn't already exist, it'll insert a new document and return the same to the caller
    def insert(self, device_id, desc, type, manufacturer):
        self._latest_error = ''
        device_document = self.find_by_device_id(device_id)
        if (device_document):
            self._latest_error = f'Device id {device_id} already exists'
            return -1
        
        device_data = {'device_id': device_id, 'desc': desc, 'type': type, 'manufacturer': manufacturer}
        device_obj_id = self._db.insert_single_data(DeviceModel.DEVICE_COLLECTION, device_data)
        return self.find_by_object_id(device_obj_id)


# Weather data document contains device_id (String), value (Integer), and timestamp (Date) fields
class WeatherDataModel:
    WEATHER_DATA_COLLECTION = 'weather_data'

    def __init__(self):
        self._db = Database()
        self._latest_error = ''
    
    # Latest error is used to store the error string in case an issue. It's reset at the beginning of a new function call
    @property
    def latest_error(self):
        return self._latest_error
    
    # Since device id and timestamp should be unique in weather_data collection, this provides a way to fetch the data document based on the device id and timestamp
    def find_by_device_id_and_timestamp(self, device_id, timestamp):
        key = {'device_id': device_id, 'timestamp': timestamp}
        return self.__find(key)

    # finds all info related to device id in the weather db

    def find_weather_info_by_device_id(self, device_id):
        key = {'device_id': device_id}
        return self.__findAll(key)
    
    # Finds a document based on the unique auto-generated MongoDB object id 
    def find_by_object_id(self, obj_id):
        key = {'_id': ObjectId(obj_id)}
        return self.__find(key)
    
    # Private function (starting with __) to be used as the base for all find functions
    def __find(self, key):
        wdata_document = self._db.get_single_data(WeatherDataModel.WEATHER_DATA_COLLECTION, key)
        return wdata_document

            
    # Private function (starting with __) to be used as the base for all find functions
    def __findAll(self, key):
        wdata_document = self._db.get_all_data(WeatherDataModel.WEATHER_DATA_COLLECTION, key)
        return wdata_document
    
    # This first checks if a data item already exists at a particular timestamp for a device id. If it does, it populates latest_error and returns -1.
    # If it doesn't already exist, it'll insert a new document and return the same to the caller
    def insert(self, device_id, value, timestamp):
        self._latest_error = ''
        wdata_document = self.find_by_device_id_and_timestamp(device_id, timestamp)
        if (wdata_document):
            self._latest_error = f'Data for timestamp {timestamp} for device id {device_id} already exists'
            return -1
        
        weather_data = {'device_id': device_id, 'value': value, 'timestamp': timestamp}
        wdata_obj_id = self._db.insert_single_data(WeatherDataModel.WEATHER_DATA_COLLECTION, weather_data)
        return self.find_by_object_id(wdata_obj_id)

    #This method returns aggregated data : max,min,avg reading per day per device
    def get_aggregated_data(self):
        return self._db.get_all_aggregated_data(WeatherDataModel.WEATHER_DATA_COLLECTION,
        [
            { '$group': { '_id': { 'device_id':'$device_id', 'timestamp': {'$dateToString': {'format': "%Y-%m-%d", 'date': '$timestamp'}} }, 'max': { '$max': "$value" }, 'min': { '$min': "$value" }, 'avg': { '$avg': "$value" } } }
        ]
    )

# Reports Collections for daily aggregated data.
class DailyReportModel:
    REPORTS_DATA_COLLECTION = 'reports_data'

    def __init__(self):
        self._db = Database()
        self._latest_error = ''
    
    # Latest error is used to store the error string in case an issue. It's reset at the beginning of a new function call
    @property
    def latest_error(self):
        return self._latest_error

    def get_all_data_by_device_id(self,deviceId):
        return self.__findAll({'did':deviceId})

    def get_all_data_for_device_id_between_dates(self,deviceId,timestamStart,timestampEnd):
        return self._db.get_all_aggregated_data(
            DailyReportModel.REPORTS_DATA_COLLECTION,
            [{ "$match": { "did":deviceId, "timestamp": { "$gte": timestamStart, "$lte": timestampEnd } }}]
            )

    def get_data_by_device_id_and_time_stamp(self,deviceId,timestamp):
        return self.__find({'did':deviceId, 'timestamp': timestamp})
     
    def get_all_data_by_time_stamp(self,timestamp):
        return self.__findAll({'timestamp':timestamp})

    # Finds a document based on the unique auto-generated MongoDB object id 
    def find_by_object_id(self, obj_id):
        key = {'_id': ObjectId(obj_id)}
        return self.__find(key) 
        
    # Private function (starting with __) to be used as the base for all find functions
    def __find(self, key):
        return self._db.get_single_data(DailyReportModel.REPORTS_DATA_COLLECTION, key)
         
    # Private function (starting with __) to be used as the base for all find functions
    def __findAll(self, key):
        return self._db.get_all_data(DailyReportModel.REPORTS_DATA_COLLECTION, key)
    
    # This method is used to saved the aggregations 
    def insert(self, device_id, timestamp,min,max,avg):
        self._latest_error = ''
        rdata_document = self.get_data_by_device_id_and_time_stamp(device_id, timestamp)
        if (rdata_document):
            self._latest_error = f'Data for timestamp {timestamp} for device id {device_id} already exists'
            return -1
        
        report_data = {'did': device_id, 'timestamp': timestamp, 'min': min, 'max': max, 'avg': avg}
        rdata_obj_id = self._db.insert_single_data(DailyReportModel.REPORTS_DATA_COLLECTION, report_data)
        return self.find_by_object_id(rdata_obj_id)
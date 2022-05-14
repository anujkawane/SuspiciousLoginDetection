import enum, pickle,json

# Used to map server logs to JSON
class ServerLog():

    def __init__(self, requestID, timestamp, currentLocation, deviceType, UserID):
        self.requestID = requestID
        self.timestamp = timestamp
        self.currentLocation = currentLocation
        self.deviceType = deviceType
        self.UserID = UserID

    def serialize(self, obj):
        return pickle.dumps(obj)

    def desearilize(self, serizliedObj):
        return pickle.loads(serizliedObj)

    def toJSON(self):
        data = { "RequestID":str(self.requestID), "UserID":str(self.UserID), "Timestamp":str(self.timestamp), "currentIP":str(self.currentLocation), "DeviceType":str(self.deviceType)}
        return data
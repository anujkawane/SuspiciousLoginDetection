import enum, pickle
class ServerLogEnum():

    def __init__(self, eventId, timestamp, currentCountry, currentEventType, accountId):
        self.eventId = eventId
        self.timestamp = timestamp
        self.currentCountry = currentCountry
        self.currentEventType = currentEventType
        self.accountId = accountId


    def serialize(self, obj):
        return pickle.dumps(obj)

    def desearilize(self, serizliedObj):
        return pickle.loads(serizliedObj)

    def returnCommaSeparated(self):
        return self.eventId + ',' + \
               self.timestamp + ',' + \
               self.currentCountry + ',' + \
               self.currentEventType + ',' + \
               self.accountId

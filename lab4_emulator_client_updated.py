# Import SDK packages
from random import randint
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import json
import pandas as pd
import numpy as np
import random
import csv

#Starting and end index, modify this
device_st = 0
device_end = 5

certificate_formatter = "vehicle0_certs/d0a3fc8f256f472e92faebf6a66d4bb437ae491656b90faa4f512070612960f2-certificate.pem.crt"
key_formatter = "vehicle0_certs/d0a3fc8f256f472e92faebf6a66d4bb437ae491656b90faa4f512070612960f2-private.pem.key"

vehicles = ["vehicle0", "vehicle1", "vehicle2", "vehicle3", "vehicle4"]
vehicle_file = ["vehicle0.csv", "vehicle1.csv", "vehicle2.csv", "vehicle3.csv", "vehicle4.csv"]
vehicle_data = []

class MQTTClient:
    def __init__(self, device_id, cert, key):
        # For certificate based connection
        self.device_id = str(device_id)
        self.state = 0
        self.client = AWSIoTMQTTClient(self.device_id)
        self.client.configureEndpoint("a2v9x3lekav88e-ats.iot.us-east-1.amazonaws.com", 8883)
        self.client.configureCredentials("vehicle0_certs/AmazonRootCA1.pem", key, cert)
        self.client.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
        self.client.configureDrainingFrequency(2)  # Draining: 2 Hz
        self.client.configureConnectDisconnectTimeout(10)  # 10 sec
        self.client.configureMQTTOperationTimeout(5)  # 5 sec
        self.client.onMessage = self.customOnMessage
        

    def customOnMessage(self,message):
        # Print message from process emissions or self publish
        if message.topic == "emission/all/" + self.device_id:
            print("vehicle = {}, Max_Emission = {}".format(self.device_id, json.loads((message.payload).decode('UTF-8'))[self.device_id]))
        else:
            print("client {} received payload {} from topic {}".format(self.device_id, message.payload, message.topic))

    # Suback callback
    def customSubackCallback(self,mid, data):
        #You don't need to write anything here
        pass

    # Puback callback
    def customPubackCallback(self,mid):
        #You don't need to write anything here
        pass

    def publish(self, Payload="payload"):
        # # Subscribe back to self
        # self.client.subscribeAsync("emissions/" + self.device_id, 0, ackCallback=self.customSubackCallback)

        # Subscribe to process emissions
        self.client.subscribeAsync("emission/all/" + self.device_id, 0, ackCallback=self.customSubackCallback)
        
        # Publish emission data from csv
        self.client.publishAsync("emissions/" + self.device_id, Payload, 0, ackCallback=self.customPubackCallback)

print("Loading vehicle data...")
for i in range(len(vehicle_file)):
    with open("/home/pi/Downloads/" + vehicle_file[i], 'r') as file:
        reader = csv.reader(file)
        temp = []
        for num, row in enumerate(reader):
            temp.append(row[2])

        # Each row is time based emission data for vehicle
        vehicle_data.append(temp)


print("Initializing MQTTClients...")
clients = []
for device_id in vehicles:
    client = MQTTClient(device_id, certificate_formatter.format(device_id,device_id), key_formatter.format(device_id,device_id))
    client.client.connect()
    clients.append(client)
 
count = 1

while True:
    print("Number of clients: {}".format(len(clients)))

    for i,c in enumerate(clients):
        c.publish(json.dumps( {"vehicle_id": str(i), "data": str(vehicle_data[i][count])} ))

        count += 1

    time.sleep(5)

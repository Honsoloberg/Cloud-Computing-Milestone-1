from google.cloud import pubsub_v1
import glob
import json
import csv
import os 
import random
import numpy as np                      
import time

files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0]

project_id="canvas-griffin-485115-v8"
topic_name = "smartMeter"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Published messages with ordering keys to {topic_path}.")

# CSV FORMAT
# 0: time || 1: profileName || 2: temperature || 3: humidity || 4: pressure

try:
    with open("Labels.csv", 'r') as file:
        csvFile = csv.DictReader(file)

        for line in csvFile:

            message = json.dumps(line)

            # print("")
            # print(message)
            # print("")

            bMessage = message.encode("utf-8")

            try:    
                
                future = publisher.publish(topic_path, bMessage)
                
                future.result()    
                print("The messages {} has been published successfully".format(message))
            except: 
                print("Failed to publish the message")
            
            time.sleep(.5)
            # break 
except KeyboardInterrupt:
    publisher.stop()
    exit(0)

publisher.stop()
from google.cloud import pubsub_v1
import glob
import json
import csv
import os                      
from time import sleep

files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0]

project_id="canvas-griffin-485115-v8" # Needs to be changed when used in different Projects
topic_name = "weather-data"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Published messages with ordering keys to {topic_path}.")

try:
    with open("Labels.csv", 'r') as file:
        csvFile = csv.DictReader(file)

        for line in csvFile:
            # Convert an individual row in the CSV into JSON
            message = json.dumps(line)

            # Serialize the JSON string
            bMessage = message.encode("utf-8")

            try:    
                
                # Publish the binary message to the google cloud topic
                future = publisher.publish(topic_path, bMessage)
                
                # wait for the transaction to complete
                future.result()    
                print("The messages {} has been published successfully".format(message))
            except: 
                print("Failed to publish the message")
            
            sleep(.5) # wait is to ensure no messages are missed in sending. As the "future.result()" can still fail.

            # Uncomment the below line to send only one line (for testing)
            break
except KeyboardInterrupt:
    #if the user wants to manually stop the script. Catch the the Interrupt then close the script.
    publisher.stop()
    exit(0)

#if the script completes then stop the publisher
publisher.stop()
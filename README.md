### Milestone 1: Data Ingestion System (Cloud Pub/Sub)
## SOFE4630U-MS1

# Objectives
1. Understand the role of the Data Ingestion System in Event Driven Architecture.
2. Get familiar with Google Pub/Sub and its terminologies.
3. Be able to create topics, producers, and consumers via GUI.
4. Create producers and consumers with a Python script.

# Content
1. Labels.csv
2. Producer.py
    - Iterates over the "Labels.csv" and formats the data into a JSON format.
    - Publishes the JSON objects to a google cloud topic
3. Consumer.py
    - Subscribes to a google cloud topic and consumes the records then formats them to the terminal
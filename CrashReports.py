
# coding: utf-8

# In[1]:

import requests
import pandas as pd
import os
import xml.etree.ElementTree as ET
import gzip
import time
import tweepy


consumer_key = os.environ.get("consumer_key")
consumer_secret = os.environ.get("consumer_secret")
access_token = os.environ.get("access_token")
access_token_secret = os.environ.get("access_token_secret")

# Setup Tweepy API Authentication
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
API = tweepy.API(auth, parser=tweepy.parsers.JSONParser())


def download():
  
    r = requests.get('http://data.dot.state.mn.us/iris_xml/incident.xml.gz')
    with open('incident.xml', 'w') as handle:
        handle.write(gzip.decompress(r.content).decode('utf-8'))
    print("Downloading Traffic Data")


def data_check(XMLfile):
    try:
        with open('crash_data.csv', 'r') as CD:
            parse(XMLfile)
    except FileNotFoundError:
            All_Crash_Data = pd.DataFrame(columns=['Name', 'Date', 'Direction', 'Road', 'Location', 'Event'])
            with open('crash_data.csv', 'w') as f:
                All_Crash_Data.to_csv(f, header=True)
            parse(XMLfile)



def parse(XMLfile):
    with open('crash_data.csv', 'r') as CD:
           All_Crash_Data = pd.read_csv(CD)

    dates = []
    incident_dirs = []
    roads = []
    locations = []
    names = []
    events = []
    
    parsedXML = ET.parse(XMLfile)
    root = parsedXML.getroot()

    for child in root:
        
        if child.attrib['name'] not in All_Crash_Data['Name']:
            try:
                date = child.attrib['event_date']
                dates.append(date)
            except KeyError:
                dates.append("none")
            try:
                name = str(child.attrib['name'])
                names.append(name)
            except KeyError:
                name.append("none")
            try:
                direction = child.attrib['dir']
                incident_dirs.append(direction)
            except KeyError:
                incident_dir.append("none")
            try:
                road = child.attrib['road']
                roads.append(road)
            except KeyError:
                roads.append('None')

            try:
                location = child.attrib['location']
            except KeyError:
                location = "none"
            locations.append(location)
           
            try: 
                event = child.attrib['event_type'].split("_", 1)
                event = event[1]
                events.append(event)
            except KeyError:
                events.append("none")
            print(event)
            if event == 'HAZARD' or 'CRASH':
                update_str = "{} reported at {} {} {}, Data From MNDOT Traffic".format(event, direction, road, location)
                try:
                    API.update_status(update_str)
                except tweepy.error.TweepError as t:
                    print(t)

    DF = pd.DataFrame({"Name" : names,
                       "Date" : dates,
                       "Direction": incident_dirs,
                       "Road" : roads,
                       "Location" : locations,
                       "Event" : events})
    
    print(DF)


    with open('crash_data.csv', 'a') as f:
        DF.to_csv(f, header=False)




while True:
    download()
    data_check("incident.xml")
    print("MDOT checked, waiting 30min")
    time.sleep(1800)


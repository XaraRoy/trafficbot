
# coding: utf-8

# In[1]:


import pandas as pd
import os
import xml.etree.ElementTree as ET
import gzip
import time
import shutil
import urllib


# In[2]:


def download():
    url = "http://data.dot.state.mn.us/iris_xml/incident.xml.gz"
    content = urllib.request.urlopen(url)
    output = open("incidents.XML.gz", "wb")
    output.write(content.read())
    output.close()
    print("Downloading Incident Data")


# In[3]:


def unzip(file, target):
    unzipa(file, target)
    unzipb(file, target)


# In[4]:


def unzipa(file, target):
    attempts = 0 
    try:
        with gzip.open(file, 'rb') as f_in:
            with open(target, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    except FileNotFoundError:
        print("File not found, retrying in 10 secs")
        time.sleep(10)


# In[5]:


def unzipb(file, target):
    attempts = 0 
    try:
        with gzip.open(file, 'rb') as f_in:
            with open(target, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    except FileNotFoundError:
        print("File not found, giving up")


# In[6]:


def data_check(XMLfile):
    try:
        with open('crash_data.csv', 'r') as CD:
            parse(XMLfile)
    except FileNotFoundError:
            All_Crash_Data = pd.DataFrame(columns=['Name', 'Date', 'Direction', 'Road', 'Location', 'Event'])
            with open('crash_data.csv', 'w') as f:
                All_Crash_Data.to_csv(f, header=True)
            parse(XMLfile)


# In[7]:


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

        if child.attrib['name'] not in str(All_Crash_Data['Name']):
            try:
                dates.append(child.attrib['event_date'])
            except KeyError:
                dates.append("none")
            try:
                names.append(str(child.attrib['name']))
            except KeyError:
                name.append("none")
            try:
                incident_dirs.append(child.attrib['dir'])
            except KeyError:
                incident_dir.append("none")
            try:
                roads.append(child.attrib['road'])
            except KeyError:
                roads.append('None')

            try:
                locations.append(child.attrib['location'])
            except KeyError:
                locations.append("none")
            try: 
                events.append(child.attrib['event_type'])
            except KeyError:
                events.append("none")



    DF = pd.DataFrame({"Name" : names,
                       "Date" : dates,
                       "Direction": incident_dirs,
                       "Road" : roads,
                       "Location" : locations,
                       "Event" : events})
    
    print(DF)


    with open('crash_data.csv', 'a') as f:
        DF.to_csv(f, header=False)


# In[8]:


download()


# In[9]:


unzip("incidents.xml.gz", "incident.xml")


# In[10]:


data_check("incident.xml")


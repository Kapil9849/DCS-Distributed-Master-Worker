from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer
import sys
import json

# Master information
Master_URL = "http://localhost:23000"

# Storage of data
data_table = {}

# Function to register with the master
def register_with_master(port,group):
    try:
        master = ServerProxy(Master_URL)
        registration_data = {
            "worker_id": "worker-2",
            "ip_address": "localhost",
            "port": port,  # Port on which the worker is listening
            "group": group # group of data it works on
        }
        response = master.RegisterWorker(registration_data)
        print(response)
    except Exception as e:
        print("Error registering worker with the master:", e)


def load_data(group):
    # load data based which portion it handles (am or nz)
    print("Loading the data for group", group)
    data=open("data-nz.json")
    am_data=json.load(data)
    for person, person_data in am_data.items():
        data_table[person] = person_data

# Loads the data into data_table record by record. The data will be published by the publisher
def publish_data(record_id,record):
    print("data from publisher to worker 2")
    print(record_id,record)
    data_table[record_id]=record
    return True

# This function hadles the data extraction based on name
def getbyname(name):
    candidate_data=[]
    for person in data_table:
        if(person==name):
            candidate_data.append(data_table[person])
    if(len(candidate_data)>0):
        return {
            'error': False,
            'result from worker 2': candidate_data
        }
    return {
        'error': True,
        'result from worker 2': "There is no data with the given name!"
    }

# To obtain the candidate data based on location and return the data
def getbylocation(location):
    candidate_data=[]
    for person in data_table:
        person_data=data_table[person]
        if(person_data["location"]==location):
            candidate_data.append(data_table[person])
    if(len(candidate_data)>0):
        return {
            'error': False,
            'result': candidate_data
        }
    return {
        'error': True,
        'result': "There is no data with the given location!"
    }

# Returns the data of those who live at given location and year
def getbyyear(location, year):
    candidate_data=[]
    for person in data_table:
        person_data=data_table[person]
        if(person_data["location"]==location and person_data["year"]==year):
            candidate_data.append(data_table[person])
    if(len(candidate_data)>0):
        return {
            'error': False,
            'result': candidate_data
        }
    return {
        'error': True,
        'result': "There is no one living at the given location at given year"
    }

def main():
    if len(sys.argv) < 3: #checking if we have all three parameters
        print("Please provide the input in below format : ")
        print('Usage: worker.py <port> <group: am or nz>')
        sys.exit(0)

    port = int(sys.argv[1])
    group = sys.argv[2]
    server = SimpleXMLRPCServer(("localhost", port))
    print(f"Listening on port {port}...")

    #calling load_data() to load the data from json to data_store object
    load_data(group)
    
    # This method enables worker to register with master automatically
    register_with_master(port,group)

    # register RPC functions
    server.register_function(getbyname,"GetByName")
    server.register_function(getbylocation,"GetByLocation")
    server.register_function(getbyyear,"GetByYear")
    server.register_function(publish_data,"PublishData")
    server.serve_forever()

if __name__ == '__main__':
    main()
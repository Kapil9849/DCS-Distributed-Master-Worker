import json
from xmlrpc.client import ServerProxy

# Function to publish data records to workers
def publish_data_to_workers():
    # Load data from JSON files
    with open("data-am.json", "r") as file1:
        data1 = json.load(file1)
    with open("data-nz.json", "r") as file2:
        data2 = json.load(file2)
    
    # Connect to each worker
    worker_1 = ServerProxy("http://localhost:23001/")
    worker_2 = ServerProxy("http://localhost:23002/")
    
    # Publish data to worker 1
    for record_id, record in data1.items():
        try:
            print(f"Publishing record {record_id} to worker 1...")
            worker_1.PublishData(record_id, record)
        except Exception as e:
            print(f"Error publishing record {record_id} to worker 1:", e)

    # Publish data to worker 2
    for record_id, record in data2.items():
        try:
            print(f"Publishing record {record_id} to worker 2...")
            worker_2.PublishData(record_id, record)
        except Exception as e:
            print(f"Error publishing record {record_id} to worker 2:", e)

publish_data_to_workers()
    
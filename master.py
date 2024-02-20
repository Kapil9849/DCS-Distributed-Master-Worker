from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import sys

# Workers that work for master
workers = {
    # 'worker-1': ServerProxy("http://localhost:23001/"),
    # 'worker-2': ServerProxy("http://localhost:23002/")
}

# Function to register a worker
def register_worker(registration_data):
    workers[registration_data["worker_id"]]=ServerProxy("http://localhost:"+str(registration_data["port"])+"/")
    print(workers)
    return registration_data["worker_id"]+" registered successfully with the master."

#function to obtain the names of people based on location     
def getbylocation(location):  
    final_result={}  # stores the responses from both workers

    try:
        result_worker1 = workers.get("worker-1").GetByLocation(location) # getting result from worker 1
        final_result["result from worker 1"]= result_worker1["result"]
    except Exception as e:
        final_result["result from worker 1"]="Error accessing worker 1"
    try:
        result_worker2 = workers.get("worker-2").GetByLocation(location) #getting result from worker 2
        final_result["result from worker 2"]= result_worker2["result"]
    except Exception as e:
        final_result["result from worker 2"]="Error accessing worker 2"

    if(len(list(final_result.keys()))>0):
        return {"result from workers": final_result}
    else:
        return {"error": True, "message": "Unable to retrieve data from both workers"}

# function to get information based on name and reroute based on starting letter to respective worker
def getbyname(name):
    lower_name=name.lower()
    worker=""
    try:
        if(lower_name[0]>='a' and lower_name[0]<='m'): # checking if the query belongs to worker 1 or worker 2
            worker="worker-1"
            result_worker1= workers.get("worker-1").GetByName(name)
            return result_worker1["result from worker 1"]
        else:
            worker="worker-2"
            result_worker2= workers.get("worker-2").GetByName(name)
            return result_worker2["result from worker 2"]
    except Exception as e:
        return {"error": True, "message": worker+" is not available to handle the job"}
    
# ontaining information based on location and year
def getbyyear(location, year):
    final_result={} 
    try:
        result_worker1 = workers.get("worker-1").GetByYear(location,year)#getting result from worker 1
        final_result["result from worker 1"]= result_worker1["result"] 
    except Exception as e:
        final_result["result from worker 1"]="Error accessing worker 1"
    try:
        result_worker2 = workers.get("worker-2").GetByYear(location,year) #getting result from worker 2
        final_result["result from worker 2"]= result_worker2["result"]
    except Exception as e:
        final_result["result from worker 2"]="Error accessing worker 2"
    if(len(list(final_result.keys()))>0):
        return {"result from workers": final_result}
    else:
        return {"error": True, "message": "Unable to retrieve data from both workers"}


def main():
    port = int(sys.argv[1])
    server = SimpleXMLRPCServer(("localhost", port))
    print(f"Listening on port {port}...")

    # register RPC functions
    server.register_function(getbylocation,"GetByLocation")
    server.register_function(getbyname,"GetByName")
    server.register_function(getbyyear,"GetByYear")
    server.register_function(register_worker,"RegisterWorker")
    server.serve_forever()


if __name__ == '__main__':
    main()
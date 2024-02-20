import xmlrpc.client
import sys

master_port = int(sys.argv[1])
with xmlrpc.client.ServerProxy(f"http://localhost:{master_port}/") as proxy:

    #This request should be sent to worker 1 by the master as the name starts with 'a'
    name = 'alice'
    print(f'Client => Asking for person with name {name}')
    result = proxy.GetByName(name)
    print(result)
    print()

    #This request should be sent to worker 2 by the master as the name starts with 't'
    name = 'terrace'
    print(f'Client => Asking for person with name {name}')
    result = proxy.GetByName(name)
    print(result)
    print()

    location = 'Kansas City'
    print(f'Client => Asking for person lived at {location}')
    result = proxy.GetByLocation(location)
    for i in result["result from workers"]:
        print(i)
        if(type(result["result from workers"][i])==list):
            for j in result["result from workers"][i]:
                print(j)
        else:
            print(result["result from workers"][i])
    print()

    location = 'New York City'
    year = 2002
    print(f'Client => Asking for person lived in {location} in {year}')
    result = proxy.GetByYear(location, year)  
    for i in result["result from workers"]:
        print(i)
        if(type(result["result from workers"][i])==list):
            for j in result["result from workers"][i]:
                print(j)
        else:
            print(result["result from workers"][i])
    print()
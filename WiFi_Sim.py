from datetime import time
import numpy as np
import pandas as pd


def run_queue(seed):
    ### Problem Parameters ####
    inter_arrival_max = 5
    inter_arrival_min = 3

    service_time_max = 6
    service_time_min = 2

    n_events = 30

    np.random.seed(seed)
    event = 0

    #at event zero time is also zero
    time_ = 0
    #create counters for arrived and served packets
    arrived_packets = 0
    served_packets = 0
    departed_packets = 0

    #generate random variables for next events
    interarrival_time = np.random.uniform(inter_arrival_min, inter_arrival_max)
    next_arrival_time = time_ + interarrival_time
    server_status = "idle"
    queue = 0
    arrived_packets += 1

    ### event zero done
    event += 1
    #create timeseries and populate with event 1 details
    ts_columns = ['event', 'time', 'type', 'queue', 'arr cust', 'served cust', 'depar cust']
    time_series = pd.DataFrame([[1, float(next_arrival_time), 'arrival', queue, arrived_packets, 0, 0]], columns = ts_columns)


    while event <= n_events:
        #events starts
        #parameters at event t
        event_type = time_series['type'].iloc[event-1]
        time_ = time_series['time'].iloc[event-1]

        #IF EVENT IS AN ARRIVAL ##########
        if event_type == 'arrival':
            #arrival event generate by default next arrival time

            #counter of arrived packets increases by 1
            arrived_packets += 1

            #generate next arrival time
            interarrival_time = np.random.uniform(inter_arrival_min, inter_arrival_max)
            next_arrival_time = time_ + interarrival_time

            #if server status is idle customer is served immediately
            #and generates service time
            if server_status == 'idle':
                #customer is served and counter of served customer increases by 1
                served_packets += 1
                #this customer number is added to the 'served customer' column at event n
                time_series['served cust'].iloc[event-1] = served_packets

                #generate next events (service and departure time)
                service_time = np.random.uniform(service_time_min, service_time_max)
                departure_time = time_ + service_time
                departed_packets += 1 #same customer that is served at arrival time departs at departure time

                #add generated events to existing time series
                generated_events = pd.DataFrame([
                                [99, float(departure_time), "departure", 0, 0, 0, departed_packets],
                                [99, float(next_arrival_time), "arrival", 0, arrived_packets, 0, 0], 
                                ], columns = ts_columns)
                                #Order doesn't matter because it's sorted next
                            
                time_series = pd.concat([time_series, generated_events])
                #events are sorted by time
                time_series = time_series.sort_values(['time'])
                time_series.reset_index(drop=True, inplace=True)
                #event number is assigned by time order
                time_series['event'] = list(range(1, time_series.shape[0]+1))

                #event is finished and event counter increases
                event += 1


            if server_status == 'busy':
                queue += 1
                #add generated events to existing time series
                generated_events = pd.DataFrame([[99, float(next_arrival_time), "arrival", 0, arrived_packets, 0, 0]],
                                            columns = ts_columns)

                time_series = pd.concat([time_series, generated_events])
                time_series = time_series.sort_values(['time'])
                time_series.reset_index(drop=True, inplace=True)
                time_series['event'] = list(range(1, time_series.shape[0]+1))
                time_series['queue'].iloc[event-1] = queue
                #event is finished and event counter increases
                event += 1

        #IF EVENT IS A DEPARTURE #########
        if event_type == 'departure':

            # if queue is zero and customer departs, server status remains idle and next event is an arrival
            if queue == 0:
                server_status = "idle"
                #event is finised and event counter increases
                #nothing else happens until next arrival
                event += 1

            # if there are packets in queue (>0), server changes to busy and queue decreases by one
            if queue != 0:
                #customer is served and counter of served customer increases by 1
                served_packets += 1
                #this customer numebr is added to the 'served customer' column at event n
                time_series['served cust'].iloc[event-1] = served_packets

                #queue decreases by one
                queue -= 1
                server_status = "busy"


                #generate the next events (service and departure time)
                service_time = np.random.uniform(service_time_min, service_time_max)
                departure_time = time_ + service_time
                departed_packets += 1 #same customer that is served at arrival time departs at departure time

                #add generated events to existing time series
                generated_events = pd.DataFrame([
                                        [99, float(departure_time), "departure", 0, 0, 0, departed_packets]], 
                                        columns = ts_columns)
                
                time_series = pd.concat([time_series, generated_events])
                time_series = time_series.sort_values(['time'])
                time_series.reset_index(drop=True, inplace=True)
                time_series['event'] = list(range(1, time_series.shape[0]+1))
                time_series['queue'].iloc[event-1] = queue

                #event is finished and event counter increases
                event += 1

        #once the event is finished, determine the server status for the next event
        #if the next arrival is before the departure of current customer, server will be busy at arrival
        if next_arrival_time < departure_time:
            server_status = "busy"
        else:
            server_status = "idle"


    ####--------------Summary of Customer Data with results----------------########

    #get arriving packets
    arrivals = time_series.loc[time_series['type'] == 'arrival', ['time', 'arr cust']]
    arrivals.columns = ['time', 'customer']
    #get departing packets
    departure = time_series.loc[time_series['type'] == 'departure', ['time', 'depar cust']]
    departure.columns = ['time', 'customer']
    #get packets being served
    serving = time_series.loc[time_series['served cust'] != 0, ['time', 'served cust' ]]
    serving.columns = ['time', 'customer']

    #merge
    customer_df = arrivals.merge(departure, on='customer')
    customer_df = customer_df.merge(serving, on='customer')
    customer_df.columns = ['arrival time', 'customer', 'departure time', 'serving time']
    customer_df = customer_df[['customer', 'arrival time', 'serving time', 'departure time']]

    #get time in queue
    customer_df['time in queue'] = customer_df['serving time'] - customer_df['arrival time']
    #get time in system
    customer_df['time in system'] = customer_df['serving time'] - customer_df['arrival time']
    #get time in server
    customer_df['time in server'] = customer_df['departure time'] - customer_df['serving time']
    #round all floats to 2 digits
    customer_df = customer_df.round(2)

    empty_dictionary = {}
    total_cust = customer_df['customer'].max()
    avgtime_inqueue = customer_df['time in queue'].sum()/total_cust
    avgtime_inserver = customer_df['time in server'].sum()/total_cust
    avgtime_insystem = customer_df['time in system'].sum()/total_cust

    empty_dictionary['time in queue'] = [avgtime_inqueue]
    empty_dictionary['time in server'] = [avgtime_inserver]
    empty_dictionary['time in system'] = [avgtime_insystem]

    return empty_dictionary 



# Do multiple simulations runs the queuing system created above
def run_experiments(n_runs=50):
    df = pd.DataFrame(columns = ['time in queue', 'time in server', 'time in system'])
    for i in range(n_runs):
        run_result = pd.DataFrame([run_queue(seed = i)])
        df = df.append(run_result)
    df.reset_index(inplace=True, drop=True)
    df['run number'] = range(1, n_runs+1)
    df = df[['run number', 'time in queue', 'time in server', 'time in system']] #rearrange columns
    return df

experiments = run_experiments()


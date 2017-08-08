__author__ = "Najeem Adeleke, PhD"
#from datetime import datetime
import json
from Customer import Customer
from math import sqrt
import time, sys
#import numpy as np

#%load_ext memory_profiler
#%load_ext line_profiler

# ----------------------------------------------------------
# Recursive algorithm to determine the Dth degree friends of a given customer (c_obj)
# ----------------------------------------------------------
def track_network_friends(d, c_obj, c_obj_dict, network_f_ids):

    # The 'if statement' below will include the current customer in the mean and sd calc.
    if c_obj.id not in network_f_ids:
        network_f_ids.append(c_obj.id)

    if d == 1:
        for id in c_obj.friend_ids :
            if id not in network_f_ids:
                network_f_ids.append(id)
        return
    else:
        for id in c_obj.friend_ids :
            track_network_friends(d-1, c_obj_dict[id], c_obj_dict, network_f_ids)
    return

# ----------------------------------------------------------
# Function to determine obtain network purchases
# ----------------------------------------------------------
def get_network_purchases(network_f_ids, c_obj_dict, network_p_ids):
    for id in network_f_ids:
        network_p_ids.append(c_obj_dict[id].purchase_ids)
    return

# ----------------------------------------------------------
# Function to Process Batch data
# ----------------------------------------------------------
#@profile
def processBatchLogFile():

    # Opening log files
    try:
        # If running ./run.sh under 'anolamy_dection' directory from command line, use the two lines below
        data_file = open('log_input/batch_log.json')
        output_file = open('log_output/flagged_purchases.json', "w")
    except:
        # If running the code in an IDE like PyCharm, use the two lines below to load file
        data_file = open('../log_input/batch_log.json')
        output_file = open('../log_output/flagged_purchases.json', "w")
        #data_file = open('../sample_dataset/batch_log.json')

    # Reading first line of .json data file
    jd = json.loads(data_file.readline())
    print('\nNetwork Parameters --> '+json.dumps(jd))
    D = int(jd['D'])
    T = int(jd['T'])

    # Creating an dictionary to hold customer objects
    customer = {}

    # Creating an empty dictionary for purchases
    purchase = {}

    # Initiating a variable for purchase id
    p_id = 0

    # Reading remaining lines of .json data file
    data = data_file.readlines()
    data_file.close()

    # Populating customer dictionary with customer objects
    for line in data:
        jd = json.loads(line)
        # print(jd)
        event = jd['event_type']
        try:
            if event == 'befriend' :

                id1 = jd['id1']
                id2 = jd['id2']
                if customer.get(id1) is not None:
                    customer[id1].add_friend(id2)
                else:
                    customer[id1] = Customer(id1)
                    customer[id1].add_friend(id2)

                if customer.get(id2) is not None:
                    customer[id2].add_friend(id1)
                else:
                    customer[id2] = Customer(id2)
                    customer[id2].add_friend(id1)

            elif event == 'unfriend' :

                id1 = jd['id1']
                id2 = jd['id2']
                customer[id1].remove_friend(id2)
                customer[id2].remove_friend(id1)

            elif event == 'purchase' :

                p_id += 1
                amount = jd['amount']
                purchase[p_id] = (jd['timestamp'], amount)

                c_id = jd['id']
                if customer.get(c_id) is None:
                    customer[c_id] = Customer(c_id)

                #list1 = []
                #network_f_ids = []
                #track_network_friends(D, customer[c_id], customer, network_f_ids, list1)

                # Obtaining network friend ids (network_f_ids) of the Dth degree for customer with id (c_id)
                network_f_ids = []
                track_network_friends(D, customer[c_id], customer, network_f_ids)

                # Obtaining network purchase ids (network_p_ids) using network friend ids
                network_p_ids = []
                get_network_purchases(network_f_ids, customer, network_p_ids)

                # Creating a network purchases dictionary using the obtained network purchase ids
                network_purchases = {}
                for list in network_p_ids:
                    for l in list:
                        network_purchases[l] = purchase[l]

                if len(network_purchases) > 1:
                    #t0 = time.time()
                    # Calculating mean purchase amount for the latest T transactions in the network
                    n = 0
                    sum_x1 = 0
                    sum_x2 = 0
                    mean = 0
                    sd = 0
                    for key in sorted(network_purchases, reverse=True):
                        n += 1
                        x = float(network_purchases[key][1])
                        sum_x1 += x
                        sum_x2 += x * x
                        mean = sum_x1 / n
                        sd = sqrt( (n * sum_x2 - sum_x1 * sum_x1) / (n * (n - 1)) ) if n > 1 else 0
                        #print('key = {}     amount ={}  mean = {}        sd = {}'.format(key, x, mean, sd))
                        if n == T:
                            break

                    #t1 = time.time()

                    # Alternative method for calculating mean purchase amount for the latest T transactions in the network
                    #list1 = [float(network_purchases[key][1]) for key in sorted(network_purchases, reverse=True)]
                    #list2 = [a*b for a,b in zip(list1,list1)]
                    #mean =  sum(list1) / len(list1) if n > 0 else 0
                    #sd = sqrt(1/(n*(n-1)) * (n*sum(list2)  - sum(list1)*sum(list1))) if n>1 else 0

                    #t2 = time.time()
                    #print('dt1 = {} \ndt2 = {}'.format(t1 - t0, t2 - t1))

                    # Storing anomalous purchase in flagged purchases output file
                    if float(amount) > mean + (3 * sd) :
                        jd['mean'] = '{0:.2f}'.format(mean)
                        jd['sd'] = '{0:.2f}'.format(sd)
                        #output_file.write(str(json.dumps(jd)) + '\n')
                        #print('Anomalous purchase --> ' + str(json.dumps(jd)) + '\n')

                # Ensuring we don't run out of integers to represent p_id
                if p_id > 9223372036854775800:
                    p_id = 0
                    print('here')

                # Updating current customer purchase after anomaly check
                customer[c_id].update_purchases(p_id, T)

                #print('mean =', mean, 'sd = ', sd)

                #print(n, list1)
                #print('Using numpy, mean is {} and std is {}'.format(np.mean(list1), np.std(list1)))
                #print('customer id = {}\n'.format(c_id))

            else:
                    return 1
        except Exception as err:
            sys.stderr.write('Exception Error: %s' % str(err))
            print('Error in logfile  -->' + jd)
            pass



    #print(customer['1'].purchase_ids)
    #print(purchase)
    #print(D,T)
    #print(network_f_ids)
    #print(customer['1'].friend_ids)
    #print(customer['2'].friend_ids)
    #print(network_p_ids)
    #print(customer['1'].purchase_ids)
    #print(customer['2'].purchase_ids)
    #print(customer['7'].purchase_ids)
    #print(customer['5'].purchase_ids)
    output_file.close()
    return (customer, purchase, p_id, D, T)


# ----------------------------------------------------------
# Function to Process Stream data
# ----------------------------------------------------------
def processStreamLog(customer, purchase, p_id, D, T):
    # Opening log files
    try:
        # If running ./run.sh under 'anolamy_dection' directoru from command line, use the two lines below
        data_file = open('log_input/stream_log.json')
        output_file = open('log_output/flagged_purchases.json', "a")
    except:
        # If running the code in an IDE like PyCharm, use the two lines below to load file
        data_file = open('../log_input/stream_log.json')
        output_file = open('../log_output/flagged_purchases.json', "a")
        #data_file = open('../sample_dataset/stream_log.json')

    # Reading remaining lines of .json data file
    data_stream = data_file.readlines()
    data_file.close()

    count = 0
    # Populating customer dictionary with customer objects
    for line in data_stream:
        count += 1
        #print('count = {}'.format(count))
        jd = json.loads(line)
        # print(jd)
        event = jd['event_type']

        try:
            if event == 'befriend': # Create customer objects and stores them in customer dictionary

                id1 = jd['id1']
                id2 = jd['id2']
                if customer.get(id1) is not None:
                    customer[id1].add_friend(id2)
                else:
                    customer[id1] = Customer(id1)
                    customer[id1].add_friend(id2)

                if customer.get(id2) is not None:
                    customer[id2].add_friend(id1)
                else:
                    customer[id2] = Customer(id2)
                    customer[id2].add_friend(id1)

            elif event == 'unfriend': # update customer objects in customer dictionary

                id1 = jd['id1']
                id2 = jd['id2']
                customer[id1].remove_friend(id2)
                customer[id2].remove_friend(id1)

            elif event == 'purchase': # Update purchase dictionary and customer objects in customer dict.

                p_id += 1
                amount = jd['amount']
                purchase[p_id] = (jd['timestamp'], amount)

                c_id = jd['id']
                if customer.get(c_id) is None:
                    customer[c_id] = Customer(c_id)


                # Obtaining network friend ids (network_f_ids) of the Dth degree for customer with id (c_id)
                network_f_ids = []
                track_network_friends(D, customer[c_id], customer, network_f_ids)

                # Obtaining network purchase ids (network_p_ids) using network friend ids
                network_p_ids = []
                get_network_purchases(network_f_ids, customer, network_p_ids)

                # Creating a network purchases dictionary using the obtained network purchase ids
                network_purchases = {}
                for list in network_p_ids:
                    for l in list:
                        network_purchases[l] = purchase[l]

                if len(network_purchases) > 1:
                    # Calculating mean purchase amount for the latest T transactions in the network
                    n = 0
                    sum_x1 = 0
                    sum_x2 = 0
                    mean = 0
                    sd = 0
                    for key in sorted(network_purchases, reverse=True):
                        n += 1
                        x = float(network_purchases[key][1])
                        sum_x1 += x
                        sum_x2 += x * x
                        mean = sum_x1 / n
                        sd = sqrt((n * sum_x2 - sum_x1 * sum_x1) / (n * (n - 1))) if n > 1 else 0
                        #print('key = {}     amount ={}  mean = {}        sd = {}'.format(key, x, mean, sd))
                        if n == T:
                            break

                    # Alternative method for calculating mean purchase amount for the latest T transactions in the network
                    # list1 = [float(network_purchases[key][1]) for key in sorted(network_purchases, reverse=True)]
                    # list2 = [a*b for a,b in zip(list1,list1)]
                    # mean =  sum(list1) / len(list1) if n > 0 else 0
                    # sd = sqrt(1/(n*(n-1)) * (n*sum(list2)  - sum(list1)*sum(list1))) if n>1 else 0

                    # Storing anomalous purchase in flagged purchases output file
                    if float(amount) > mean + (3 * sd):
                        jd['mean'] = '{0:.2f}'.format(mean)
                        jd['sd'] = '{0:.2f}'.format(sd)
                        output_file.write(str(json.dumps(jd)) + '\n')
                        print('Anomalous purchase --> ' + str(json.dumps(jd)))

                # Ensuring we don't run out of integers to represent p_id
                if p_id > 9223372036854775800:
                    p_id = 0
                    print('here*******')

                # Updating current customer purchase after anomaly check
                customer[c_id].update_purchases(p_id, T)

            else:
                return 1
        except Exception as err:
            sys.stderr.write('Exception Error: %s' % str(err))
            print('Error in logfile  -->' + jd)
            pass


    #print(network_f_ids)
    #print(network_p_ids)
    output_file.close()
    return 0



# ----------------------------------------------------------
# Main
# ----------------------------------------------------------
#@profile
def main():

    t0 = time.time()
    print('\n--------------------------\nProcessing batch file ...\n--------------------------')
    customer, purchase, p_id, D, T = processBatchLogFile()
    t1 = time.time()
    print('\n--------------------------\nProcessing stream file ...\n--------------------------')
    processStreamLog(customer, purchase, p_id, D, T)
    t2 = time.time()
    print('\n--------------------------\nDone !!!!!!!!!!!!!!!!!!!!!\n--------------------------')
    print('\nBatch log processing ---> {} secs'
          '\nStream log processing --> {} secs'
          '\nTotal processing time --> {} secs'.format(t1-t0, t2-t1, t2-t0))
    return 0

# ----------------------------------------------------------
# __name__
# ----------------------------------------------------------
if __name__ == "__main__":
    main()
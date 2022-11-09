#!/usr/bin/python

from concurrent.futures import process
import warnings
warnings.filterwarnings('ignore')

import os

import numpy as np

import pathlib
from time import sleep, time
from argparse import ArgumentParser
import subprocess

import pandas as pd
import logging as logger
import pycurl
import statistics

import signal

import multiprocessing 
from multiprocessing import Manager


log_FORMAT = '%(created)f -- %(levelname)s: %(message)s'
logger.basicConfig(format=log_FORMAT, 
                    datefmt='%m/%d/%Y %I:%M:%S %p', 
                    level=logger.INFO) # logger format for gradient descent output

output_directory = 'data/' # output directory where the downloaded files will be stored

target_throughput = 0 # target throughput value initialization where this value changed is via command line arguements

finished_file_bytes = 0 # used for calculating the total throughput

lock_sample_list = multiprocessing.Lock() # initializing the lock for the sample list (this ensures proper synchronization and no race conditions / mutual exclusion)
lock_active_transfer_list = multiprocessing.Lock() # initializing the lock for the active transfer list (this ensures proper synchronization and no race conditions / mutual exclusion)

# CLASS FOR FILE OBJECT #
class FileObject:
    def __init__(self, filename, processID, offset): # constructor
        self.filename = filename # filename is stored in the file object (pretty much the key for the object itself)
        self.processID = processID # processID is stored in the file object (this is primarily used for the removeProcesses function)
        self.offset = offset # offset is stored in the file object to ensure the program knows where it left off during its download of the file

# DOWNLOAD MONITOR FUNCTION # 
def download_monitor (shared_dict): # arguement is the shared_dict object (this stores the important pieces of data for the high frequency file downloader)
    global finished_file_bytes, concurrency # 2 global variables that are initalized via command line arguements and changed throughout the program
    last_size = 0 # used for calculating throughput deviation
    last_concurrency_update_t = 0 # similar to above
    throughput_list = [] # basic list data structure that is used to ensure that we maintain the throughput calculations done previously (this is used heavily in the GD algo)

    while True: # loop for monitor
        temp_sample_list = list(shared_dict['sample_list']) # grabs version of sample list (this is done so we can make changes to the shared dict as the shared_dict object does not support changes like pop or append, only assignment)
        temp_active_transfer_list = list(shared_dict['active_transfer_list']) # see above
        temp_ccs = list(shared_dict['ccs']) # see above
        temp_average_throughputs = list(shared_dict['average_throughputs']) # see above # see above
        sleep(1) # used to maintain conformity, as well as to ensure that system stays synchronized
        size = finished_file_bytes # initializes size to finished_file_bytes 
        if len(temp_sample_list) == 0 and len(temp_active_transfer_list) == 0: # if we have an empty list, just break out of the download monitor
           return
        for f in temp_active_transfer_list: # iterate through the entire active transfer list
            try:
                temp_file_object_dict = dict(shared_dict['file_object_dict'])
                temp_file_object_dict[f].offset = pathlib.Path(f).stat().st_size # initalize the place where we are on in our download of the file and set this to the offset value
                shared_dict['file_object_dict'] = temp_file_object_dict # anytime we change the temp versions of the shared_dict, to maintain synchronization, we reassign
                size += pathlib.Path(f).stat().st_size # change the size to the new total
            except:
                continue
        throughput = ((size-last_size)*8)/(1000*1000.0) # basic throughput calculation derivative
        if throughput == 0:
            continue
        throughput_list.append(throughput) # add new throughput to throughput list so we can use our GD algo
        print ("Throughput {} Mbps, target {} Mbps, Active files: {}, Remaining files {}".format(throughput, target_throughput, len(temp_active_transfer_list), len(temp_sample_list))) # print statement to tell the user what is happening
        last_size = size # set the last_size to the current size in preparation for the next iteration
        if len(throughput_list) > 6 and time() > last_concurrency_update_t + 7: # this is done so that we accrue enough values to have a valid mean throughput (we should not have too small of a sample size as this could easily be skewed by noise)
            if len(temp_sample_list) < 2:
                continue

            temp_average_throughputs.append(statistics.mean(throughput_list[-5:])) # adding the new mean to the average throughput list
            shared_dict['average_throughputs'] = temp_average_throughputs # reassigning to ensure synchronization
            if concurrency != temp_ccs[-1]: # if the GD algo returns a new concurrency for us to try, enter this if statement
                # if temp_ccs[-1] == 3: # TODO: REMOVE THIS WHEN YOU TURN IT IN, THIS IS JUST USED TO TEST THE DROP IN CONCURRENCY
                #     temp_ccs[-1] = 1
                if temp_ccs[-1] - concurrency > 0: # if our new concurrency is more than our previous concurrency, we need to add that many more processes
                    add_more_processes(temp_ccs[-1] - concurrency, shared_dict)
                else:
                    remove_some_processes(concurrency - temp_ccs[-1], shared_dict) # conversely, if our new concurrency (given to us by GD) is less, than we need to remove some processes
                concurrency = temp_ccs[-1] # reassign our concurrency value in preparation for next iteration
                last_concurrency_update_t = time() # grab newest concurrency update time
            throughput_list = [] # clear the throughput list so that we have accurate mean values
    return

# FILE DOWNLOADER FUNCTION
def file_downloader (shared_dict): # arguments, same as download monitor
    global finished_file_bytes # global finished file bytes value used for throughput calculations
    print("Running thread...") # print statement to indicate thread has been initialized
    curl = pycurl.Curl() # curl object instantiation as that is the downloading method used here
    
    while True: # loop
        temp_sample_list = list(shared_dict['sample_list']) # grabs version of sample list (this is done so we can make changes to the shared dict as the shared_dict object does not support changes like pop or append, only assignment)
        temp_active_transfer_list = list(shared_dict['active_transfer_list']) # see above
        lock_sample_list.acquire() # unlock the sample list
        if len(sample_list) == 0:
            print("Exiting thread...")
            return
        # Fetch a file from file list and add is to currently transferred file list
        # Synchronized operation due to using concurrency
        if len(temp_sample_list) != 0:
            filename = temp_sample_list.pop() # grabs the next filename off the top of the stack
        temp_file_object_dict = dict(shared_dict['file_object_dict'])
        temp_file_object_dict['data'+filename].processID = multiprocessing.current_process().pid # initalizes the file object's processID with the current process's id
        print(temp_file_object_dict['data'+filename].filename)
        print(temp_file_object_dict['data'+filename].processID)
        shared_dict['file_object_dict'] = temp_file_object_dict # reassigns as the file object dict has changed
        print(shared_dict['file_object_dict']['data'+filename].filename)
        print(shared_dict['file_object_dict']['data'+filename].processID)

        shared_dict['sample_list'] = temp_sample_list # reassigns as the sample list object has changed
        lock_sample_list.release() # relocks the sample list

        lock_active_transfer_list.acquire() # unlocks the active transfer list
        file_path = output_directory + filename # gets the filepath

        temp_active_transfer_list.append(file_path) # adds said filepath to the active transfer list
        shared_dict['active_transfer_list'] = temp_active_transfer_list # reassigns the active transfer list

        lock_active_transfer_list.release() # relocks the active transfer list


        # initialize and start curl file download
        sample_ftp_url = discover_ftp_paths([filename])[0] # calls the discover ftp paths function so that we know where to download from
        #print("Starting to download " + filename, sample_ftp_url)
        fp = open(file_path, "wb") # possibly append # opens up a file that we can write to (i.e. initalizes our version of the downloaded file)
        curl.setopt(pycurl.URL, sample_ftp_url) # sets the URL that we are downloading from
        curl.setopt(pycurl.WRITEDATA, fp) # sets where to write the data to
        curl.setopt(pycurl.LOW_SPEED_LIMIT, 1) # sets the lowest possible transmission rate that we are allowing curl to have before it terminates
        curl.setopt(pycurl.LOW_SPEED_TIME, 2) # sets the max amount of time that the curl object will try and download with no response before it terminates
        
        header = ['Range: bytes=' + str(temp_file_object_dict[file_path].offset) + '-'] # sets the offset 
        curl.setopt(pycurl.HTTPHEADER, header) # tells curl to start from this specific point in the file


        #curl.setopt(curl.NOPROGRESS, False)
        #curl.setopt(curl.XFERINFOFUNCTION, status)
        retry = 0 # initalizes retry value that will be used to ensure that download does not hang
        while retry < 3: # see above
            try: 
                curl.perform() # try and download the file
                file_size = pathlib.Path(file_path).stat().st_size # change the file size accordingly
                finished_file_bytes += file_size # add the number of bytes that we have downloaded so far to the absolute total
                temp_file_object_dict['data'+filename].offset = finished_file_bytes # change the offset accordingly
                shared_dict['file_object_dict'] = temp_file_object_dict # reassign the shared dict since we changed the temp copy
                print("Finished {} size: {} MB".format(filename, (file_size/(1024*1024)))) # print statement indicating to the user how many MB we downloaded
            except pycurl.error as exc: # clean catch of pycurl errors
                print("Unable to download file %s (%s)" % (filename, exc)) # tell the user that we were unable to download the requested file
                retry +=1 # increment retry value (see above for why)
            finally:
                fp.close() # after done, close file pointer object
                lock_active_transfer_list.acquire() # unlock active transfer list
                temp_active_transfer_list.remove(file_path) # remove file from said transfer list
                shared_dict['active_transfer_list'] = temp_active_transfer_list # reassign the shared dict since we changed the temp copy
                print(temp_active_transfer_list)

                lock_active_transfer_list.release() # relock the active transfer list
                break # hop out of while retry < 3 loop
        if retry == 3: # if the program ever hits the max amount of retries
            print("Download attempt for file %s (%s) failed () times" % (filename, exc, retry)) # tell the user that we tried 3 times and failed
            lock_sample_list.acquire() # unlock the sample list
            temp_sample_list.append(filename) # add this file back to the sample list since we didn't download it
            shared_dict['sample_list'] = temp_sample_list # reassign the shared dict since we changed the temp copy

            lock_sample_list.release() # relock the sample list
    curl.close()
# FUNCTION THAT IS USED TO GRAB THE FTP PATHS FOR OUR DOWNLOAD
def discover_ftp_paths (sample_list):
    command = "srapath " + " ".join(sample_list)
    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE)
    paths = result.stdout.decode('utf-8').splitlines()
    return paths

# ADD MORE PROCESSES FUNCTION
def add_more_processes(count, shared_dict): # arguments are the number of processes that we need to add, as well as the shared dict
    for i in range(count): # iterate count number of times
        thread = multiprocessing.Process(target=file_downloader, \
                                  args=(shared_dict,), daemon=True) # for each iteration, add a new process of file downloader
        print("Creating new thread ") # tell the user what is happening
        thread.start() # start the thread

# REMOVE SOME PROCESSES FUNCTION
def remove_some_processes(count, shared_dict): # arguments are the number of processes that we need to remove, as well as the shared dict
    temp_sample_list = list(shared_dict['sample_list']) # grabs version of sample list (this is done so we can make changes to the shared dict as the shared_dict object does not support changes like pop or append, only assignment)
    temp_active_transfer_list = list(shared_dict['active_transfer_list']) # see above
    temp_file_object_dict = dict(shared_dict['file_object_dict']) # see above

    for i in temp_file_object_dict.keys():
        file = temp_file_object_dict[i]
        print(file.filename, file.processID, file.offset)
    
    print(count)

    for i in range(count): # iterate count number of times
        lock_active_transfer_list.acquire() # unlock the active transfer list
        filename = temp_active_transfer_list.pop(0) # grab the file that was first added
        print(filename)
        temp_file_object_dict = dict(shared_dict['file_object_dict'])
        temp_file_object_dict[filename].offset = pathlib.Path(filename).stat().st_size # grab said file's most recent offset value

        os.kill(temp_file_object_dict[filename].processID, signal.SIGKILL) # kill the process that is downloading the file using its pid

        print('Deleting thread...') # tell the user what we did
        shared_dict['active_transfer_list'] = temp_active_transfer_list # reassign the shared dict as we changed the temp copy
        shared_dict['file_object_dict'] = temp_file_object_dict # reassign the shared dict as we changed the temp copy
        lock_active_transfer_list.release() # relock the active transfer list

        lock_sample_list.acquire() # unlock the sample list
        temp_sample_list.append(filename[4:len(filename)]) # add the filename that we just removed back to the sample list
        shared_dict['sample_list'] = temp_sample_list # reassign the shared dict since we changed the temp copy
        lock_sample_list.release() # relock the sample list
# GRADIENT DESCENT ALGO
def harp_response(params, count, shared_dict):
    global max_cc, thrpt
    cc = params[0]
    logger.info("Iteration {0} Starts ...".format(count))
    logger.info("Sample Transfer -- Probing Parameters: {0}".format(params))
    thrpt = 0
    while True:
        temp_average_throughputs = list(shared_dict['average_throughputs'])
        while thrpt != temp_average_throughputs[-1]:
            try:
                thrpt = temp_average_throughputs[-1]
                if thrpt is not None:
                    break
                
            except Exception as e:
                logger.exception(e)
                thrpt = -1
                    
        if thrpt == -1:
            logger.info("Optimizer Exits ...")
            exit(1)
        else:
            score = (thrpt/(1.02)**cc) * (-1)
        
        logger.info("Sample Transfer -- Throughput: {0}Mbps, Score: {1}".format(
            np.round(thrpt), score))
        return score

# GRADIENT DESCENT ALGO
def gradient(black_box_function, shared_dict):
    global thrpt
    max_thread, count = max_cc, 0
    soft_limit, least_cost = max_thread, 0
    values = []
    theta = 0

    while True:
        sleep(0.05)
        temp_average_throughputs = list(shared_dict['average_throughputs'])
        temp_ccs = list(shared_dict['ccs'])
        while len(temp_average_throughputs) > 0 and thrpt != temp_average_throughputs[-1]:
            count += 1
            values.append(black_box_function([temp_ccs[-1]], count, shared_dict))
            if values[-1] < least_cost:
                least_cost = values[-1]
                soft_limit = min(ccs[-1]+10, max_thread)
            
            if len(temp_ccs) == 1:
                temp_ccs.append(2)
                shared_dict['ccs'] = temp_ccs
            
            else:
                dist = max(1, np.abs(temp_ccs[-1] - temp_ccs[-2]))
                if temp_ccs[-1]>temp_ccs[-2]:
                    gradient = (values[-1] - values[-2])/dist
                else:
                    gradient = (values[-2] - values[-1])/dist
                
                if values[-2] !=0:
                    gradient_change = np.abs(gradient/values[-2])
                else:
                    gradient_change = np.abs(gradient)
                
                if gradient>0:
                    if theta <= 0:
                        theta -= 1
                    else:
                        theta = -1
                        
                else:
                    if theta >= 0:
                        theta += 1
                    else:
                        theta = 1
            
                update_cc = int(theta * np.ceil(temp_ccs[-1] * gradient_change))
                next_cc = min(max(temp_ccs[-1] + update_cc, 2), soft_limit)
                logger.info("Gradient: {0}, Gradient Change: {1}, Theta: {2}, Previous CC: {3}, Choosen CC: {4}".format(gradient, gradient_change, theta, temp_ccs[-1], next_cc))
                temp_ccs.append(next_cc)
                shared_dict['ccs'] = temp_ccs


if __name__ == "__main__":
    # LINES BELOW ARE USED FOR COMMNAD LINE ARGUMENT PARSING
    parser = ArgumentParser()

    parser.add_argument('-i', '--input',
                  action="store", dest="sample_list",
                  help="input file for sample list", default="samples_extremely_large.tsv") # samples_extremely_large.tsv
    parser.add_argument('-o', '--output',
                      action="store", dest="output_directory",
                      help="output directory to save sample files", default="data")
    parser.add_argument('-t', '--target',
                      action="store", dest="target_throughput", type=int,
                      help="target throughput for the transfer", default=0)

    args = parser.parse_args()

    sample_file = pd.read_csv(args.sample_list, sep="\s+", dtype=str).set_index("sample", drop=False)

    manager = Manager()
    shared_dict = manager.dict()

    active_transfer_list = []
    sample_list = sample_file["sample"].values.tolist()

    # dataSRR are the keys and the values are the file objects
    file_object_dict = {}

    for i in sample_list:
        file_object_dict['data'+i] = FileObject('data'+i, 0, 0)
    

    average_throughputs = []
    ccs = [1]

    shared_dict['active_transfer_list'] = active_transfer_list
    shared_dict['sample_list'] = sample_list
    shared_dict['file_object_dict'] = file_object_dict
    shared_dict['average_throughputs'] = average_throughputs
    shared_dict['ccs'] = ccs

    max_cc = 100
    thrpt = 0
    concurrency = ccs[-1]

    output_directory = args.output_directory
    pathlib.Path(output_directory).mkdir(parents=True, exist_ok=True)

    # START INITIAL THREADS
    t = multiprocessing.Process(target=download_monitor, \
                     args=(shared_dict, )) # TODO: POSSIBLY ADD DAEMON = TRUE BACK TO THIS PROCESS
    gradient_thread = multiprocessing.Process(target=gradient, args=((harp_response, shared_dict)))
    t.start()
    gradient_thread.start()
    add_more_processes(concurrency, shared_dict)
    t.join()

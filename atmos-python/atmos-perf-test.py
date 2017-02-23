from __future__ import division
ATMOS_KEY = "194ed3dd98414dcebc780f030d36e616/131318694590302234@ecstestdrive.emc.com"
ATMOS_SECRET = "/rDrx8U/Os8KJsbLClrLrDrDuyyV74YbBFrHfm0d"
HOST = 'atmos.ecstestdrive.com'
PORT = 443
 
import re
import os
import sys
import subprocess
import time
import json
from EsuRestApi import EsuRestApi
import datetime

numberOfIterations = 2
api = EsuRestApi(HOST, PORT, ATMOS_KEY, ATMOS_SECRET)
results = {}
objectList = []
fileDetails = {}
#fileDetails['../512k']={ "count": '100', "size": '512'}
#fileDetails['../1MB']={ "count": '50', "size": '1000'}
#fileDetails['../10MB']={ "count": '25', "size": '10000'}
#fileDetails['../100MB']={ "count": '10', "size": '100000'}
fileDetails['../1000MB']={ "count": '5', "size": '1000000'}

def createDirectories():
    for directory in fileDetails.keys():
        if not os.path.exists(directory):
            os.makedirs(directory)

def createTestFiles():
    print('Creating test files, this can take a while')
    for key, value in fileDetails.iteritems():
        directory = key
        count = int(value['count'])
        size = str(value['size'])
        if os.listdir(directory) == []: 
            for num in range(count):
                subprocess.call(['dd', 'if=/dev/zero', "of=" + directory + "/file-" + str(num), "bs=1024", "count=" + size])

def cleanup():
    for object in objectList:
        api.delete_object(object)

def get_size(start_path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

def sizeof_fmt(num):
    num = (num/1024)/1024
    return num

def uploadFiles():
    for directory, value in fileDetails.iteritems():
        transferTimeList = []
        for num in range(numberOfIterations):
            dirSize = get_size(directory)
            filenames = os.listdir(directory)
            print('Uploading ' + fileDetails[directory]['count'] + " " + directory + ' files')
            for fname in filenames:
                start_time = datetime.datetime.now()
                fileData = open(directory + '/' + fname, 'rb')
                objectId = api.create_object(data = fileData.read())
                objectList.append(objectId)
                fileData = open(directory + '/' + fname, 'rb')
                fileData.close()
                end_time = datetime.datetime.now()
                transferTime = (end_time.microsecond - start_time.microsecond)
                transferTimeList.append(int(transferTime))
                
        transferTime1 = reduce(lambda x, y: x + y, transferTimeList) / len(transferTimeList)
        transferTime1 /= 1000
        throughput = (sizeof_fmt(dirSize)/transferTime1)
        results[directory]={'time': transferTime1, 'throughput': throughput}

createDirectories()
createTestFiles()
#listObjects()
uploadFiles()
print json.dumps(results, sort_keys=True,indent=4, separators=(',', ': '))
#cleanup()

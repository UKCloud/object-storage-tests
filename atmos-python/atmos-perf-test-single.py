ATMOS_KEY = "194ed3dd98414dcebc780f030d36e616/131318694590302234@ecstestdrive.emc.com"
ATMOS_SECRET = "/rDrx8U/Os8KJsbLClrLrDrDuyyV74YbBFrHfm0d"
HOST = 'atmos.ecstestdrive.com'
PORT = 443
 
from Queue import Queue
import re
import os
import sys
import subprocess
import time
import json
from datetime import datetime
from EsuRestApi import EsuRestApi

api = EsuRestApi(HOST, PORT, ATMOS_KEY, ATMOS_SECRET)
results = {}
objectList = []
fileDetails = {}
#fileDetails['../512k']={ "count": '100', "size": '512'}
#fileDetails['../1MB']={ "count": '50', "size": '1000'}
#fileDetails['../10MB']={ "count": '25', "size": '10000'}
#fileDetails['../100MB']={ "count": '10', "size": '100000'}
fileDetails['../1000MB']={ "count": '5', "size": '1000000'}
transferTimeList = []
throughputList = []
numberOfIterations = 10

def createDirectories():
    for directory in fileDetails.keys():
        if not os.path.exists(directory):
            os.makedirs(directory)

def createTestFiles():
    for key, value in fileDetails.iteritems():
        directory = key
        count = int(value['count'])
        size = str(value['size'])
        if os.listdir(directory) == []: 
            print('Creating test files, this can take a while')
            for num in range(count):
                subprocess.call(['dd', 'if=/dev/zero', "of=" + directory + "/file-" + str(num), "bs=1024", "count=" + size])

def cleanup():
    for object in objectList:
        api.delete_object(object)

def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return num
        num /= 1024.0
    return num

def singleFileUpload(value, filePath):
    start_time = os.times()[4]
    fileData = open(filePath, 'rb')
    objectList.append(api.create_object(data = fileData.read()))
    fileData.close()
    end_time = os.times()[4]
    transferTime = ( end_time - start_time )
    throughput = sizeof_fmt((float(value['size'])/1000)/float(transferTime))
    transferTimeList.append(transferTime)
    throughputList.append(throughput)

def uploadFiles():
    for num in range(numberOfIterations):
        my_queue = Queue(maxsize=0)
        for directory, value in fileDetails.iteritems():
            filenames = os.listdir(directory)
            for fname in filenames:
                singleFileUpload(value, directory + '/' + fname)
    throughput = reduce(lambda x, y: x + y, throughputList) / len(throughputList)
    transferTime = reduce(lambda x, y: x + y, transferTimeList) / len(transferTimeList)
    print(value['size'] + ',' + str(transferTime) + ',' + str(throughput))

createDirectories()
createTestFiles()
#listObjects()
uploadFiles()
#print json.dumps(results, sort_keys=True,indent=4, separators=(',', ': '))
#cleanup()

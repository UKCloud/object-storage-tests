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
from datetime import datetime
from EsuRestApi import EsuRestApi
import threading

api = EsuRestApi(HOST, PORT, ATMOS_KEY, ATMOS_SECRET)
results = {}
objectList = []
fileDetails = {}
fileDetails['../512k']={ "count": '100', "size": '512'}
fileDetails['../1MB']={ "count": '50', "size": '1000'}
fileDetails['../10MB']={ "count": '25', "size": '10000'}
fileDetails['../100MB']={ "count": '10', "size": '100000'}
fileDetails['../1000MB']={ "count": '5', "size": '1000000'}
transferTimeList = []
throughputList = []
numberOfIterations = 1
threads = []
maxthreads = 1
sema = threading.Semaphore(value=maxthreads)
threads = list()


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

def get_size(start_path):
    p = subprocess.Popen(['du', '-ms', start_path], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    return re.sub(r'\t.*', '', out)

def singleFileUpload(value, filePath):
    sema.acquire()
    fileData = open(filePath, 'rb')
    objectList.append(api.create_object(data = fileData.read()))
    fileData.close()
    sema.release()

def uploadFiles():
    start_time = os.times()[4]
    for directory, value in fileDetails.iteritems():
        for num in range(numberOfIterations):
            filenames = os.listdir(directory)
            for fname in filenames:
                t = threading.Thread(target=singleFileUpload, args=(value, directory + '/' + fname,))
                threads.append(t)
                t.start()
        for t in threads:
            t.join()
        end_time = os.times()[4]
        transferTime = ( end_time - start_time )
        throughput = (float(get_size(directory)))/float(transferTime)
        print(value['size'] + ',' + str(transferTime) + ',' + str(throughput))

createDirectories()
createTestFiles()
#listObjects()
uploadFiles()
#print json.dumps(results, sort_keys=True,indent=4, separators=(',', ': '))
#cleanup()

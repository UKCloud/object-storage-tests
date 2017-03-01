AWS_KEY = "131318694590302234@ecstestdrive.emc.com"
AWS_SECRET = "/rDrx8U/Os8KJsbLClrLrDrDuyyV74YbBFrHfm0d"
HOST = 'object.ecstestdrive.com'
PORT = 443
DEBUG=0

import re
import os
import sys
import subprocess
import time
import json
from datetime import datetime
from boto.s3.connection import S3Connection
import threading
import socket

conn = S3Connection(aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET, host=HOST, port=PORT, debug=DEBUG)
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

def createBucket():
    for bucket in fileDetails.keys():
        bucket = re.sub('\.\.\/', "", bucket.lower() )
        bucket = bucket.lower()
        try:
            bucket = conn.get_bucket(bucket)
        except Exception as e:
            if re.match(r".*404 Not Found.*", str(e)):
                conn.create_bucket(bucket)
            else:
                print("couldn't create bucket")

def listObjects():
    for bucket in fileDetails.keys():
        bucket = re.sub('\.\.\/', "", bucket.lower() )
        bucket = bucket.lower()
        bucket = conn.get_bucket(bucket)
        for obj in bucket.list():
            print(obj)

def cleanup():
    for bucket in fileDetails.keys():
        bucket = re.sub('\.\.\/', "", bucket.lower() )
        bucket = conn.get_bucket(bucket)
        bucket.delete_keys(bucket.list())

def get_size(start_path):
    p = subprocess.Popen(['du', '-ms', start_path], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    return re.sub(r'\t.*', '', out)

def singleFileUpload(directory, filename):
    sema.acquire()
    bucket = re.sub('\.\.\/', "", directory.lower() )
    bucket = conn.get_bucket(bucket)
    k = bucket.new_key(filename)
    k.set_contents_from_filename(directory + "/" + filename)
    sema.release()

def uploadFiles():
    start_time = os.times()[4]
    for directory, value in fileDetails.iteritems():
        for num in range(numberOfIterations):
            filenames = os.listdir(directory)
            for fname in filenames:
                t = threading.Thread(target=singleFileUpload, args=(directory, fname,))
                threads.append(t)
                t.start()
        for t in threads:
            t.join()
        end_time = os.times()[4]
        transferTime = ( end_time - start_time )
        throughput = (float(get_size(directory)))/float(transferTime)
        print(value['size'] + ',' + str(transferTime) + ',' + str(throughput))

def downloadFiles():
    for bucket, value in fileDetails.iteritems():
        start_time = os.times()[4]
        directory = bucket
        bucket = re.sub('\.\.\/', "", bucket.lower() )
        bucket = bucket.lower()
        bucket = conn.get_bucket(bucket)
        for obj in bucket.list():
            keyString = str(obj.key)
            obj.get_contents_to_filename(directory + '/' + obj.key)
        end_time = os.times()[4]
        transferTime = ( end_time - start_time )
        throughput = (float(get_size(directory)))/float(transferTime)
        print('download: ' + value['size'] + ',' + str(transferTime) + ',' + str(throughput))

createDirectories()
createTestFiles()
#listObjects()
createBucket()
uploadFiles()
downloadFiles()
#print json.dumps(results, sort_keys=True,indent=4, separators=(',', ': '))
cleanup()

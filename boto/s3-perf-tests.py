AWS_KEY = "131318694590302234@ecstestdrive.emc.com"
AWS_SECRET = "/rDrx8U/Os8KJsbLClrLrDrDuyyV74YbBFrHfm0d"
HOST = 'object.ecstestdrive.com'
 
import re
import os
import sys
import subprocess
import time
import json
from boto.s3.connection import S3Connection

conn = S3Connection(aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET, host=HOST)
results = {}
fileDetails = {}
#fileDetails['512k']={ "count": '100', "size": '512'}
fileDetails['1MB']={ "count": '50', "size": '1000'}
#fileDetails['10MB']={ "count": '25', "size": '10000'}
#fileDetails['100MB']={ "count": '10', "size": '100000'}
#fileDetails['1000MB']={ "count": '5', "size": '1000000'}

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

def createBucket():
    for bucket in fileDetails.keys():
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
        bucket = bucket.lower()
        bucket = conn.get_bucket(bucket)
        for obj in bucket.list():
            print(obj)

def cleanup():
    for bucket in fileDetails.keys():
        bucket = bucket.lower()
        bucket = conn.get_bucket(bucket)
        bucket.delete_keys(bucket.list())

def get_size(start_path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

def uploadFiles():
    start_time = time.time()
    for directory, value in fileDetails.iteritems():
        dirSize = get_size(directory)
        filenames = os.listdir(directory)
        print('Uploading ' + fileDetails[directory]['count'] + " " + directory + ' files')
        for fname in filenames:
            bucket = conn.get_bucket(directory.lower())
            k = bucket.new_key(fname)
            k.set_contents_from_filename(directory + "/" + fname)
        transferTime = ("%i" % (time.time() - start_time))
        throughput = sizeof_fmt(int(dirSize)/int(transferTime))
        results[directory]={'time': transferTime, 'throughput': throughput}

createDirectories()
createBucket()
createTestFiles()
#listObjects()
uploadFiles()
print json.dumps(results, sort_keys=True,indent=4, separators=(',', ': '))
cleanup()

#!/usr/bin/python
import os
import requests
from multiprocessing import Process
import math
import shutil
from optparse import OptionParser
from posixpath import basename, dirname
from urlparse import urlparse
import hashlib


def fetchUrl(url,filePath,userName=None,password=None,threads=None):
    """Downloads a file from an url location with concurrent threads"""
    
    # get file size (we assume header content-length is always present)
    r=requests.head(url,auth=(userName,password))
    
    if not (r.status_code == requests.codes.ok):
        print "HTTP status code %d returned, I'm giving up" % r.status_code
        exit()
    
    if 'content-length' in r.headers:
        fileSize=int(r.headers['content-length'])
    else:
        print "Header 'content-length' not found"
        exit()

    # if number of threads is not defined, make a smart guess depending on file size
    if threads is None:
        if fileSize>10485760: 
            threads=5 
        else:
            threads=1
        
    # calculate the size of every part
    partSize=int(math.ceil(float(fileSize)/float(threads)))

    # dispatch download workers
    procs=[]
    for i in range(threads):
        p=Process(target=__fetchFilePart,args=(i,url,filePath,userName,password,partSize,fileSize))
        procs.append(p)
        p.start()
    
    # Wait for all worker processes to finish
    for p in procs:
        p.join()
        
    # Concatenate file parts to one file and delete file parts
    destination = open(filePath, 'wb')
    for fileName in (filePath+'.part'+str(i) for i in range(0,threads)):
        shutil.copyfileobj(open(fileName, 'rb'), destination)
        os.remove(fileName)   
    destination.close()
    
        
def __fetchFilePart(processId=None,url=None,filePath=None,userName=None,password=None,partSize=None,fileSize=None):
    
    partfileName=filePath+'.part'+str(processId)
    f = open(partfileName, "ab")
    
    # if filepart already exists, resume download
    existingFileSize=0
    if os.path.exists(partfileName):
        existingFileSize=os.path.getsize(partfileName)
    
    chunkSize = 1024    
    startAtByte=processId*partSize+existingFileSize
    stopAtByte=min(((processId+1)*partSize-1),fileSize)
    
    # set the HTTP request header that defines the part of the file we want to download
    headers = {'Range': 'bytes=%d-%d' % (startAtByte,stopAtByte)}
    r=requests.get(url,headers=headers,auth=(userName,password),stream=True)
    

    positionInFile=startAtByte
    while positionInFile < stopAtByte:
        chunkToRead=min(chunkSize,stopAtByte-positionInFile+1)
        data=r.raw.read(chunkToRead)
        f.write(data)
        f.flush()
        positionInFile=positionInFile+chunkToRead
    
    f.close()


# when run from command line
if __name__=='__main__':

    # define command line parameters, default values and usage text
    # @todo: consider to replace with argparse in the future, as optparse is deprecated as of 2.7
    parser = OptionParser("usage: %prog [options] URL")    
    # parser.add_option("-c", "--checksum", type="string", action="store", dest="checksumurl",             metavar="CHECKSUMURL", help="url of checksum file. If provided checksum will be performed after download")    
    parser.add_option("-f", "--file",     type="string", action="store", dest="filePath",                metavar="FILE",        help="path where downloaded content will be stored")
    parser.add_option("-p", "--password", type="string", action="store", dest="password",                                       help="HTTP authentication username")              
    parser.add_option("-t", "--threads",  type="int",    action="store", dest="threads",   default=1,                           help="number of concurrent download threads (default=%default)")              
    parser.add_option("-u", "--username", type="string", action="store", dest="userName",                                       help="HTTP authentication password")              

    (options, args) = parser.parse_args()
    
    # check if url is specified (this is the only positional argument)
    if len(args) != 1:
        parser.error("argument URL not specified or too many positional arguments supplied")
    else:
        url=args[0]
    
    # get fileName from URL if it isn't specified
    if options.filePath is None:
        fileName=basename(urlparse(url).path)
        if len(fileName) is 0:
            parser.error("no output fileName specified and not possible to extract a fileName from the url specified")
        filePath=os.getcwd() + '/' + fileName
    
    fetchUrl(url,fileName,userName=options.userName,password=options.password,threads=options.threads)
    
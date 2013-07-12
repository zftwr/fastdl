#!/usr/bin/python
import os
import requests
from multiprocessing import Process
import math
import shutil


def url2file(url,fpath,userName=None,password=None,conn=5):
    """Downloads a file from an url location with concurrent threads"""
    
    # get file size (we assume header content-length is always present)
    fileSize=int(requests.head(url,auth=(userName,password)).headers['content-length'])
    partSize=int(math.ceil(float(fileSize)/float(conn)))

    # dispatch multiple download workers
    procs=[]
    for i in range(conn):
        procs[i]=Process(target=__downloadFilePart,args=(i,url,fpath,userName,password,partSize,fileSize))
        procs[i].start()
    
    # Wait for all worker processes to finish
    for p in procs:
        p.join()
        
    # Concatenate the files into one file
    destination = open(fpath, 'wb')
    for filename in (fpath+'.part'+str(i) for i in range(0,conn)):
        shutil.copyfileobj(open(filename, 'rb'), destination)
        os.remove(filename)   
    destination.close()
        
def __downloadFilePart(processId,url,fpath,userName,password,partSize,fileSize):
    
    partFileName=fpath+'.part'+str(processId)
    f = open(partFileName, "wb")

    
    chunkSize = 1024    
    startAtByte=processId*partSize
    stopAtByte=min(((processId+1)*partSize-1),fileSize)
    
    headers = {'Range': 'bytes=%d-%d' % (startAtByte,stopAtByte)}
    r=requests.get(url,headers=headers,auth=(userName,password),stream=True)
    
    positionInFile=startAtByte
    
    while positionInFile < stopAtByte:
        chunkToRead=min(chunkSize,stopAtByte-positionInFile+1)
        #print "id: %d | posInFile: %d | chunk2read: %d | start: %d | stop: %d | fileSize: %d" % (processId,positionInFile,chunkToRead,startAtByte,stopAtByte,fileSize)
        data=r.raw.read(chunkToRead)
        f.write(data)
        positionInFile=positionInFile+chunkToRead
    
    f.close()


# when run from command line
if __name__=='__main__':
    """Run some tests when run from command line"""
    url2file('http://a4.mzstatic.com/us/r1000/118/Purple2/v4/72/f6/e1/72f6e1b2-6222-3920-cf96-574c07b568cf/mzl.ewtvoyav.512x512-75.jpg','test.jpg')       
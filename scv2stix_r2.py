# Please use exported csv file from Kibana visualization that include columns at least @timestamp, request.keyword and count.

from datetime import datetime
from stix2 import Sighting
from os import path
import sys
import csv
from elasticsearch import Elasticsearch

es = Elasticsearch(sys.argv[1])

def readcsvfile(csvfile):
    datalist = []
    current_dir = path.dirname(__file__)
    f = open(path.join(current_dir, csvfile),'r',encoding='utf-8')
    reader = csv.reader(f)
    header = next(reader)
    # get column info
    for n in range(len(header)):
        if '@timestamp:' in header[n]:
            timen = n
        elif 'request.keyword:' in header[n]:
            reqestn = n
        elif 'Count' in header[n]:
            countn = n

    for row in reader:
        if getid(row[reqestn]) != None:
            datalist.append([getid(row[reqestn]), int(row[timen]), row[reqestn], int(row[countn])])
        else:
            print(row[reqestn] + ' is not on STIX index.')
    createdata(datalist)
    f.close()

def createdata(datalist):
    firstseen = 9999999999999
    lastseen = 0000000000000
    id = datalist[0][0]
    count = 0
    nextlist = []

    for dataone in datalist:
        if id == dataone[0]:
            if firstseen > dataone[1]:
                firstseen = dataone[1]
            if lastseen < dataone[1]:
                lastseen = dataone[1]
            count = count + dataone[3]
        else:
            nextlist.append(dataone)
    createsighting(id,firstseen,lastseen,count)
    if not nextlist == []:
        createdata(nextlist)


def createsighting(id,firstseen,lastseen,count):
    firstseen = createzulu(firstseen)
    lastseen = createzulu(lastseen)
    sighting = Sighting(sighting_of_ref=id,first_seen=firstseen,last_seen=lastseen,count=count)
    print(sighting)


def getid(requestv):
    # extract domain or IP address information from stix index
    res = es.search(index='stix', body={"from": 0, "size": 10000, 'query': {'match_all': {}}})
    for hit in res['hits']['hits']:
        if 'domain' in hit['_source']:
            stixdomain = hit['_source']['domain']
            if stixdomain in requestv:
                id = hit['_source']['id']
        if 'IPaddress' in hit['_source']:
            stixdomain = hit['_source']['IPaddress']
            if stixdomain in requestv:
                id = hit['_source']['id']
    if 'id' in locals():
        return id

def createzulu(unixtime):
    validtime = datetime.utcfromtimestamp(int(str(unixtime)[0:10])).isoformat() + '.000Z'
    return validtime

if __name__ == '__main__':
    readcsvfile(sys.argv[2])
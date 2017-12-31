# Please use exported csv file from Kibana visualization

from datetime import datetime
from stix2 import Sighting
import pandas as pd
from os import path
import sys
import csv
from elasticsearch import Elasticsearch

es = Elasticsearch(sys.argv[1])
df = pd.DataFrame(columns = ['id','time','request','count'])

def readcsvfile(csvfile):
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
        creatdf(row[timen], row[reqestn], row[countn])
    createsighting(df)
    f.close()

def creatdf(timev, requestv, countv):
    global df
    df = df.append(pd.Series([getid(requestv),timev,requestv,int(countv)], index=df.columns), ignore_index = True)

def createsighting(df):
    firstseen = df.groupby('id')['time'].min()
    lastseen = df.groupby('id')['time'].max()
    totalcount = df.groupby('id')['count'].sum()
    print(firstseen,lastseen,totalcount)

def extract_data(df):
    print(df.pivot_table(df,index='id',columns='time'))
    # unixtime = row[0]
    # clientip = row[1]
    # patterninfo = row[2]
    # validtime = datetime.utcfromtimestamp(int(str(unixtime)[0:10])).isoformat() + '.000Z'
    # sighting = Sighting(name="test",
    #                       labels=["malicious-activity"],
    #                       pattern="[domain-name:value = " + patterninfo+ "]",
    #                       valid_from=validtime)

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

    return id

if __name__ == '__main__':
    readcsvfile(sys.argv[2])
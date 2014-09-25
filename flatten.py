#!/usr/bin/python
# -*- coding:utf-8 -*-
import os, sys
import time
import pprint
import traceback
import MySQLdb
import elasticsearch
from gp import client

EL_HOST="localhost"
EL_PORT=9200
EL_INDEX="gptest1wiki"
GP_HOST="localhost"
GP_PORT=6666
GP_GRAPH="gptest1wiki"
SQL_HOST="localhost"
SQL_PORT=3306
SQL_DB="gptest1wiki"

pp= pprint.PrettyPrinter(indent=2)

def getParentcats(categories, gp, cursor):
    paramfmt= ','.join(['%s']*len(categories))
    sqlstr= "select page_id,page_title from page where page_namespace=14 and page_title in (%s)" % paramfmt
    params= [ cat.encode('utf-8') for cat in categories ]
    #~ print sqlstr, params
    cursor.execute(sqlstr, params)
    sqlres= cursor.fetchall()
    #~ pp.pprint(sqlres)
    totalcats= dict()
    for row in sqlres:
        cat_id= row[0]
        parentcats= gp.capture_traverse_predecessors_withdepth(cat_id, 1000)
        #~ print parentcats
        if parentcats!=None:
            for cat in parentcats:
                if not str(cat[0]) in totalcats or totalcats[str(cat[0])] > cat[1]+1:
                    totalcats[str(cat[0])]= cat[1]+1
    return totalcats

    
def updateParents(hit, es, gp, cursor):
    #~ print "%s (%s) in index %s" % (hit["fields"]["title"], hit["_id"], hit["_index"])
    if "category" in hit["fields"]:
        totalcats= getParentcats(hit["fields"]["category"], gp, cursor)
    else:
        totalcats= dict()
    body= { "script": "ctx._source.parent_categories= %s" % str(totalcats) }
    es.update(index=hit["_index"], doc_type="page", id=hit["_id"], body=body)

if __name__=='__main__':
    es= elasticsearch.Elasticsearch(hosts=[ { "host": EL_HOST, "port": EL_PORT } ])
    gp= client.Connection(client.ClientTransport(GP_HOST, int(GP_PORT)))
    gp.connect()
    gp.use_graph(GP_GRAPH)
    sql= MySQLdb.connect(read_default_file=os.path.expanduser("~/.my.cnf"), host=SQL_HOST, port=SQL_PORT, db=SQL_DB)
    cursor= sql.cursor()
    
    es.indices.put_mapping(index=EL_INDEX, doc_type="page", body= { "dynamic": "true" })
    
    scrollfrom= 0
    begintime= time.time()
    while True:
        iterbegintime= time.time()
        res= es.search(index=EL_INDEX, doc_type="page", q="!_exists_:parent_categories", fields=["_id", "title", "category"], size=50, from_=scrollfrom)
        if not len(res["hits"]["hits"]): 
            break
        for hit in res["hits"]["hits"]:
            try:
                updateParents(hit, es, gp, cursor)
            except Exception as ex:
                traceback.print_exc(file=sys.stdout)
                print("exception caught, continuing...")
        scrollfrom+= len(res["hits"]["hits"])
        print(" * processed %s hits of %s at %.2f/sec (%.2f/sec)" % 
            (scrollfrom, res["hits"]["total"], scrollfrom/(time.time()-begintime), len(res["hits"]["hits"])/(time.time()-iterbegintime)))
        sys.stdout.flush()
    


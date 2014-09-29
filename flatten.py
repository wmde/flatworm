#!/usr/bin/python
# -*- coding:utf-8 -*-
import os, sys
import time
import pprint
import traceback
import MySQLdb
import elasticsearch
import elasticsearch.helpers
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

BULK_ENABLED=False
BULK_CHUNK_SIZE=500
TARGET_FIELD="parent_categories"

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

# from http://stackoverflow.com/questions/1038824
def strip_suffix(text, suffix):
    if not text.endswith(suffix):
        return text
    return text[:len(text)-len(suffix)]
    
def makeBulkUpdateAction(hit, gp, cursor):
    if "category" in hit["fields"]:
        parentcats= getParentcats(hit["fields"]["category"], gp, cursor)
    else:
        parentcats= dict()
    #~ print("makeBulkUpdateAction: %s in index %s" % (hit["fields"]["title"], hit["_index"]))
    parentcats["dummy"]= 1   # we add this because empty dicts confuse elasticsearch, end up as empty lists in the index, and are ignored in "q=_exists_" searches...
    action= { 
        "_op_type": "update",
        "_index": strip_suffix(hit["_index"]),
        "_type": "page",
        "_id": hit["_id"],
        "script": "ctx._source.%s= %s" % (TARGET_FIELD, str(parentcats))
    }
    return action

def updateParents(hit, es, gp, cursor):
    #~ print "%s (%s) in index %s" % (hit["fields"]["title"], hit["_id"], hit["_index"])
    if "category" in hit["fields"]:
        totalcats= getParentcats(hit["fields"]["category"], gp, cursor)
    else:
        totalcats= dict()
    totalcats["dummy"]= 1   # we add this because empty dicts confuse elasticsearch, end up as empty lists in the index, and are ignored in "q=_exists_" searches...
    body= { "script": "ctx._source.remove(\"%s\"); ctx._source.%s= %s" % (TARGET_FIELD, TARGET_FIELD, str(totalcats)) }
    #~ body= { "script": "ctx._source.remove(\"%s\")" % TARGET_FIELD }
    es.update(index=strip_suffix(hit["_index"], "_first"), doc_type="page", id=hit["_id"], body=body)

if __name__=='__main__':
    es= elasticsearch.Elasticsearch(hosts=[ { "host": EL_HOST, "port": EL_PORT } ])
    gp= client.Connection(client.ClientTransport(GP_HOST, int(GP_PORT)))
    gp.connect()
    gp.use_graph(GP_GRAPH)
    sql= MySQLdb.connect(read_default_file=os.path.expanduser("~/.my.cnf"), host=SQL_HOST, port=SQL_PORT, db=SQL_DB)
    cursor= sql.cursor()
    
    es.indices.put_mapping(index="_all", doc_type="page", body= { "dynamic": "true" })
    
    res= es.count(index=EL_INDEX, doc_type="page", q="!_exists_:%s" % TARGET_FIELD)
    count= res["count"]
    
    print("getting pages without '%s' field, approx. count: %s..." % (TARGET_FIELD, count))
    scroll= elasticsearch.helpers.scan(es, doc_type="page", fields=["_id", "title", "category"], q="!_exists_:%s" % TARGET_FIELD)
    begintime= time.time()
    bulkactions= []
    hits_processed= 0
    for hit in scroll:
        if BULK_ENABLED:
            bulkactions.append(makeBulkUpdateAction(hit, gp, cursor))
        else:
            updateParents(hit, es, gp, cursor)
            print "%5d/%d... (%.2f/sec.)               \r" % (hits_processed, count, hits_processed/(time.time()-begintime)), 
        hits_processed+= 1
        if len(bulkactions)==BULK_CHUNK_SIZE:
            print(" * running bulk update...")
            sys.stdout.flush();
            r= elasticsearch.helpers.bulk(es, bulkactions, request_timeout=60*2)
            print "   bulk result: ",
            pp.pprint(r)
            print(" * processed %s of approx %s hits at %.2f/sec" % 
                (hits_processed, count, hits_processed/(time.time()-begintime)))
            bulkactions= []
    if len(bulkactions):
        print(" * running bulk update...")
        sys.stdout.flush();
        r= elasticsearch.helpers.bulk(es, bulkactions, request_timeout=60*2)
        print "   bulk result: ",
        pp.pprint(r)
        es.indices.flush(index="_all")
        bulkactions= []
    print("")


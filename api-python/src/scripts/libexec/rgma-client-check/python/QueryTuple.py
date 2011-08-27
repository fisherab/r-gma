#!/usr/bin/env python

import sys, time, rgma

if len(sys.argv) != 2:
    print >> sys.stderr, "Must have one argument"
    sys.exit()

select = "SELECT aString FROM Default.userTable WHERE userId='%s'" % sys.argv[1]
consumer = rgma.Consumer(select, rgma.QueryType.H)

numResults = 0    
while 1:
    results = consumer.pop(50)
    numResults += len(results.getData())        
    if results.isEndOfResults():
        break
    time.sleep(1)
consumer.close()
if numResults != 1: print >> sys.stderr, numResults, "tuples returned rather than 1"

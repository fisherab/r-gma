#!/usr/bin/env python

import sys, time
import rgma

try:
    
    historyPeriod = rgma.TimeInterval(10, rgma.TimeUnit.MINUTES)
    timeout = rgma.TimeInterval(5, rgma.TimeUnit.MINUTES)
    select = "SELECT userId, aString, aReal, anInt, RGMATimestamp " \
             "FROM default.userTable"
    consumer = rgma.Consumer(select, 
                             rgma.QueryTypeWithInterval.C, historyPeriod, 
                             timeout)

    endOfResults = False
    while not endOfResults:
        tupleSet = consumer.pop(2000)
        data = tupleSet.getData()
        if len(data) == 0:
            time.sleep(2)
        else:
            for tuple in data:
                print "userId=" + tuple.getString(0) + ",",
                print "aString=" + tuple.getString(1) + ",",
                print "aReal=" + `tuple.getFloat(2)` + ",",
                print "anInt=" + `tuple.getInt(3)`
        endOfResults = tupleSet.isEndOfResults()
        
    consumer.close()
  
except rgma.RGMAException, e:
    sys.stderr.write("RGMAException " +  e.getMessage() + "\n")
    sys.exit(1)
            

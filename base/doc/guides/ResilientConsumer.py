#!/usr/bin/env python

import sys, time
import rgma

try:

    select = "SELECT userId, aString, aReal, anInt FROM default.userTable"

    consumer = None
    while not consumer:
        try:
            consumer = rgma.Consumer(select,
                                     rgma.QueryTypeWithInterval.C,
                                     rgma.TimeInterval(30))
        except rgma.RGMATemporaryException, e:
            sys.stderr.write("RGMATemporaryException " +  e.getMessage() + 
                             " will retry in 60s\n")
            time.sleep(60)

    while True:
        try:
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
            if tupleSet.getWarning() != "":
                print "WARNING: " + tupleSet.getWarning()
        except rgma.RGMATemporaryException, e:
            sys.stderr.write("RGMATemporaryException " +  e.getMessage() + 
                             " will retry in 60s\n")
            time.sleep(60)

except rgma.RGMAPermanentException, e:
    sys.stderr.write("RGMAPermanentException " +  e.getMessage() + "\n")
    sys.exit(1)
      

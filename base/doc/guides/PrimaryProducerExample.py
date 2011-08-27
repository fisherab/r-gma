#!/usr/bin/env python

import sys
import rgma

if len(sys.argv) != 2:
    sys.stderr.write("Exactly one argument must be specified\n")
    sys.exit(1)

userId = sys.argv[1]

try:
    producer = rgma.PrimaryProducer(rgma.Storage.getMemoryStorage(), 
                                    rgma.SupportedQueries.C)

    predicate =  "WHERE userId = '%s'" % userId
    historyRetentionPeriod = rgma.TimeInterval(60, rgma.TimeUnit.MINUTES)
    latestRetentionPeriod = rgma.TimeInterval(60, rgma.TimeUnit.MINUTES)
    producer.declareTable("default.userTable", predicate,
                          latestRetentionPeriod, historyRetentionPeriod)

    insert = "INSERT INTO default.userTable " \
             "(userId, aString, aReal, anInt) " \
             "VALUES ('%s', 'Python producer', 3.1415962, 42)" % userId
    producer.insert(insert)
    
    producer.close()

except rgma.RGMAException, e:
    sys.stderr.write("RGMAException " +  e.getMessage() + "\n")
    sys.exit(1)

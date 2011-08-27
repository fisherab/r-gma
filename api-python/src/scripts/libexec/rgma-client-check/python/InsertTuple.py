#!/usr/bin/env python

import sys, time, rgma

if len(sys.argv) != 2:
    print >> sys.stderr, "Must have one argument"
    sys.exit()

producer = rgma.PrimaryProducer(rgma.Storage.getMemoryStorage(), rgma.SupportedQueries.CH)

predicate =  "WHERE userId = '%s'" % sys.argv[1]
latestRetentionPeriod = historyRetentionPeriod = rgma.TimeInterval(10, rgma.TimeUnit.MINUTES)
producer.declareTable("Default.userTable",
                      predicate,
                      latestRetentionPeriod,
                      historyRetentionPeriod)

insert = "INSERT INTO Default.userTable (userId, aString, aReal, anInt) \
      VALUES ('%s', 'Python producer', 3.1415962, 42)" % sys.argv[1]
producer.insert(insert)
producer.close()

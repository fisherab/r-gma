#!/usr/bin/env python

import sys, time
import rgma

if len(sys.argv) != 2:
   sys.stderr.write("Exactly one argument must be specified\n")
   sys.exit(1)
   
userId = sys.argv[1]

try:
    
   producer = None
   while not producer:
      try:
         producer = rgma.PrimaryProducer(rgma.Storage.getMemoryStorage(), 
                                         rgma.SupportedQueries.C)
      except rgma.RGMATemporaryException, e:
         sys.stderr.write("RGMATemporaryException " +  e.getMessage() + 
                          " will retry in 60s\n")
         time.sleep(60)

   predicate =  "WHERE userId = '%s'" % userId
   historyRetentionPeriod = rgma.TimeInterval(50, rgma.TimeUnit.MINUTES)
   latestRetentionPeriod = rgma.TimeInterval(25, rgma.TimeUnit.MINUTES)
   
   tableDeclared = False   
   while not tableDeclared:
      try:
         producer.declareTable("default.userTable",
                                predicate,
                                latestRetentionPeriod,
                                historyRetentionPeriod)
         tableDeclared = True
      except rgma.RGMATemporaryException, e:
         sys.stderr.write("RGMATemporaryException " +  e.getMessage() + 
                          " will retry in 60s\n")
         time.sleep(60)

   data = 0             
   while True:
      try:
         insert = "INSERT INTO default.userTable " \
                   "(userId, aString, aReal, anInt) " \
                   "VALUES ('%s', 'resilient Python producer', " \
                   "0.0, %d)" % (userId, data)
         producer.insert(insert)
         print insert
         data += 1
         time.sleep(30)
      except rgma.RGMATemporaryException, e:
         sys.stderr.write("RGMATemporaryException " +  e.getMessage() + 
                          " will retry in 60s\n")
         time.sleep(60)
      
except rgma.RGMAPermanentException, e:
   sys.stderr.write("RGMAPermanentException " +  e.getMessage() + "\n")
   sys.exit(1)

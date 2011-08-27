#!/usr/bin/env python

import sys, time
import rgma

sp = None
try:
    
    try:
        
        location = "pythonExample"
        storage = rgma.Storage.getDatabaseStorage(location)
        sp = rgma.SecondaryProducer(storage, rgma.SupportedQueries.CL)
        
        predicate = ""
        historyRetentionPeriod = rgma.TimeInterval(2, rgma.TimeUnit.HOURS)
        sp.declareTable("default.userTable", predicate,
                        historyRetentionPeriod)
        
        sleepSecs = rgma.RGMAService.getTerminationInterval().getValueAs()/3
        while True:
            sp.showSignOfLife()
            time.sleep(sleepSecs)
                            
    except rgma.RGMAException, e:
        sys.stderr.write("RGMAException " +  e.getMessage() + "\n")
        sys.exit(1)
       
finally:
    if sp:
        sp.close()




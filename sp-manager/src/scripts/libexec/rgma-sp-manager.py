#!/usr/bin/env python

import sys, os, glob, re, rgmasp, datetime

class Archiver:
    
    def __init__(self, archiverName):
        self.archiverName = archiverName
        
    def updatedConfig(self):
        return os.path.getmtime(os.path.join(var, self.archiverName + ".id")) < os.path.getmtime(os.path.join(etc, self.archiverName))
        
    def start(self):
        try:
            rgmasp.start(os.path.join(var, self.archiverName + ".id"), os.path.join(etc, self.archiverName))
            log("Secondary producer with config name " + self.archiverName + " started")
        except rgmasp.SPError, e:
            log("Secondary producer with config name " + self.archiverName + " failed to start: " + e.args[0])    
    
    def ping(self):
        try:
            rgmasp.ping(os.path.join(var, self.archiverName + ".id"))
        except rgmasp.SPError, e:
            log("Secondary producer with config name " + self.archiverName + " did not ping : " + e.args[0])
            try:
                self.stop()
            except rgmasp.SPError:
                pass
            try:
                self.start()
            except rgmasp.SPError:
                pass
    
    def stop(self):
        try:
            rgmasp.stop(os.path.join(var, self.archiverName + ".id"))
            log("Secondary producer with config name " + self.archiverName + " stopped")
        except rgmasp.SPError, e:
            log("Secondary producer with config name " + self.archiverName + " failed to stop: " + e.args[0])    
    
def log(msg):
    print datetime.datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S"), "-", msg
  
def main():    
    # Try to get lock
    lock = os.path.join(var, "lock")
    if os.path.isfile(lock):
        log("Lock file exists - cannot continue")
        sys.exit(1)
    try:
        f = open(lock,"w")
        f.write(str(os.getpid()))
        f.close()
    except Exception, e:
        log("Unable to open lock file for write - cannot continue " + `e`)
        sys.exit(1)
    
    archiverExists = {}
    os.chdir(etc)
    files = glob.glob("*")
    confExists = {}
    for file in files:
        archiverName = file
        confExists[archiverName] = None
        archiverExists[archiverName] = None
    os.chdir(var)
    files = glob.glob("*.id")
    idExists = {}
    for file in files:
        archiverName = file[:-3]
        idExists[archiverName] = None
        archiverExists[archiverName] = None
    for archiverName in archiverExists.keys():
        archiver = Archiver(archiverName)
        if not archiverName in confExists:
            archiver.stop()
        elif not archiverName in idExists:
            archiver.start()
        elif archiver.updatedConfig():
            archiver.stop()
            archiver.start()
        else:
            archiver.ping()

    # Give up lock
    os.remove(lock)

# Define some globals and call main    
rgma_home = sys.argv[1]
etc = os.path.join(rgma_home, "etc", "rgma-sp-manager")
var = os.path.join(rgma_home, "var", "rgma-sp-manager")    
main()

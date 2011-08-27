#!/usr/bin/env python

# Script to run some confidence tests on a new R-GMA installation. Exits with status 0 on success, 1 on error.

import sys, os, socket, re, time, urllib

def getValueFromList(list, prop):
    list = list.split()
    for item in list:
        if re.match(prop, item):
            return item.split("\"")[1]

hostname = socket.getfqdn()
print "Running R-GMA server tests"

rgma_home = os.environ["RGMA_HOME"]

for filename in [ "client-acl.txt", "log4j.properties", "rgma-server.props", "service-version.txt", "ServletAuthentication.props"]:
    ff = os.path.join(rgma_home, "etc", "rgma-server", filename)
    if not os.path.isfile(ff): sys.exit("File " + ff + " is missing")

fd = os.path.join(rgma_home, "var", "rgma-server", "vdb")
if not os.path.isdir(fd): sys.exit("Directory " + fd + " is missing")
vdbCount = len(os.listdir(fd))
if vdbCount == 0: print " - WARNING: No vdbs are defined in " + fd
print " - There are " + `vdbCount` + " vdbs defined"
    
try:
    HostKeyFile=os.path.join("/", "etc", "grid-security", "hostkey.pem")
    HostCertFile=os.path.join("/", "etc", "grid-security", "hostcert.pem")
    url = urllib.URLopener(key_file=HostKeyFile, cert_file=HostCertFile)
    connection = url.open("https://127.0.0.1:8443/R-GMA/ConsumerServlet/getProperty?name=ServiceStatusDetails")
    response = connection.read()

    print " - Successfully connected to Server"
    print " - RGMA version:", getValueFromList(response, "ServiceVersion")
    print " - JVM version:", getValueFromList(response, "JVMVersion")
    print " - Server time:", time.strftime("%H:%M:%S %d-%m-%Y", time.gmtime(int(getValueFromList(response, "ServiceTimeMillis"))/1000)) + " GMT"
    print " - Percentage of heap in use (measured after GC):",  getValueFromList(response, "JVMGCHeapUsePercentage")
    print " - Current request count (probably 1 if lightly loaded):", getValueFromList(response, "CurrentRequestCount")
    print " - HighestRequestCount (never reset):", getValueFromList(response, "HighestRequestCount")

    seconds = (long(getValueFromList(response, "ServiceTimeMillis")) - long(getValueFromList(response, "ServiceStartTimeMillis")))/1000
    minutes = seconds/60
    seconds = int(seconds - minutes * 60)
    hours = int(minutes/60)
    minutes = int(minutes - hours *60)
    print " - Server has been running for", hours, "hours", minutes, "minutes", seconds, "seconds"
    sc = getValueFromList(response, "StatusCode")
    if (sc != "OK"):
        print " - Status is", sc, ":", getValueFromList(response, "ServiceMessage")
except Exception, e:
    print >> sys.stderr, e
    print >> sys.stderr, "Can't connect to R-GMA server. Tomcat can take a little while to start up."
    sys.exit(1)

print " - R-GMA server test successful"



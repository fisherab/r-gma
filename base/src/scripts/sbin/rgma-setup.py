#!/usr/bin/env python

# rgma-setup
#
# This script configures an RGMA installation - running other scripts with default values. 
# If you need more control then run the scripts individually.
#
# Copyright (c) Members of the EGEE Collaboration. 2004-2009.
#

import sys, os, optparse, threading, socket, time

def runSafe(cmd):
    stdout, stderr = runSafe2(cmd)
    if stderr: abend(stderr)
    return stdout

def runSafe2(cmd):
    stdin, stdout, stderr = os.popen3(cmd)        
    stdin.close()
    stdoutReader = NonBlockingReader(stdout)
    stderrReader = NonBlockingReader(stderr)
    stdoutReader.start()
    stderrReader.start()
    stdoutReader.join()
    stderrReader.join()
    return stdoutReader.read(), stderrReader.read()
    
class NonBlockingReader(threading.Thread):
    def __init__(self, file):
        threading.Thread.__init__(self)
        self.file = file
        
    def run(self):
        self.result = self.file.read()

    def read(self):
        return self.result.strip()
    
def check(options, n):
    v = eval("options." + n)
    if v == "":
        abend(n + " must be specified")

def callOne(message, cmd, silent):
    if not silent: print "\nInvoking [" + cmd + "] " + message
    out, err = runSafe2(cmd)
    if not silent: 
        if out: print out
    if err: abend(err)
    
def abend(msg):
    print >> sys.stderr, "ERROR...", msg
    sys.exit(1)

parser = optparse.OptionParser(usage="usage: %prog [options]", description="""
Sets up an R-GMA installation. 
""")

hostname = socket.getfqdn()
if os.name == "nt":
    defaultGliteLocation = "\\glite"
    defaultTomcatService = "tomcat5"
else:
    defaultGliteLocation = "/opt/glite"
    distro = runSafe("lsb_release -i").split(":")[1].strip()
    if distro in ["Debian"]:
        defaultTomcatService = "tomcat5.5"
    else:
        defaultTomcatService = "tomcat5"    
    
parser.add_option("--tomcat_service", help="the name of the tomcat service [" + defaultTomcatService + ']', default = defaultTomcatService)
parser.add_option("--DBAdminUser", help="admin user for DB [root]", default = "root")
parser.add_option("--DBAdminPassword", help="admin password for DB []", default = "")
parser.add_option("--hostname", help="full DNS name of server [" + hostname + ']', default = hostname)
parser.add_option("--server_port", help="server's port number [8443]", default = "8443")
parser.add_option("--streaming_port", help="server's streaming port number [8088]", default = "8088")
parser.add_option("--silent", help="execute quietly", action="store_true")
parser.add_option("--glite_location", help="where the glite directory tree is rooted [" + defaultGliteLocation +']', default=defaultGliteLocation)
parser.add_option("--rgma_home", help="where the rgma directory tree is rooted [<glite_location>]")
parser.add_option("--http_proxy", help="url of a web proxy if required to access web pages []", default = None)

options, args = parser.parse_args()
if (len(args) != 0):
    abend("There must be no arguments")
if options.silent:
    optionsString = 'True'
else:
    optionsString = 'False'
    
glite_location = options.glite_location
rgma_home = options.rgma_home
if not rgma_home: rgma_home = glite_location

libexec = os.path.join(rgma_home, "libexec")

# Check that sufficient options have been set"
actionFound = False
server = os.path.join(libexec, "rgma-server-setup.py")
if os.path.exists(server):
    check(options, "DBAdminPassword")
    check(options, "rgma_home")
    actionFound = True
    
browser = os.path.join(libexec, "rgma-browser-setup.py")
if os.path.exists(browser):
    check(options, "rgma_home")
    if not os.path.exists(server): abend("The browser cannot be installed as there is no server present")
    actionFound = True

cli = os.path.join(libexec, "rgma-command-line-setup.py")
if os.path.exists(cli): actionFound = True

client = os.path.join(libexec, "rgma-client-setup.py")
if os.path.exists(client):
    check(options, "rgma_home")
    check(options, "glite_location") 
    actionFound = True
    
sp = os.path.join(libexec, "rgma-sp-setup.py")
if os.path.exists(sp):
    check(options, "rgma_home")
    actionFound = True
        
spManager = os.path.join(libexec, "rgma-sp-manager-setup.py")
if os.path.exists(spManager):
    check(options, "rgma_home")
    if not os.path.exists(sp): abend("The archiver-setup cannot be installed as there is no sp present")
    actionFound = True
    
trustfile = os.path.join(rgma_home, "etc", "rgma-server", "ServletAuthenticationProps")
if client and not server and not "X509_USER_PROXY" in os.environ and not "TRUSTFILE" in os.environ:
    abend("Neither TRUSTFILE nor X509_USER_PROXY is set")
  
if not actionFound: abend("There are no R-GMA components found in rgma_home: " + rgma_home)

sys.path.append(os.path.join(rgma_home, "lib", "python"))
from rgmautils import *
    
# Now run them
if os.path.exists(server):
    cmd = server + " --tomcat_service=" + options.tomcat_service + " --server.port=" + options.server_port 
    cmd = cmd + " --streamingreceiver.port=" + options.streaming_port
    if options.http_proxy:
        cmd = cmd + " --http_proxy=" + options.http_proxy
    if options.silent:
        cmd = cmd + " --silent"
    cmd = cmd + " " + rgma_home + " " + options.DBAdminUser + " " + options.DBAdminPassword
    callOne("to setup server", cmd, options.silent)

if os.path.exists(browser):
    cmd = browser + " --tomcat_service=" + options.tomcat_service + " " + rgma_home 
    callOne("to setup browser", cmd, options.silent)
    
if os.path.exists(server):
    tomcat_service = options.tomcat_service
    if os.name == "nt":
        cmd = "net stop " + tomcat_service
        if not options.silent: print "\nInvoking [" + cmd + "] to stop tomcat"
        out, err = runSafe2(cmd)
        if not options.silent: print out
        if not options.silent: print err
        callOne("to start tomcat", "net start " + tomcat_service, options.silent)
    else:
        if distro in ["Debian"]:
            print " - *** Should restart tomcat but doing it the normal way makes this script hang on Debian ***"
        else:
            callOne("to restart tomcat", "/etc/init.d/" + tomcat_service + " restart", options.silent)
    if not options.silent: print " - Tomcat service " + tomcat_service + " restarted" 
 
if os.path.exists(cli):
    cmd = cli + " " + rgma_home
    callOne("to set up cli", cmd, options.silent)
    
if os.path.exists(client):      
    cmd = client + " " + rgma_home + " " + options.hostname + " " + options.server_port + " " + optionsString  + " " + glite_location
    callOne("to set up clients", cmd, options.silent)
    
if os.path.exists(sp):      
    cmd = sp + " " + rgma_home + " " + optionsString
    callOne("to set up sp", cmd, options.silent)
        
if os.path.exists(spManager):      
    cmd = spManager + " " + rgma_home + " " + optionsString
    callOne("to set up sp-manager", cmd, options.silent)

if os.path.exists(server):
    nsecs = 10
    if not options.silent:
        print "\nWaiting for 10 seconds to avoid having the server reject requests immediately after restart"
        for i in range(nsecs):
            time.sleep(1)
            print ".",
        print
    else:
        time.sleep(nsecs)
    callOne("to check server", os.path.join(rgma_home, "sbin", "rgma-server-check"), options.silent)

if os.path.exists(client):
    if not options.silent: print "\nChecking clients"
    checkClients(rgma_home, glite_location, options.silent)

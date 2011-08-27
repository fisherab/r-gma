#!/usr/bin/env python

# rgma-remove
#
# This script attempts to remove all rgma related files whether or not 
# associated with a package manager
#
# Copyright (c) Members of the EGEE Collaboration. 2004-2009.
#

import sys, os, optparse, threading, glob, shutil

def terminate(msg):
    print >> sys.stderr, "ERROR...", msg
    sys.exit(1)

def runSafe(cmd):
    stdout, stderr = runSafe2(cmd)
    if stderr: terminate(stderr)
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

parser = optparse.OptionParser(usage="usage: %prog [options]", description="""
Cleans up an R-GMA installation. 
""")

if os.name == "nt":
    defaultRgmaLocation = "\\glite"
    distro = None
else:
    defaultRgmaLocation = "/opt/glite"
    distro = runSafe("lsb_release -i").split(":")[1].strip()
    
parser.add_option("--silent", help="execute quietly", action="store_true")
parser.add_option("--rgma_home", help="where the rgma directory tree is rooted [" + defaultRgmaLocation + ']', default=defaultRgmaLocation)
#parser.add_option("--packages", help="also remove those files typically installed by a package manager - e.g. rpms", action="store_true")

options, args = parser.parse_args()
if (len(args) != 0):
    terminate("There must be no arguments")
if options.silent:
    optionsString = 'True'
else:
    optionsString = 'False'
    
rgma_home = options.rgma_home
silent = options.silent

def zap(file, silent):
    if os.path.isdir(file):
        shutil.rmtree(file)
    else:
        os.remove(file)
    if not silent: print "Removed", file

# Now do it
if os.name != "nt":       
    for file in glob.glob(os.path.join("/etc", "cron.d", "rgma*")):
        zap(file, silent)
        
    for file in glob.glob(os.path.join("/etc", "logrotate.d", "rgma*")):
        zap(file, silent)
            
if os.path.exists(rgma_home):  
    for file in glob.glob(os.path.join(rgma_home, "bin", "rgma*")):
        zap(file, silent)
        
    for file in glob.glob(os.path.join(rgma_home, "sbin", "rgma*")):
        zap(file, silent)
    
    for file in glob.glob(os.path.join(rgma_home, "etc", "rgma*")):
        zap(file, silent)
        
    for file in glob.glob(os.path.join(rgma_home, "var", "rgma*")):
        zap(file, silent)
        
    for file in glob.glob(os.path.join(rgma_home, "var", "proxies", "rgma*")):
        zap(file, silent)
        
    for file in glob.glob(os.path.join(rgma_home, "lib", "python", "rgma*")):
        zap(file, silent)
        
    for file in glob.glob(os.path.join(rgma_home, "libexec", "rgma*")):
        zap(file, silent)
        
    for file in glob.glob(os.path.join(rgma_home, "log", "rgma*")):
        zap(file, silent)
        
    for file in glob.glob(os.path.join(rgma_home, "share", "doc", "rgma*")):
        zap(file, silent)
        
    for file in glob.glob(os.path.join(rgma_home, "share", "java", "*-rgma-*")):
        zap(file, silent)
        
    for file in glob.glob(os.path.join(rgma_home, "include", "rgma*")):
        zap(file, silent)
        
    for file in glob.glob(os.path.join(rgma_home, "lib", "*-rgma-*")):
        zap(file, silent)

else:
    print "Warning", rgma_home, "does not exist."





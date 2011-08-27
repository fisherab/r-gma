#!/usr/bin/env python

# rgma-client-setup.py
#
# A script to set up an R-GMA client.
#

import os, sys, optparse, socket

def terminate(msg):
    print "ERROR...", msg
    sys.exit(1)

def main():
    parser = optparse.OptionParser(usage="usage: %prog <rgma_home> <hostname> <port> <silent> <glite_location>", description="""
    Sets up an R-GMA installation. 
    """)
    
    options, args = parser.parse_args()
    if (len(args) != 5):
        terminate("There must be five arguments")

    rgma_home, hostname, port, silentValue, glite_location = args
    silent = silentValue == "True"

    if not silent: print "\nSetting up R-GMA clients"

    fname = os.path.join(rgma_home, "etc", "rgma", "rgma.conf")
    if not os.path.exists(os.path.dirname(fname)): os.makedirs(os.path.dirname(fname))
    file = open(fname, "w")
    file.write("hostname=" + hostname + "\n")
    file.write("port=" + port + "\n")
    file.close()
    if not silent: print " - Written out", fname
    
    nt = os.name == "nt"
    fname = os.path.join(rgma_home, "sbin", "rgma-client-check")
    if nt: fname = fname + ".bat"
 
    f = open(fname, "w")
    if nt:
        f.write("set RGMA_HOME=" + rgma_home + "\n")
        f.write("set PYTHONPATH=" + os.path.join(rgma_home, "lib", "python") + "\n")
        f.write("set LD_LIBRARY_PATH=" + os.path.join(rgma_home, "lib") + "\n")
        f.write("set GLITE_LOCATION=" + glite_location + "\n")
        f.write(os.path.join(rgma_home, "libexec", "rgma-client-check.py") + "\n")
        f.close()
    else:
        f.write("#!/bin/sh\n")
        f.write("export RGMA_HOME=" + rgma_home + "\n")
        f.write("export PYTHONPATH=" + os.path.join(rgma_home, "lib", "python") + "\n")
        f.write("export LD_LIBRARY_PATH=" + os.path.join(rgma_home, "lib") + "\n")
        f.write("export GLITE_LOCATION=" + glite_location + "\n")
        f.write(os.path.join(rgma_home, "libexec", "rgma-client-check.py") + "\n")
        f.close()
        os.chmod(fname, 0700)
        
    if not silent: print " - Written out", fname
       
if __name__ == "__main__": main()

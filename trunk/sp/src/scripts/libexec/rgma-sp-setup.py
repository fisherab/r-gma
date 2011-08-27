#!/usr/bin/env python

# rgma-sp-setup.py
#
# A script to set up rgma-sp.
#

import os, sys, optparse, socket

def terminate(msg):
    print "ERROR...", msg
    sys.exit(1)

def main():
    parser = optparse.OptionParser(usage="usage: %prog <rgma_home> <silent>", description="""
    Sets up rgma-sp. 
    """)
    
    options, args = parser.parse_args()
    if (len(args) != 2):
        terminate("There must be two arguments")

    rgma_home, silentValue = args
    silent = silentValue == "True"

    if not silent: print "\nSetting up the sp component"
    
    nt = os.name == "nt"
    fname = os.path.join(rgma_home, "bin", "rgma-sp")
    if nt: fname = fname + ".bat"
 
    f = open(fname, "w")
    if nt:
        f.write("set RGMA_HOME=" + rgma_home + "\n")
        f.write("set PYTHONPATH=" + os.path.join(rgma_home, "lib", "python") + "\n")
        f.write(os.path.join(rgma_home, "libexec", "rgmasp.py") + "\n")
        f.close()
    else:
        f.write("#!/bin/sh\n")
        f.write("export RGMA_HOME=" + rgma_home + "\n")
        f.write("export PYTHONPATH=" + os.path.join(rgma_home, "lib", "python") + "\n")
        f.write(os.path.join(rgma_home, "libexec", "rgmasp.py") + ' "$@"\n')
        f.close()
        os.chmod(fname, 0755)
        
    if not silent: print " - Written out", fname
       
if __name__ == "__main__": main()

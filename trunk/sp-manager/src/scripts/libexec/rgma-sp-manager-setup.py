#!/usr/bin/env python

# rgma-archiver-setup-setup.py
#
# A script to set up the archiver setup component.
#

import os, sys, optparse, socket

def terminate(msg):
    print "ERROR...", msg
    sys.exit(1)

def main():
    parser = optparse.OptionParser(usage="usage: %prog <rgma_home> <silent>", description="""
    Sets up the sp-manager component. 
    """)
    
    options, args = parser.parse_args()
    if (len(args) != 2):
        terminate("There must be two arguments")

    rgma_home, silentValue = args
    silent = silentValue == "True"

    if not silent: print "\nSetting up the archiver setup component"
    
    libexec = os.path.join(rgma_home, "libexec")
    if not os.path.isdir(libexec):
        os.makedirs(libexec)
    
    log = os.path.join(rgma_home, "log", "rgma")
    if not os.path.isdir(log):
        os.makedirs(log)
    
    nt = os.name == "nt"
    fname = os.path.join(libexec, "rgma-sp-manager")
    if nt: fname = fname + ".bat"
    
    etc = os.path.join(rgma_home, "etc", "rgma-sp-manager")
    if not os.path.isdir(etc): os.makedirs(etc)
    var = os.path.join(rgma_home, "var", "rgma-sp-manager")
    if not os.path.isdir(var): os.makedirs(var)
 
    f = open(fname, "w")
    if nt:
        f.write("set RGMA_HOME=" + rgma_home + "\n")
        f.write("set PYTHONPATH=" + os.path.join(rgma_home, "lib", "python") + ";" + libexec + "\n")
        f.write("set TRUSTFILE=" + os.path.join(rgma_home, "etc", "rgma-server", "ServletAuthentication.props") + "\n") 
        f.write(os.path.join(rgma_home, "libexec", "rgma-sp-manager.py") + "\n")
        f.close()
    else:
        f.write("#!/bin/sh\n")
        f.write("export RGMA_HOME=" + rgma_home + "\n")
        f.write("export PYTHONPATH=" + os.path.join(rgma_home, "lib", "python") + ":" + libexec + "\n")
        f.write("export TRUSTFILE=" + os.path.join(rgma_home, "etc", "rgma-server", "ServletAuthentication.props") + "\n") 
        f.write(os.path.join(rgma_home, "libexec", "rgma-sp-manager.py") + ' "$@"\n')
        f.close()
        os.chmod(fname, 0700)
        
    if not silent: print " - Written out", fname
    
    f = open("/etc/cron.d/rgma-sp-manager", "w")
    f.write("*/5 * * * * root " + fname + " " + rgma_home + " >> " + log + "/rgma-sp-manager.log 2>&1\n")
    f.close()
    if not silent: print " - Written rgma-sp-manager to '/etc/cron.d'"
    
    fname = os.path.join(libexec, "rgma-sp-manager-remove-lock")
    f = open("/etc/cron.d/rgma-sp-manager-remove-lock", "w")
    f.write("*/5 * * * * root " + fname + " " + rgma_home + " 10 >> " + log + "/rgma-sp-manager-remove-lock.log 2>&1\n")
    f.close()
    if not silent: print " - Written rgma-sp-manager-remove-lock to '/etc/cron.d'"
       
if __name__ == "__main__": main()

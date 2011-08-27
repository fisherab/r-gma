#!/usr/bin/env python

# rgma-server-setup
#
#This script configures an R-GMA cli
#

import sys, os, optparse

def terminate(msg):
    print >> sys.stderr, "ERROR...", msg
    sys.exit(1)

def generateRunScript(rgma_home, silent):
    nt = os.name == "nt"
    fdir = os.path.join(rgma_home, "bin")
    if not os.path.exists(fdir): os.makedirs (fdir)
    fname = os.path.join(fdir, "rgma")
    if nt: fname = fname + ".bat"
 
    f = open(fname, "w")
    if nt:
        f.write("set RGMA_HOME=" + rgma_home + "\n")
        f.write("set PYTHONPATH=" + os.path.join(rgma_home, "lib", "python") + "\n")
        f.write(os.path.join(rgma_home, "libexec", "rgma-cli.py") + ' "$@"\n')
        f.close()
    else:
        f.write("#!/bin/sh\n")
        f.write("export RGMA_HOME=" + rgma_home + "\n")
        f.write("export PYTHONPATH=" + os.path.join(rgma_home, "lib", "python") + "\n")
        f.write(os.path.join(rgma_home, "libexec", "rgma-cli.py") + ' "$@"\n')
        f.close()
        os.chmod(fname, 0755)
        
    if not silent: print " - Written out", fname

def main():
    parser = optparse.OptionParser(usage="usage: %prog [options] <rgma_home>", description="""
    Sets up an R-GMA Command Line Tool.
    """)
    parser.add_option("--silent", help="execute quietly", action="store_true")
            
    options, args = parser.parse_args()
    if (len(args) != 1):
        terminate("There must be exactly one argument")

    rgma_home, = args

    if not options.silent: print "\nSetting up R-GMA cli"
    
    # Generate run script
    generateRunScript(rgma_home, options.silent)
 
if __name__ == "__main__": main()

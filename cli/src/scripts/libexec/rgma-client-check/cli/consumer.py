#!/usr/bin/env python

# consumer.py - cli version

import os, sys
sys.path.append(os.path.join(os.environ["RGMA_HOME"], "lib", "python"))
from rgmautils import *

def main():
    rgma_home = os.environ["RGMA_HOME"]
 
    exe = os.path.join(rgma_home, "bin", "rgma")
    cmd = exe + ' -c "set query H"'
    cmd = cmd + ' -c "set timeout 10"'
    cmd = cmd + ' -c "SELECT userId from userTable WHERE userId=\'' + sys.argv[1] + "'\""
    
    stdout, stderr = runSafe2(cmd)
    numResults = -1
    if stdout:
        for line in stdout.split("\n"):
            if line.startswith("|"):
                numResults =+ 1

    if numResults != 1: terminate(`numResults` + " tuples returned rather than 1")  
    if stderr: terminate(stderr)

if __name__ == "__main__": main()

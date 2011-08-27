#!/usr/bin/env python

# producer.py - cli version

import os, sys
sys.path.append(os.path.join(os.environ["RGMA_HOME"], "lib", "python"))
from rgmautils import *

def main():
    rgma_home = os.environ["RGMA_HOME"]
 
    cmd = os.path.join(rgma_home, "bin", "rgma")
    cmd = cmd + ' -c "set pp storage memory"'
    cmd = cmd + ' -c "set pp table userTable where userId=\'' + sys.argv[1] + "'\""
    cmd = cmd + ' -c "INSERT INTO userTable (userId, aString, aReal, anInt) VALUES (\'' + sys.argv[1] + '\', \'R-GMA Command line\', 3.1415962, 42)" >/dev/null'

    stdout, stderr = runSafe2(cmd)
    if stdout: print stdout + "\n"
    if stderr: terminate(stderr)

if __name__ == "__main__": main()

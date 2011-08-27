#!/usr/bin/env python

# consumer.py - C version

import os, sys
sys.path.append(os.path.join(os.environ["RGMA_HOME"], "lib", "python"))
from rgmautils import *

def main():
    rgma_home = os.environ["RGMA_HOME"]
    os.environ["LD_LIBRARY_PATH"] = os.path.join(rgma_home, "lib")

    exe = os.path.join(rgma_home, "libexec", "rgma-client-check", "c", "QueryTuple")
    stdout, stderr = runSafe2(exe + " " + " ".join(sys.argv[1:]))
    if stdout: print stdout + "\n"
    if stderr: terminate(stderr)

if __name__ == "__main__": main()

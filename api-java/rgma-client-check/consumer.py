#!/usr/bin/env python

# consumer.py - java version

import os, sys
sys.path.append(os.path.join(os.environ["RGMA_HOME"], "lib", "python"))
from rgmautils import *

def main():
    rgma_home = os.environ["RGMA_HOME"]
    bcprov = None
    for loc in ["/usr/share/java-ext/bouncycastle-jdk1.5", "/usr/share/java"]:
        fName = os.path.join(loc, "bcprov.jar")
        if os.path.exists(fName): bcprov = fName
    if not bcprov: terminate("bcprov.jar not found")
    glite_location = os.environ["GLITE_LOCATION"]
    cp=       rgma_home+"/libexec/rgma-client-check/java"
    cp=cp+":"+rgma_home+"/share/java/glite-rgma-api-java.jar"
    cp=cp+":/usr/share/java/log4j.jar"
    cp=cp+":"+bcprov
    cp=cp+":"+glite_location+"/share/java/glite-security-trustmanager.jar"
    cp=cp+":"+glite_location+"/share/java/glite-security-util-java.jar"
 
    security_opts = ""
    if "TRUSTFILE" in os.environ: security_opts = security_opts + " -DTRUSTFILE=" + os.environ["TRUSTFILE"]
    if "X509_USER_PROXY" in os.environ: security_opts = security_opts + " -DX509_USER_PROXY=" + os.environ["X509_USER_PROXY"]
    log4j_opts = " -Dlog4j.configuration=file:///$RGMA_HOME/libexec/rgma-client-check/java/test-log4j.properties"

    exe = "java -classpath " + cp + security_opts + log4j_opts + " -DRGMA_HOME=" + rgma_home + " QueryTuple"
    stdout, stderr = runSafe2(exe + " " + " ".join(sys.argv[1:]))
    if stdout: print stdout + "\n"
    if stderr: terminate(stderr)

if __name__ == "__main__": main()

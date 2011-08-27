#!/usr/bin/env python

# rgma-browser-setup
#
#This script configures an R-GMA browser

import sys, os, optparse, string, threading, shutil

def terminate(msg):
    print >> sys.stderr, "ERROR...", msg
    sys.exit(1)

def configureUnixTomcat(rgma_home, tomcat_service):
           
    # Find CATALINE_HOME
    CATALINA_HOME = None
        
    if distro in ["Debian"]:
        CATALINA_HOME = "/usr/share/tomcat5.5" # This is very crude
        CONFIG_FILE = "/etc/default/tomcat5.5"
    else:
        fName = "/etc/tomcat5/tomcat5.conf"
        if os.path.exists(fName):
            CATALINA_HOME = runSafe(". " + fName + " && echo $CATALINA_HOME")
        else:
            terminate(fname + " does not exist - please check that tomcat is installed")
     
        fName = os.path.join("/etc/sysconfig", tomcat_service)
        if os.path.exists(fName):
            CATALINA_HOME2 = runSafe(". " + fName + " && echo $CATALINA_HOME")
            if CATALINA_HOME2: CATALINA_HOME = CATALINA_HOME2   
        
    if not CATALINA_HOME:
        terminate("CATALINA_HOME not obtainable from configuration files - please check that tomcat is installed")

    # Install the browser
    path = os.path.join(CATALINA_HOME, "webapps/Browser")
    if os.path.isdir(path): shutil.rmtree(path)
    shutil.copy(os.path.join(rgma_home, "share/webapps/Browser.war"), os.path.join(CATALINA_HOME, "webapps"))
    
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
    def __init__(self,file):
        threading.Thread.__init__(self)
        self.file = file
        
    def run(self):
        self.result = self.file.read()

    def read(self):
        return self.result.strip()

def main():
    parser = optparse.OptionParser(usage="usage: %prog [options] <rgma_home>", description="""
    Sets up an R-GMA Browser.
    """)
    parser.add_option("--tomcat_service", help="the name of the tomcat service", default ="tomcat5")
    parser.add_option("--silent", help="execute quietly", action="store_true")
            
    options, args = parser.parse_args()
    if (len(args) != 1):
        print "There must be exactly one argument"
        sys.exit(1)
    rgma_home = args[0]


    if not options.silent: print "\nSetting up R-GMA browser"
                           
    # Setup tomcat
    if os.name != "nt":
        global distro
        distro = runSafe("lsb_release -i").split(":")[1].strip()
        if not options.silent: print " - Distribution is:", distro
        configureUnixTomcat(rgma_home, options.tomcat_service)
 
if __name__ == "__main__": main()

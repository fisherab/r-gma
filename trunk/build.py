#!/bin/env python

import optparse, os, sys, threading, xml.dom.minidom

def main():

    if not os.path.exists("configure.py"): terminate ("Current directory must hold \"configure.py\"")

    parser = optparse.OptionParser(usage="usage: %prog clean|install", description="""
        Build R-GMA.
        """)
    
    parser.add_option("--silent", help="execute quietly", action="store_true")
    
    options, args = parser.parse_args()
    if (len(args) != 1):
        terminate("There must be exactly one argument")

    action = args[0]

    if not options.silent: print "Reading configuration from config.xml"
    try:
        serverDOM = xml.dom.minidom.parse("config.xml")
    except xml.parsers.expat.ExpatError, e:
        terminate(str(e))
    if not options.silent: print "Configuration looks like xml - good"
        
    here = os.getcwd()
    config = serverDOM.childNodes[0]
    print "Will install version", config.getAttribute("version"), "of R-GMA in", config.getAttribute("prefix")
    foundAction = False
    for operations in config.childNodes:
        if operations.nodeName == "operations":
            if operations.getAttribute("name") == action:
                foundAction = True
                for operation in operations.childNodes:
                    if operation.nodeName == "operation":
                        component = operation.getAttribute("component")
                        if not options.silent: print 50*"=", "\n\n", "Running", action, "for", component, ":", operation.getAttribute("action")
                        os.chdir(os.path.join(here, component))
                        stdout, stderr = runSafe2(operation.getAttribute("action"))
                        if not options.silent: print stdout
                        if stderr:
                            print stderr
                            terminate("Failed to " + action + " " + component)
                            
    os.chdir(here)
    if not foundAction: terminate("Action " + action + " not found in config file")

def terminate(msg):
    print "\nERROR: ", msg
    sys.exit(1)

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


main()

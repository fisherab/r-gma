import os, sys, threading, random, urllib, xml.dom.minidom

def terminate(msg):
    print >> sys.stderr, "ERROR...", msg
    sys.exit(1)
    
def runSafe(cmd):
    stdout, stderr = runSafe2(cmd)
    if stderr: terminate(stderr)
    return stdout

def runSafe2(cmd, timeout = None):
    stdin, stdout, stderr = os.popen3(cmd)        
    stdin.close()
    stdoutReader = NonBlockingReader(stdout)
    stderrReader = NonBlockingReader(stderr)
    stdoutReader.start()
    stderrReader.start()
    stdoutReader.join(timeout)
    stderrReader.join(timeout)
    if stdoutReader.isAlive() or stderrReader.isAlive():
        return "", cmd + " failed to complete in " + `timeout` + " seconds." 
    return stdoutReader.read(), stderrReader.read()
    
class NonBlockingReader(threading.Thread):
    def __init__(self,file):
        threading.Thread.__init__(self)
        self.file = file
        
    def run(self):
        self.result = self.file.read()

    def read(self):
        return self.result.strip()

def checkClients(rgma_home, glite_location, silent = False):
    
    rgma_conf = os.path.join(rgma_home, "etc", "rgma", "rgma.conf")
    if not os.path.exists(rgma_conf):
        terminate(rgma_conf + " does not exist")
    f = open(rgma_conf)
    lines =f.readlines()
    f.close()
    properties = {}
    for line in lines:
        line = line.strip()
        if line == "" or line.startswith("#"): continue
        equals_index = line.find("=")
        
        if equals_index >= 0:
            property = line[:equals_index].strip()
            value = line[equals_index + 1:].strip()
            properties[property] = (value)
        else:
            terminate("Configuration file " + rgma_conf + " has a bad entry: " + line)
    if "hostname" not in properties: terminate("hostname not defined in " + rgma_conf)
    if "port" not in properties: terminate("port not defined in " + rgma_conf)

    if os.geteuid() != 0: terminate("You must be root to run this command")
    HostKeyFile=os.path.join("/", "etc", "grid-security", "hostkey.pem")
    HostCertFile=os.path.join("/", "etc", "grid-security", "hostcert.pem")
    url = urllib.URLopener(key_file=HostKeyFile, cert_file=HostCertFile)
    request="https://" + properties["hostname"] + ":" + properties["port"] + "/R-GMA/SchemaServlet/createTable?canForward=True&vdbName=default"
    request=request+"&createTableStatement=" + urllib.quote("create table userTable(userId varchar(255) primary key, aString varchar(255), aReal real, anInt integer)")
    request=request+"&tableAuthz=" + urllib.quote("::RW")                                       
   
    try:
        connection = url.open(request)
        response = connection.read()
    except Exception, e:
        print >> sys.stderr, e
        terminate("Can't connect to R-GMA server. Tomcat can take a little while to start up: " + `e`)

    dom = xml.dom.minidom.parseString(response)
    root = dom.documentElement
    if root.nodeType == xml.dom.Node.ELEMENT_NODE:
        if root.nodeName == "p" or root.nodeName == "t":
            message = root.getAttribute("m")
            if message != "A table of this name (case-insensitive) has already been created by someone with a different DN.":
                print "Warning:", message

    os.environ["RGMA_HOME"] = rgma_home
    os.environ["GLITE_LOCATION"] = glite_location
    trustfile = os.path.join(rgma_home, "etc", "rgma-server", "ServletAuthentication.props")
    if os.path.exists(trustfile): os.environ["TRUSTFILE"] = trustfile
    tdir = os.path.join(rgma_home, "libexec", "rgma-client-check")
    timeout = 15
    testFound = False
    testFailed = False
    if os.name == "nt":
        ukey = os.environ["USERNAME"]
    else:
        ukey = os.environ["USER"]
    ukey = ukey + `random.randint(0,2<<30)`
    if os.path.isdir(tdir):
        for t in os.listdir(tdir):
            tfull = os.path.join(tdir, t)
            if os.path.exists(tfull):
                testFound = True
                if not silent: 
                    print " - Checking " + t + " API"
                producer = os.path.join(tfull, "producer.py")
                if not os.path.exists(producer):
                    print  >> sys.stderr, "Executable " + producer + " does not exist"
                    testFailed = True
                    continue
                stdout, stderr = runSafe2(producer + " " + ukey + ":" + t, timeout)
                if stdout: 
                    print stdout
                if stderr:
                    print  >> sys.stderr, "Failed to insert test tuple\n" , stderr
                    testFailed = True
                    continue
                consumer = os.path.join(tfull, "consumer.py")
                if not os.path.exists(consumer):
                    print  >> sys.stderr, "Executable " + consumer + " does not exist"
                    testFailed = True
                    continue
                stdout, stderr = runSafe2(consumer + " " + ukey + ":" + t, timeout)
                if stdout: 
                    print stdout
                if stderr:
                    print  >> sys.stderr, "Failed to query test tuple\n" , stderr
                    testFailed = True
                    continue
    if testFailed: terminate("One or more tests failed")
    if not testFound: 
        print "WARNING: No clients tests were found to execute"
    elif not silent:
        print "Client tests were all successful"

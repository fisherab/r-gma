#!/usr/bin/env python

# rgma-server-setup
#
#This script configures an R-GMA server
#

import sys, os, socket, optparse, shutil, string, stat, tempfile, threading, shutil, xml.dom.minidom, random

if os.name != "nt":
    import pwd
else:
    import ctypes
    class MEMORYSTATUSEX(ctypes.Structure):
        _fields_ = [
        ('dwLength', ctypes.c_ulong),
        ('dwMemoryLoad', ctypes.c_ulong),
        ('dwTotalPhys', ctypes.c_ulonglong),
        ('dwAvailPhys', ctypes.c_ulonglong),
        ('dwTotalPageFile', ctypes.c_ulonglong),
        ('dwAvailPageFile', ctypes.c_ulonglong),
        ('dwTotalVirtual', ctypes.c_ulonglong),
        ('dwAvailVirtual', ctypes.c_ulonglong),
        ('dwAvailExtendedVirtual', ctypes.c_ulonglong),
        ]

def terminate(msg):
    print >> sys.stderr, "ERROR...", msg
    sys.exit(1)


class section:
    def __init__(self, name, properties):
        self.name = name
        self.dict = {}
        properties.addSection(self)
        
    def add(self, name, value, comment):
        entry = value, comment
        self.dict[name] = entry
            
    def getValue(self, name):
        return self.dict[name][0]

    def getEntry(self, name):
        return self.dict[name]

    def set(self, name, value):
        entry = self.dict[name]
        entry = value, entry[1]
        self.dict[name] = entry
        
    def getName(self):
        return self.name
    
    def getAttributeNames(self):
        attributeNames = self.dict.keys()
        attributeNames.sort()
        return attributeNames
            
class properties:
    def __init__(self):
        self.dict = {}
        
    def addSection(self, section):
        self.dict[section.getName()] = section
        
    def getSections(self):
        serviceNames = self.dict.keys()
        serviceNames.sort()
        services = []
        for serviceName in serviceNames:
            services.append(self.dict[serviceName])
        return services
            
    def getAttributeNames(self):
        attributes = []
        serviceNames = self.dict.keys()
        serviceNames.sort()
        for serviceName in serviceNames:
            service = self.dict[serviceName]
            for attribute in service.getAttributeNames():
                attributes.append('%s.%s' % (serviceName, attribute))
        return attributes
    
    def splitFullName(self, fullName):
        bits = fullName.split(".")
        sectionName = bits[0]
        attName = ".".join(bits[1:])
        return sectionName, attName
    
    def getEntry(self, fullName):
        sectionName, attName = self.splitFullName(fullName)
        service = self.dict[sectionName]
        return service.getEntry(attName)
    
    def getValue(self, fullName):
        sectionName, attName = self.splitFullName(fullName)
        service = self.dict[sectionName]
        return service.getValue(attName)
    
    def set(self, fullName, value):
        sectionName, attName = self.splitFullName(fullName)
        service = self.dict[sectionName]
        service.set(attName, value)
        
    def loadDefaults(self, rgma_home, hostname):
    
        serverEtcDir = os.path.join(rgma_home, 'etc', 'rgma-server')
        serverVarDir = os.path.join(rgma_home, 'var', 'rgma-server')
    
        consumer = section("consumer", self)
        consumer.add('schemaCheckIntervalSecs', '60', 'Time taken in seconds before the schema is checked for table modifications')
        consumer.add('maxTaskTimeSecs', '60', 'Maximum time in seconds for any consumer task')
        consumer.add('maxTuplesMem', '1000', "Maximum number of tuples in the consumer's memory queue. It is best for this to be a multiple of consumer.tupleWriteBatchSize")
        consumer.add('maxTuplesDB', '100000', "Maximum number of tuples in the consumer's DB queue")
        consumer.add('tupleWriteBatchSize', '50', 'Number of tuples to write to the TupleQueue database in one operation. 50 is a good number for MySQL')
        consumer.add('maxTupleCountPerStreamedChunk', '50', 'Maximum number of tuples per streaming chunk')
        consumer.add('pingIntervalSecs', '30', 'How often to check streaming producers are still alive in seconds')	
        consumer.add('maxPopTuplesCount', '1000', 'Maximum number of tuples a consumer can pop each time')	
        consumer.add('idFile', os.path.join(serverVarDir, 'nextConsumerId'), 'Name of the file used to store the next available Consumer ID')
     
        database = section("database", self)
        database.add('location.url', 'jdbc:mysql://127.0.0.1:3306/_RGMA_', 'The URL location of the database')
        database.add('username', 'rgma', 'The database username')
        password = []
        for i in range(10):
            password.append(random.choice(string.ascii_letters))
        database.add('password',  "".join(password), 'The database password')
        database.add('jdbc.driver', 'org.gjt.mm.mysql.Driver', 'The jdbc connection driver')	
        database.add('type', 'mysql', 'Database type - not case sensitive')
        database.add('connection.pool.maxActive', '30', 'The maximum number of connections the pool will handle')	
        database.add('connection.pool.maxIdle', '2', 'The maximum number of idle connections the pool will have')	
        database.add('connection.pool.maxWait.secs', '300', 'The maximum time to wait for a connection before throwing an exception -1 means wait indefinitely')	
        database.add('log.width', '500', 'The width of the security log of database accesses')    
    
        memory = section("memory", self)
        memory.add('database.location.url', 'jdbc:hsqldb:mem:_RGMA_', 'The URL location of the database')
        memory.add('database.jdbc.driver', 'org.hsqldb.jdbcDriver', 'The jdbc connection driver')
      
        primaryproducer = section("primaryproducer", self)
        primaryproducer.add('idFile', os.path.join(serverVarDir, 'nextPrimaryProducerId'), 'Name of the file used to store the next available Primary Producer ID')
        primaryproducer.add('cleanupIntervalSecs', '60', 'Interval for cleaning up waiting producer for consumer')
    
        producer = section("producer", self)
        producer.add('maxTupleCountPerStreamedChunk', '50', 'Maximum number of tuples to send in each streaming chunk')	
        
        registry = section("registry", self)
        registry.add('cleanupthread.interval.secs', '60', 'Registry cleanup thread interval time to wait in seconds')
        registry.add('replication.interval.secs', '60', 'Registry replication thread interval time to wait in seconds')	
        registry.add('replication.max.task.time.secs', '120', 'Maximum time a replication task should take before it is aborted')
        registry.add('replication.lag', '5', 'The amount of seconds to be added to the last contact time during replication to compensate for the time taken to replicate')
     
        resource = section("resource", self)
        resource.add('termIntervalSecs', '900', 'Termination interval. It should not normally be about 15 minutes (900 seconds).')
        resource.add('localUpdateIntervalSecs', '30', 'Interval in seconds for checking to the state of the resources and marking as closed')
        resource.add('remoteUpdateIntervalSecs', '1200', 'Interval in seconds for checking the state of the resources and removing from the manager and update the registry')
        resource.add('registryLatencySecs', '120', 'Worse case time for passing update message to the registry from the resource manager, in seconds. This must be less than the "minTermIntervalSecs" and should not normally be less than 5 minutes (300 seconds)')
        resource.add('idRecordingIntervalCount', '10', 'Interval between writing resource id numbers to disk')
        resource.add('maxTaskAttemptCount', '2', 'Maximum number of tries for a task call')
        resource.add('intervalToGiveUpOnUnreachableSecs', '10800', 'After this time, when pinging a resource, a remote expection is treated as an unknown resource exception')
     
        schema = section("schema", self)
        schema.add('replicationIntervalSecs', '300', 'Schema replication interval')
        schema.add('replicationMaxTaskTimeSecs', '60', 'Maximum time a schema replication task should take before it is aborted')
           
        secondaryproducer = section("secondaryproducer", self)
        secondaryproducer.add('idFile', os.path.join(serverVarDir, 'nextSecondaryProducerId'), 'Name of the file used to store the next available Secondary Producer ID')
        secondaryproducer.add('countOfTuplesBetweenMemoryChecks', '10', 'Indicates the Interval(No of inserted tuples) between Memory Check')
        
        ondemandproducer = section("ondemandproducer", self)
        ondemandproducer.add('idFile', os.path.join(serverVarDir, 'nextOnDemandProducerId'), 'Name of the file used to store the next available OnDemand Producer ID')
        
        server = section("server", self)
        server.add('hostname', hostname, 'Server hostname')
        server.add('port', '8443', 'Server port')
        server.add('version.file.location', os.path.join(serverEtcDir, 'service-version.txt'), 'The location of the file which defines what version this server is')
        server.add('poolToWatch1', 'Tenured Gen', 'Name of memory pool to watch for running low on memory')
        server.add('poolToWatch2', 'PS Perm Gen', 'Name of memory pool to watch for running low on memory')
        server.add('maxHeadRoom', '500000', 'Bytes of memory to keep free on the HEAP')
        server.add('allowed.client.hostname.patterns.file', os.path.join(serverEtcDir, 'client-acl.txt'), 'The files with a list of glob patterns of clients allowed access')
        server.add('client.access.configuration.check.interval.secs', '500', 'Interval in seconds between checking the files of allowed client patterns')
        server.add('maximumExpectedResponseTimeMillis', '5000', 'Maximum expected response time for a service call in milliseconds (integer)')
        server.add('maximumRequestCount', '10', 'Maximum simultaneous requests to be handled before server is considered busy')
    
        servletconnection = section("servletconnection", self)
        servletconnection.add('X509_USER_PROXY', os.path.join(rgma_home, 'var', 'proxies', 'rgma-tomcat-proxy'), 'The location of the X509_USER_PROXY')
        servletconnection.add('X509_CERT_DIR', os.path.join('/etc', 'grid-security', 'certificates'), 'The location of the X509_CERT_DIR')
            
        streamingreceiver = section("streamingreceiver", self)
        streamingreceiver.add('cleanupIntervalSecs', '600', 'The frequency to check for and cleanup dead RunningReplies')
        streamingreceiver.add('port', '8088', 'Port number for the streaming receiver to listen on')
    
        streamingsender = section("streamingsender", self)
        streamingsender.add('cleanupIntervalSecs', '600', 'The frequency to check for and cleanup StreamingSources')
        streamingsender.add('optimalPacketSizeBytes', '4096', 'Optimal NIO packet size in bytes.')    
        streamingsender.add('periodToKeepRedundantSource', '900', 'How long to keep a source alive when it has no queries (seconds as integer).')      
        
        streaming = section("streaming", self)
        streaming.add('allocateDirect', 'True', 'Set True to use direct buffers for I/O')
        
        taskmanager = section("taskmanager", self)
        taskmanager.add('threadsInPool', '20', 'The number TaskInvocators, one TaskInvocators per thread')
        taskmanager.add('goodOnlyThreads', '5', 'The number of TaskInvocators that will only process tasks with good keys')
        taskmanager.add('hangingInvocatorsCheckPeriodSecs', '300', 'The frequency to check for hung TaskInvocators')
        taskmanager.add('hangingInvocatorsCheckDelaySecs', '20', 'The period after which a task should have finished, that the TaskInvocator is considered to have hung')
        taskmanager.add('maximumGoodQueuedTaskCount', '100', 'The maximum number of tasks that are queued and would run if there were a slot')
     
        tuplestoremanager = section("tuplestoremanager", self)
        tuplestoremanager.add('db.cleanupIntervalSecs', '900', 'How often the TupleStoreManager runs the tuple cleanup operation for DB storage')
        tuplestoremanager.add('db.maxHistoryTuples', '1000000000000', 'Maximum number of tuples to he held in a history tuple store for DB storage')
        tuplestoremanager.add('mem.cleanupIntervalSecs', '300', 'How often the TupleStoreManager runs the tuple cleanup operation for MEM storage')
        tuplestoremanager.add('mem.maxHistoryTuples', '10000', 'Maximum number of tuples to he held in a history tuple store for MEM storage')
        
        vdb = section("vdb", self)
        vdb.add('configuration.directory', os.path.join(serverVarDir, 'vdb'), '')
        vdb.add('configuration.check.interval.secs', '300', '')

def create_dir_and_set_permissions(dir, owner, permissions):
    if not os.path.exists(dir):
        os.makedirs(dir)
    if os.name != "nt":
        uid = pwd.getpwnam(owner)[2]
        gid = pwd.getpwnam(owner)[3] 
        os.chmod(dir, permissions)
        os.chown(dir, uid, gid)
    
def setup_mysql_database(props, DBAdminUser, DBAdminPassword):
    try:
        url = props.getValue('database.location.url')
        bits = url.split('/')
        database_host, database_port = bits[2].split(":")
        database_name = bits[3]
    except:
        terminate("'database.location.url' has bad syntax " + url)
 
    username = props.getValue('database.username')
    password = props.getValue('database.password')
    hostname = props.getValue('server.hostname')
    
    dir = tempfile.mkdtemp();
    fileName = os.path.join(dir, "t")
    
    sql_file = open(fileName, 'w');
    if os.name == "nt": 
        sql_file.write("SELECT host, user FROM mysql.db WHERE UCASE(Db)='" + database_name.upper() + "';\n")
    else:
         sql_file.write("SELECT host, user FROM mysql.db WHERE Db='" + database_name + "';\n")
    sql_file.close()
    
    stdout, stderr = runSafe2('mysql -u%s -p%s --host=%s --port=%s < %s' % (DBAdminUser, DBAdminPassword, database_host, database_port, fileName))
    if stderr:
        os.remove(fileName)
        terminate("Cannot create R-GMA database " + stderr) 
        
    sql_file = open(fileName, 'w');
    lines = stdout.split("\n")
    if len(lines) > 1:
        for line in lines[1:-1]:
            host, user = line.split("\t")
            sql_file.write("REVOKE ALL ON %s.* FROM '%s'@'%s';\n" % (database_name, user, host))
 
    sql_file.write("CREATE DATABASE IF NOT EXISTS %s;\n" % (database_name))
    sql_file.write("GRANT ALL ON %s.* TO '%s'@'%s' IDENTIFIED BY '%s';\n" % (database_name, username, hostname, password))
    sql_file.write("GRANT ALL ON %s.* TO '%s'@'%s' IDENTIFIED BY '%s';\n" % (database_name, username, "localhost", password))
    sql_file.write("GRANT ALL ON %s.* TO '%s'@'%s' IDENTIFIED BY '%s';\n" % (database_name, username, "127.0.0.1", password))
    sql_file.write("FLUSH PRIVILEGES;\n")
    sql_file.close()
    
    stdout, stderr = runSafe2('mysql -u%s -p%s --host=%s --port=%s < %s' % (DBAdminUser, DBAdminPassword, database_host, database_port, fileName))
    if stderr:
        os.remove(fileName)
        msg = "Cannot create R-GMA database\n"
        t = open(fileName)
        for line in t: msg = msg + line + "\n"
        t.close()
        terminate (msg + stderr)
        
    os.remove(fileName)
    os.rmdir(dir)   
    
def storeInProps(option, opt_str, value, parser, *args):
    opt = opt_str[2:]
    props = args[0]
    props.set(opt, value)
        
    
def createParser(props):
    parser = optparse.OptionParser(usage="usage: %prog [options] <rgma_home> <DBAdminUser> <DBAdminPassword>", description="""
    Sets up an R-GMA server.
    """)
    if os.name == "nt":
        defaultGliteLocation = ""
    else:
        defaultGliteLocation = "/opt/glite"
    parser.add_option("--tomcat_service", help="the name of the tomcat service", default="tomcat5")
    parser.add_option("--glite_location", help="where the glite directory tree is rooted", default=defaultGliteLocation)
    parser.add_option("--silent", help="execute quietly", action="store_true")
    parser.add_option("--http_proxy", help="url of a web proxy if required to access web pages []", default = None)
            
    for attribute in props.getAttributeNames():
        entry = props.getEntry(attribute)
        parser.add_option("--" + attribute, action="callback", type="string", callback=storeInProps, callback_args=(props,), help=entry[1] + " (+%default)", default=entry[0])
        
    return parser

def getTomcatUser(tomcat_service):
        
    # Find the tomcat user
    tomcat_user = None
    
    if distro in ["Debian"]:
        tomcat_user = "tomcat55" # This is very crude
    else:
        
        fName = "/etc/tomcat5/tomcat5.conf"
        if os.path.exists(fName):
            tomcat_user = runSafe(". " + fName + " && echo $TOMCAT_USER")
        else:
            terminate(fName + " does not exist - please check that tomcat is installed")
     
        fName = os.path.join("/etc/sysconfig", tomcat_service)
        if os.path.exists(fName):
            tomcat_user2 = runSafe(". " + fName + " && echo $TOMCAT_USER")
            if tomcat_user2: tomcat_user = tomcat_user2
        
    if not tomcat_user:
        terminate("Unable to derive the tomcat_user from the installation")
    
    return tomcat_user

def configureTomcat(rgma_home, tomcat_user, server_port, hostname, tomcat_service, glite_location, silent, http_proxy):

    nt = os.name == "nt"
    
    if not nt:
        uid = pwd.getpwnam(tomcat_user)[2]
        gid = pwd.getpwnam(tomcat_user)[3]
            
        # Find the configuration file to edit.
        CATALINA_HOME = None
        
        if distro in ["Debian"]:
            CATALINA_HOME = "/usr/share/tomcat5.5" # This is also very crude
            CONFIG_FILE = "/etc/default/tomcat5.5"
        else:
            fName = "/etc/tomcat5/tomcat5.conf"
            if os.path.exists(fName):
                CONFIG_FILE = fName
                CATALINA_HOME = runSafe(". " + CONFIG_FILE + " && echo $CATALINA_HOME")
            else:
                terminate(fname + " does not exist - please check that tomcat is installed")
         
            fName = os.path.join("/etc/sysconfig", tomcat_service)
            if os.path.exists(fName):
                CONFIG_FILE = fName
                CATALINA_HOME2 = runSafe(". " + CONFIG_FILE + " && echo $CATALINA_HOME")
                if CATALINA_HOME2: CATALINA_HOME = CATALINA_HOME2   
            
        if not CATALINA_HOME:
            terminate("CATALINA_HOME not obtainable from configuration files - please check that tomcat is installed")

    else:
        stdout, stderr = runSafe2('reg query "hklm\\software\\wow6432node\\apache software foundation\\tomcat"')
        if stderr:
             stdout, stderr = runSafe2('reg query "hklm\\software\\apache software foundation\\tomcat"')
        if stderr:
            terminate("Unable to find tomcat installation in registry")
        largestV = 0
        for key in stdout.split("\n"):
            if len(key) > 0:
                v = key.split("\\")[-1]
                if v > largestV:
                    largestV = v
                    largestKey = key
        if largestV == 0:
            terminate("Unable to find tomcat installation in registry")
        stdout = runSafe('reg query "' + largestKey + '" /v InstallPath')
        CATALINA_HOME = " ".join(stdout.split("\n")[1].split()[2:])
    if not silent: print " - CATALINA_HOME is", CATALINA_HOME
 
    # Install the Web App
    path = os.path.join(CATALINA_HOME, "webapps", "R-GMA")
    if os.path.isdir(path): shutil.rmtree(path)
    shutil.copy(os.path.join(rgma_home, "share", "webapps", "R-GMA.war"), os.path.join(CATALINA_HOME, "webapps"))
        
    # Set up security files
    for fName in ["hostkey.pem", "hostcert.pem"]:
        fullName = os.path.join("/etc/grid-security", fName)
        if not os.path.exists(fullName): terminate(fullName + " does not exist")

        shutil.copy2(fullName, os.path.join(CATALINA_HOME, "conf"))
        if not nt: os.chown(os.path.join(CATALINA_HOME, "conf", fName), uid, gid)
    
    f = open(os.path.join(rgma_home, "etc", "rgma-server", "ServletAuthentication.props"), "w")
    f.write("sslCertFile=" + os.path.join(CATALINA_HOME, "conf/hostcert.pem\n"))
    f.write("sslKey=" + os.path.join(CATALINA_HOME, "conf/hostkey.pem\n"))
    f.write("crlEnabled=true\n")
    f.write("crlFiles=/etc/grid-security/certificates/*.r0\n")
    f.write("sslCAFiles=/etc/grid-security/certificates/*.0\n")
    f.close()
    
    # Add secure connector if not present
    fName = os.path.join(CATALINA_HOME, "conf", "server.xml")
    if not silent: print " - Configuring", fName
    serverDOM = xml.dom.minidom.parse(fName)
    done = False;
    
    for inDoc in serverDOM.childNodes:
        if inDoc.nodeName == "Server":
            server = inDoc
            for inServer in server.childNodes:
                if inServer.nodeName == "Service":
                    service = inServer
                    serviceAtts = service.attributes
                    serviceName = serviceAtts.getNamedItem("name")
                    if serviceName: serviceName = serviceName.value
                    for inService in service.childNodes:
                        if inService.nodeName == "Connector":
                            connector = inService
                            connectorAtts = inService.attributes
                            secure = connectorAtts.getNamedItem("secure")
                            if secure: secure = secure.value
                            port = connectorAtts.getNamedItem("port")
                            if port: port = port.value
                            if secure != "true":
                                service.removeChild(connector)
                                if  not silent: print " - Removed insecure connector on port", port, "from", serviceName
                            elif port == server_port:
                                service.removeChild(connector)
                                if  not silent: print " - Removed connector on port", port, "from", serviceName + ".", "It will be re-added"
                            else:
                                 if  not silent: print " - Leaving connector on port", port, "on", serviceName
                    connector = serverDOM.createElement("Connector")
                    connector.setAttribute("sSLImplementation", "org.glite.security.trustmanager.tomcat.TMSSLImplementation")
                    connector.setAttribute("acceptCount", "100")
                    connector.setAttribute("port", server_port)
                    connector.setAttribute("clientAuth", "true")
                    connector.setAttribute("crlFiles", "/etc/grid-security/certificates/*.r0")
                    connector.setAttribute("debug", "0")
                    connector.setAttribute("disableUploadTimeout", "true")
                    connector.setAttribute("enableLookups", "true")
                    connector.setAttribute("maxSpareThreads", "75")
                    connector.setAttribute("maxThreads", "1000")
                    connector.setAttribute("minSpareThreads", "25")
                    connector.setAttribute("maxPostSize", "0")  
                    connector.setAttribute("scheme", "https")
                    connector.setAttribute("secure", "true")
                    connector.setAttribute("sslCAFiles", "/etc/grid-security/certificates/*.0")
                    connector.setAttribute("sslProtocol", "TLS")
                    connector.setAttribute("sslCertFile", os.path.join(CATALINA_HOME, "conf", "hostcert.pem"))
                    connector.setAttribute("sslKey", os.path.join(CATALINA_HOME,  "conf", "hostkey.pem"))
                    connector.setAttribute("log4jConfFile", os.path.join(CATALINA_HOME, "conf", "log4j-trustmanager.properties"))
                    
                    service.appendChild(connector)
                    done = True
                    if not silent: print " - Added connector on port", server_port, "on", serviceName
    
    if not done: terminate("no 'Service' section defined in " + fName)
                      
    f = open(fName, "w")
    f.write(serverDOM.toxml())
    f.close()

    # Set up the main configuration file for unix or modify the registry settings for windows
    if nt:
        x = MEMORYSTATUSEX()
        x.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
        ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(x))
        actualMem = int(x.dwTotalPhys / 2 ** 20)
        maxMem = max(2048, actualMem / 2)
        stdout, stderr = runSafe2('reg query "hklm\\software\\wow6432node\\apache software foundation\\procrun 2.0\\tomcat5\\parameters\\java"')
        if stderr:
             stdout, stderr = runSafe2('reg query "hklm\\software\\apache software foundation\\procrun 2.0\\tomcat5\\parameters\\java"')
        if stderr:
            terminate("Unable to find procrun installation for tomcat startup in registry")
        key = stdout.split("\n")[0]
 
        data = '"-Dcatalina.home=C:\Program Files (x86)\Apache Software Foundation\Tomcat 5.5\\0'
        data = data + '-Dcatalina.base=C:\Program Files (x86)\Apache Software Foundation\Tomcat 5.5\\0'
        data = data + '-Djava.endorsed.dirs=C:\Program Files (x86)\Apache Software Foundation\Tomcat 5.5\common\endorsed\\0'
        data = data + '-Djava.io.tmpdir=C:\Program Files (x86)\Apache Software Foundation\Tomcat 5.5\temp\\0'
        data = data + '-Djava.util.logging.manager=org.apache.juli.ClassLoaderLogManager\\0'
        data = data + '-Djava.util.logging.config.file=C:\\Program Files (x86)\\Apache Software Foundation\\Tomcat 5.5\\conf\\logging.properties\\0'
        data = data + '-Dsun.net.client.defaultReadTimeout=240000\\0'
        data = data + '-Dsun.net.client.defaultConnectTimeout=189000\\0'
        data = data + '-Dsun.net.inetaddr.ttl=1800\\0'
        data = data + '-DRGMA_HOME=\\rgma"'
        stdout = runSafe('reg add "' + key + '" /v Options /t REG_MULTI_SZ /d ' + data + " /f")
#        stdout = runSafe('reg add "' + key + '" /v JvmMx /t REG_DWORD /d ' + `maxMem`  + " /f")
         
    else:
        if distro in ["Debian"]:
            key = "JAVA_OPTS"
            opts = ""
        else:
            key = "CATALINA_OPTS"
            opts = "-server"
        actualMem = runSafe("free -m")
        actualMem = int(actualMem.split("\n")[1].split()[1])
        maxMem = max(2048, actualMem / 2)
        f = open(CONFIG_FILE)
        lines = f.readlines()
        f.close()
        f = open(CONFIG_FILE, "w")
        for line in lines:
            if not line.startswith(key): f.write(line)
        f.write(key + '="-Xmx' + `maxMem` + 'M ' + opts + ' -DRGMA_HOME=' + rgma_home + ' -Dsun.net.client.defaultReadTimeout=240000 -Dsun.net.client.defaultConnectTimeout=189000 -Dsun.net.inetaddr.ttl=1800"')
        f.close()
    if not silent: print " - Machine has " + `actualMem` + "MB of memory. Maximum JVM heap size set to " + `maxMem` + "MB."  
       
    # Set links for unix then copy from war file for anything missing. bcprov is not in the war file so must be pre-installed
    cwd = os.getcwd()

    if not nt:
        os.chdir(os.path.join(CATALINA_HOME, "common", "lib"))
        locns = ["/usr/share/java", "/usr/share/java-ext/bouncycastle-jdk1.5"]
        try:
            os.remove("bcprov.jar")
        except:
            pass
        for loc in locns:
            fName = os.path.join(loc, "bcprov.jar")
            if os.path.exists(fName):
                os.symlink(fName, "bcprov.jar")
                if not silent: print " - Symlink written to", fName, "from", os.path.join(CATALINA_HOME, "common", "lib")
                break
     
        os.chdir(os.path.join(CATALINA_HOME, "server", "lib"))
        try:
            os.remove("bcprov.jar")
            if not silent: print " - Removed 'bcprov.jar' from " + os.path.join(CATALINA_HOME, "server", "lib")
        except:
            pass
        
        for dName, fName in [("/usr/share/java", "log4j.jar"), (os.path.join(glite_location, "share/java"), "glite-security-trustmanager.jar"), (os.path.join(glite_location, "share/java"), "glite-security-util-java.jar")]:
            if not os.path.exists(fName):
                f = os.path.join(dName, fName)
                if os.path.exists(f): 
                    os.symlink(f, fName)
                    if not silent: print " - Symlink written to", f, "from", os.path.join(CATALINA_HOME, "server", "lib")
    
    os.chdir(os.path.join(CATALINA_HOME, "common", "lib"))
    if not os.path.exists("bcprov.jar"):
        terminate("bcprov.jar must be installed in " + os.path.join(CATALINA_HOME, "common", "lib"))
    
    os.chdir(os.path.join(CATALINA_HOME, "server", "lib"))
    runSafe('jar xf "' + os.path.join(CATALINA_HOME, "webapps", "R-GMA.war") + '" WEB-INF/lib')
    for fName in ["log4j.jar", "glite-security-trustmanager.jar", "glite-security-util-java.jar"]:
        if not os.path.exists(fName):
            shutil.move(os.path.join("WEB-INF", "lib", fName), fName)
    shutil.rmtree("WEB-INF")
    
    os.chdir(os.path.join(CATALINA_HOME, "conf"))
    if not os.path.exists("log4j-trustmanager.properties"):   
        f = open("log4j-trustmanager.properties", "w")
        f.write("log4j.logger.org.glite.security=INFO, fileout\n")
        f.write("log4j.appender.fileout=org.apache.log4j.RollingFileAppender\n")
        f.write("log4j.appender.fileout.File=" + os.path.join(CATALINA_HOME, "logs", "trustmanager.log") + "\n")
        f.write("log4j.appender.fileout.MaxFileSize=2000KB\n")
        f.write("log4j.appender.fileout.MaxBackupIndex=10\n")
        f.write("log4j.appender.fileout.layout=org.apache.log4j.PatternLayout\n")
        f.write("log4j.appender.fileout.layout.ConversionPattern=tomcat [%t]: %d{yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ} %-5p %c{2} %x - %m%n\n")
        f.close()

    os.chdir(cwd)

    # Create the client-acl.txt file and populate with a default configuration for the domain
    fName = os.path.join(rgma_home, "etc", "rgma-server", "client-acl.txt")
    if not os.path.exists(fName):
        f = open(fName, "w")
        acl = "." + ".".join(hostname.split(".")[1:])
        f.write(acl + "\n")
        f.close()
        if not silent: print " - Clients with tails matching", acl, "may connect"

    if not nt:
        # Setup the cron jobs to create the tomcat proxy and run it once
        proxyDir = os.path.join(rgma_home, "var", "proxies")
        if not os.path.exists(proxyDir): os.makedirs(proxyDir)
        ctp = os.path.join(rgma_home, "libexec", "rgma-create-tomcat-proxy")
        uid = pwd.getpwnam(tomcat_user)[2]
        gid = pwd.getpwnam(tomcat_user)[3]
        cmd = ctp + " " + os.path.join(proxyDir, "rgma-tomcat-proxy") + " " + `uid` + ":" + `gid`
        f = open("/etc/cron.d/rgma-create-tomcat-proxy", "w")
        f.write("37 5,11,17,23 * * * root " + cmd + "\n")
        f.close()
        if not silent: print " - Written rgma-create-tomcat-proxy to '/etc/cron.d'"
        stdout = runSafe(cmd)
        if not silent: print " - Proxy created"
 
    # Ensure that Tomcat keeps going
    if not nt:
        if distro in ["Debian"]:
            pass
        else:
            runSafe("/sbin/chkconfig --add " + tomcat_service)
            runSafe("/sbin/chkconfig " + tomcat_service + " on")
       
        f = open("/etc/cron.d/rgma-check-tomcat", "w")
        f.write("*/10 * * * * root " + rgma_home + "/libexec/rgma-check-tomcat " + tomcat_service + " >>  /usr/share/" + tomcat_service + "/logs/rgma-check-tomcat.log 2>&1\n")
        f.close()
        if not silent: print " - Written rgma-check-tomcat to '/etc/cron.d'"
        
    # Set up cron job to get the vdbs
    log = os.path.join(rgma_home, "log", "rgma")
    if not os.path.isdir(log): os.makedirs(log)
    if not nt:
        dir = os.path.join(rgma_home, "etc", "rgma-server", "vdb")
        if not os.path.exists(dir):
            os.makedirs(dir)
        f = open("/etc/cron.d/rgma-fetch-vdbs", "w")
        if http_proxy:
            proxy = "http_proxy=" + http_proxy
        else:
            proxy = ""
        f.write("*/10 * * * * root " + proxy + " " + rgma_home + "/libexec/rgma-fetch-vdbs.py " + rgma_home + " >> " + log + "/rgma-fetch-vdbs.log 2>&1\n")
        f.close()
        if not silent: print " - Written rgma-fetch-vdbs.py to '/etc/cron.d'"
        
    # Set up logrotate
    if not nt:
        f = open("/etc/logrotate.d/rgma", "w")
        f.write("weekly\n")
        f.write("rotate 5\n")
        f.write("missingok\n")
        f.write("compress\n")
        f.write("/var/log/" + tomcat_service + "/rgma-check-tomcat.log {}\n")
        f.write("/var/log/" + tomcat_service + "/rgma-server.log {}\n")
        f.write(os.path.join(rgma_home, "log", "rgma") + "/rgma-fetch-vdbs.log {}\n")
        f.write(os.path.join(rgma_home, "log", "rgma") + "/rgma-sp-manager.log {}\n")
        f.write(os.path.join(rgma_home, "log", "rgma") + "/rgma-sp-manager-remove-lock.log {}\n")
        f.close()
        if not silent: print " - Written rgma to '/etc/logrotate.d'"
 
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
    def __init__(self, file):
        threading.Thread.__init__(self)
        self.file = file
        
    def run(self):
        self.result = self.file.read()

    def read(self):
        return self.result.strip()

def main():
    props = properties()
    props.loadDefaults(rgma_home="<rgma_home>", hostname=socket.getfqdn());
    parser = createParser(props)
    options, args = parser.parse_args()
    if (len(args) != 3): terminate("There must be exactly three arguments")
    rgma_home, DBAdminUser, DBAdminPassword = args
    
    hostname = props.getValue("server.hostname")
    props = properties()
    props.loadDefaults(rgma_home=rgma_home, hostname=hostname)
    parser = createParser(props)
    options, args = parser.parse_args()
           
    # Error checking of inputs
    if props.getValue('database.type') != 'mysql':
        terminate(props.getValue('database.type') + " is not supported. Currently mysql is the only supported database")
        
    # Write props file
    if not options.silent: print "\nSetting up R-GMA Server"
    props_dir = os.path.join(rgma_home, "etc", "rgma-server")
    if not os.path.isdir(props_dir): os.makedirs(props_dir)
    props_filename = os.path.join(props_dir, "rgma-server.props")
    props_file = open(props_filename, "w")
    props_file.write("# \n")
    props_file.write("# R-GMA server properties file\n")
    props_file.write("# This file was generated by the rgma-server-setup script\n")
    props_file.write("# \n\n")
    
    for section in props.getSections():
        sectionName = section.getName()
        props_file.write('##\n')
        props_file.write('## %s\n' % (sectionName))
        props_file.write('##\n\n')
        for attributeName in section.getAttributeNames():
            entry = section.getEntry(attributeName)   
            props_file.write('# %s\n' % (entry[1]))
            props_file.write('%s.%s = %s\n\n' % (sectionName, attributeName, entry[0]))
        props_file.write('\n')
    
    props_file.close()
    
    if os.name == "nt": # double up all the back slashes
        props_file = open(props_filename, "r")
        lines = props_file.readlines()
        props_file.close()
        props_file = open(props_filename, "w")
        for line in lines:
            props_file.write(line.replace("\\", "\\\\"))
        props_file.close()
        
    tomcat_user = None
    if os.name != "nt":
        global distro
        distro = runSafe("lsb_release -i").split(":")[1].strip()
        if not options.silent: print " - Distribution is:", distro
        try:
            tomcat_user = getTomcatUser(options.tomcat_service)
            uid = pwd.getpwnam(tomcat_user)[2]
            gid = pwd.getpwnam(tomcat_user)[3]
        except KeyError:
            terminate("TOMCAT_USER " + tomcat_user + " does not exist")

        os.chown(props_filename, uid, gid)
        permissions = stat.S_IREAD | stat.S_IWRITE
        os.chmod(props_filename, permissions)
    
    if not options.silent: print ' - Wrote configuration to: %s' % props_filename
    
    # Setup tomcat
    configureTomcat(rgma_home, tomcat_user, props.getValue("server.port"), hostname, options.tomcat_service, options.glite_location, options.silent, options.http_proxy)

    # Set up directories and  permissions
    permissions = stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC
    for thing in ["consumer", "primaryproducer", "secondaryproducer", "ondemandproducer"]:
        dir, file = os.path.split(props.getValue(thing + ".idFile"))
        create_dir_and_set_permissions(dir, tomcat_user, permissions)
     
    permissions = stat.S_IREAD | stat.S_IEXEC
    vdbDir = props.getValue('vdb.configuration.directory')
    create_dir_and_set_permissions(vdbDir, tomcat_user, permissions)
    
    cmd = "openssl x509 -in /usr/share/tomcat5/conf/hostcert.pem -noout -subject  | sed 's/subject= //'"
    dn, stderr = runSafe2(cmd)
    if stderr: terminate("Can't execute " + cmd + "\n" + stderr) 
        
    f = open(os.path.join(vdbDir, "default.xml"), "w")
    f.write('<?xml version="1.0" ?>\n')
    f.write('<vdb name="Default">\n')
    f.write('        <host masterSchema="true" name="' + hostname + '" port="' + props.getValue("server.port") + '" registry="true"/>\n')
    f.write('        <rule action="CRW" credentials="[DN]=\'' + dn + '\'"/>\n')
    f.write('        <rule action="CR"/>\n')
    f.write('</vdb>\n')
    f.close()
    if not options.silent: print ' - Wrote local only vdb definition:', f.name
        
    # Set up database
    dbType = props.getValue('database.type')
    if dbType == 'mysql':
        setup_mysql_database(props, DBAdminUser, DBAdminPassword)
    else:
        terminate("Unable to configure " + dbType + " database")

    if not options.silent: print ' - Database configured'
        
    # Write wrappers
    writeWrapper(rgma_home, "rgma-server-check", [os.path.join(rgma_home, "lib", "python")], options.silent)
    writeWrapper(rgma_home, "rgma-show-db", silent = options.silent)
      
def writeWrapper(rgma_home, cmd, pythonpath = [], silent = False):
    fname = os.path.join(rgma_home, "sbin", cmd)
    pyscript = os.path.join(rgma_home, "libexec", cmd+ '.py "$@"')
    nt = os.name == "nt"
    if nt:
        f = open(fname + ".bat", "w")
        f.write("set RGMA_HOME=" + rgma_home + "\n")
        if pythonpath: f.write("set PYTHONPATH=" + ";".join(pythonpath) + "\n")
        f.write(pyscript + "\n")
        f.close()
    else:
        f = open(fname, "w")
        f.write("#!/bin/sh\n")
        f.write("export RGMA_HOME=" + rgma_home + "\n")
        if pythonpath: f.write("export PYTHONPATH=" + ":".join(pythonpath) + "\n")
        f.write(pyscript + "\n")
        f.close()
        os.chmod(fname, 0700)
    if not silent: print ' - Wrapper', cmd, "written to", os.path.join(rgma_home, "sbin")

  
if __name__ == "__main__": main()

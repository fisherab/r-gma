#!/bin/env python

import optparse, os, sys, threading, glob

class component:
    def __init__(self, name, configurables):
        self.name = name
        self.clean = None
        self.install = None
        self.extDeps = []
        self.deps = []
        self.configurables = configurables
        
    def addExternalDependency(self, thing):
        self.extDeps.append(thing)
        self.configurables[thing] = 1
        
    def addDependency(self, thing):
        self.deps.append(thing)
        
    def getDependencies(self):
        return self.deps
    
    def getExternalDependencies(self):
        return self.extDeps
    
def createComponents(versions):
    
    components = {}
    configurables = {}
    
    # Define the browser
    browser = component("browser", configurables)
    components["browser"] = browser
    browser.clean = "ant clean"
    browser.install = """ant install
      -Dprefix=${prefix}
    """
    browser.addDependency("server")

    # Define the Java API
    apiJava = component("api-java", configurables)
    components["api-java"] = apiJava
    apiJava.clean = "ant clean"
    apiJava.install = """ant install 
        -Dbouncycastle.location=${bouncycastle.location} 
        -Dorg.glite.security.util-java.location=${org.glite.security.util-java.location} 
        -Dorg.glite.security.trustmanager.location=${org.glite.security.trustmanager.location} 
        -Dprefix=${prefix}
    """ + " -Dversion=" + versions["api-java"] 
    apiJava.addExternalDependency("bouncycastle")
    apiJava.addExternalDependency("org.glite.security.trustmanager")
    apiJava.addExternalDependency("org.glite.security.util-java")
    apiJava.addDependency("base")
    
    # Define the Python API
    apiPython = component("api-python", configurables)
    components["api-python"] = apiPython
    apiPython.clean = "ant clean"
    apiPython.install = """ant install 
        -Dprefix=${prefix}
    """ + " -Dversion=" + versions["api-python"]
    apiPython.addDependency("base")
    
    # Define the C API
    apiC = component("api-c", configurables)
    components["api-c"] = apiC
    apiC.clean = "make clean"
    apiC.install = """make 
        prefix=${prefix}
    """ + " version=" + versions["api-c"] + " install"
    apiC.addDependency("base")
    
    # Define the C++ API
    apiCPP = component("api-cpp", configurables)
    components["api-cpp"] = apiCPP
    apiCPP.clean = "make clean"
    apiCPP.install = """make 
        prefix=${prefix}
    """ + " version=" + versions["api-cpp"] + " install"
    apiCPP.addDependency("base")
    
    # Define the Command Line Tool
    cli = component("cli", configurables)
    components["cli"] = cli
    cli.clean = "ant clean"
    cli.install = """ant install 
        -Dprefix=${prefix}
    """
    cli.addDependency("api-python")

    # Define base    
    base = component("base", configurables)
    components["base"] = base
    base.clean = "ant clean"
    base.install = "ant -Dprefix=${prefix} install"
    
    #Define sp
    sp = component("sp", configurables)
    components["sp"] = sp
    sp.clean = "ant clean"
    sp.install = "ant -Dprefix=${prefix} install"
    sp.addDependency("base")
    sp.addDependency("api-python")
        
    #Define so-manager
    spManager = component("sp-manager", configurables)
    components["sp-manager"] = spManager
    spManager.clean = "ant clean"
    spManager.install = "ant -Dversion=" + versions["sp-manager"] + " -Dprefix=${prefix} install"
    spManager.addDependency("sp")
    
    # Define the server
    server = component("server", configurables)
    components["server"] = server
    server.clean = "ant clean"
    server.install = """ant install 
        -Dlog4j.location=${log4j.location} 
        -Dprefix=${prefix} 
        -Dorg.glite.security.trustmanager.location=${org.glite.security.trustmanager.location} 
        -Dorg.glite.security.util-java.location=${org.glite.security.util-java.location} 
        -Dorg.glite.security.voms-api-java.location=${org.glite.security.voms-api-java.location} 
        -Dhsqldb.location=${hsqldb.location} 
        -Dcommons-pool.location=${commons-pool.location} 
        -Dcommons-dbcp.location=${commons-dbcp.location} 
        -Dtomcat.location=${tomcat.location} 
        -Dbouncycastle.location=${bouncycastle.location} 
        -Dxerces2-j.location=${xerces2-j.location} 
        -Dmysql-jdbc.location=${mysql-jdbc.location} 
        -Dorg.glite.security.voms-api-java.location=${org.glite.security.voms-api-java.location} 
    """ + " -Dversion=" + versions["server"]
        
    server.addExternalDependency("bouncycastle")
    server.addExternalDependency("commons-dbcp")
    server.addExternalDependency("commons-pool")
    server.addExternalDependency("hsqldb")           
    server.addExternalDependency("tomcat")    
    server.addExternalDependency("log4j")    
    server.addExternalDependency("mysql-jdbc")  
    server.addExternalDependency("org.glite.security.trustmanager")
    server.addExternalDependency("org.glite.security.util-java") 
    server.addExternalDependency("org.glite.security.voms-api-java") 
    server.addExternalDependency("xerces2-j")
    server.addDependency("base")
        
    return components, configurables.keys()

def terminate(msg):
    print "\nERROR: ", msg
    sys.exit(1)

def runSafe(cmd):
    stdout, stderr = runSafe2(cmd)
    if stderr:
        terminate(stderr)
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
    
def checkCmd(cmd, silent):
    if os.name != "nt":
        runSafe("which " + cmd.split(" ")[0])
    stdout, stderr = runSafe2(cmd)
    if stderr.find("is not recognized as an internal or external command") >= 0:
        terminate(stderr)
    if not silent: print "   ", " ".join((stdout + " " + stderr).split())
    
def main():

    if not os.path.exists("configure.py"): terminate ("Current directory must hold \"configure.py\"")

    parser = optparse.OptionParser(usage="usage: %prog [options]", description="""
        Configure R-GMA for building.
        """)
    
    versionsFile = open("versions.txt")
    lines = versionsFile.read().split("\n")
    versionsFile.close()
    versions = {}
    for line in lines:
        if line.strip():
            key, value = line.split()
            versions[key] = value
    
    components, locations = createComponents(versions)
    cList = " ".join(components.keys())
    defaultExternalLocations = []
    if os.name == "nt":
        defaultPrefix = "\\rgma"
    else: 
        defaultPrefix = "/usr/local"
        defaultExternalLocations.append("/usr")
        defaultExternalLocations.append("/usr/local")
        defaultExternalLocations.append("/opt/glite")
        defaultExternalLocations.append("/opt")
    dList = " ".join(defaultExternalLocations)
    
    parser.add_option("--components", "-c", help="the components to build (default = \"" + cList + "\")", default = cList)
    parser.add_option("--locations", "-l", help = "where to look for external components (default  = \"" + dList + "\")", default = dList)
    parser.add_option("--listLocationKeys", "-p", help = "list keys for external components", action="store_true")
    parser.add_option("--keyedLocations", "-k", help="space separated list of name value pairs to relate a location to a specific component (default = \"\")", default = "")
    parser.add_option("--prefix", help="where to install the package (default = \"" + defaultPrefix + "\")", default = defaultPrefix)
    parser.add_option("--silent", help="execute quietly", action="store_true")
    
    options, args = parser.parse_args()
    if (len(args) != 0):
        terminate("There must be no arguments")

    if options.listLocationKeys:
        print "Values that can be assigned are:"
        for location in locations:
            print "   ", location
        sys.exit(0)

    keyedLocations = {}
    klist = options.keyedLocations.split()
    if len(klist)%2 != 0: terminate("keyed locatations must be in pairs")
    for i in range(0, len(klist) ,2):
        key = klist[i]
        if key not in locations:
            terminate (key + " specified in 'keyedLocations' is not recognised")
        keyedLocations[key] = os.path.abspath(klist[i+1])
    
    if not options.silent: print "\nUsing following components:"
    checkCmd("python -V", options.silent)
    checkCmd("ant -version", options.silent)
    checkCmd("java -version", options.silent)
    checkCmd("javac -version", options.silent)
        
    componentsToBuild = {}
    for c in options.components.split(" "):
        componentsToBuild[c] = 1
    while(1):
        extras = {}
        for comp in componentsToBuild:
            deps = components[comp].getDependencies()
            for dep in deps:
                if dep not in componentsToBuild:
                    if not options.silent: print "\nAdding", dep, "to satisfy dependencies"
                    extras[dep] = 1
        for extra in extras:
            componentsToBuild[extra] = 1
        if extras == {}: break

    buildList = []
    while (1):
        added = False
        for comp in componentsToBuild.keys():
            deps = components[comp].getDependencies()
            canBuild = True
            for dep in deps: 
                if dep not in buildList: canBuild= False
            if canBuild:
                del componentsToBuild[comp]
                buildList.append(comp)
                added = True
        if not added: break
  
    externals = {}
    for comp in buildList:
       deps = components[comp].getExternalDependencies()
       for dep in deps:
           externals[dep] = 1
    
    externalPatterns= {}
    externalPatterns["org.glite.security.trustmanager"] = os.path.join("share", "java", "glite-security-trustmanager.jar")
    externalPatterns["org.glite.security.util-java"] = os.path.join("share", "java", "glite-security-util-java.jar")
    externalPatterns["org.glite.security.voms-api-java"] = os.path.join("share", "java", "vomsjapi.jar")
    externalPatterns["bouncycastle"] = os.path.join("share", "java", "bcprov.jar")
    externalPatterns["commons-dbcp"] = os.path.join("commons-dbcp-*.jar")
    externalPatterns["commons-pool"] = os.path.join("commons-pool-*.jar")
    externalPatterns["log4j"] = os.path.join("share", "java", "log4j.jar")
    externalPatterns["tomcat"] = os.path.join("common", "lib", "servlet-api.jar")
    externalPatterns["xerces2-j"] = os.path.join("xercesImpl.jar")
    externalPatterns["hsqldb"] = os.path.join("lib", "hsqldb.jar")
    externalPatterns["mysql-jdbc"] = os.path.join("mysql-connector-java*.jar")
 
    externalDict = {}
    externalDict["prefix"] = os.path.join(os.getcwd(), options.prefix)

    for external in externals:
        fExternalName = external +".location"
        if external in keyedLocations:
            par = keyedLocations[external]
        else:
            par = findExternal(external, externalPatterns, options.locations.split())
        externalDict[fExternalName] = par
        
    used = {}
    f = open("config.xml", "w")
    f.write("<config prefix=\"" + options.prefix + "\" version=\"" + versions["rgma"] + "\">\n")
    f.write("   <operations name=\"install\">\n")
            
    for compName in buildList:
        install = components[compName].install
        if install:
            f.write("      <operation component=\"" + compName + "\" action=\"" + expanded(install, externalDict, used) + "\"/>\n")

    f.write("   </operations>\n")
    f.write("   <operations name=\"clean\">\n")

    buildList.reverse()
    for compName in buildList:
        clean = components[compName].clean
        if clean:
            f.write("      <operation component=\"" + compName + "\" action=\"" + expanded(clean, externalDict, used) + "\"/>\n")
    f.write("   </operations>\n")
    f.write("</config>\n")

    f.close()

    if not options.silent:
        print "\nVariables used:"
        for key in used.keys():
            print "  ", key,"=",  externalDict[key]

def findExternal(ext, patterns, locations):
    pattern = patterns[ext]
    for location in locations:
        for fname in glob.glob(os.path.join(location, pattern)):
            if os.path.exists(fname):
                return os.path.abspath(location)
    terminate("Unable to find " + pattern + " in " + `locations` + " for " + ext)

def expanded(cmd, map, used):
    cmd = " ".join(cmd.split())
    for key, value in map.items():
        if cmd.find("${") < 0: break
        fkey = "${" + key + "}"
        if cmd.find(fkey) >= 0:
            cmd = cmd.replace(fkey, value)
            used[key] = 1
    if cmd.find("${") == 0:
        terminate("Unable to find value for: " +  cmd[cmd.find("${") + 2:cmd.find("}")])
            
    return cmd
    
main()


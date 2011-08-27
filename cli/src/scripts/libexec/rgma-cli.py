#!/usr/bin/env python

# rgma-cli.py
#
# Command line tool for R-GMA
#
# Mimics a SQL database command line, allowing SELECT and INSERT statements
# in standard SQL. Also provides commands to set Consumer and Producer
# properties, create Secondary Producers and query the Schema and Registry.

import rgma, sys, cmd, string, time, os, re, textwrap, glob

SBOLD = "\033[1m" 
EBOLD = "\033[0m"
SULINE = "\033[4m" 
EULINE = "\033[0m"

def main():
    command_next = False
    commands = []
    file_next = False
    batch_file = None
    vdb = None
   
    for arg in sys.argv[1:]:
                          
        if command_next:
            commands.append(arg)
            command_next = False
        elif file_next:
            batch_file = arg
            file_next = False
        elif arg in ["--help", "-h"]:
            showHelp()
            sys.exit(0)
        elif arg in ["--version", "-v"]:
            showVersion()
            sys.exit(0)
        elif arg == "-c":
            command_next = True
        elif arg == "-f":
            file_next = True
        elif vdb == None:
            vdb = arg
        else:
            sys.stderr.write("\nERROR - unrecognized command line option %s\n" % arg)
            sys.stderr.write("Try rgma --help for usage information\n")
            sys.exit(1)
                               
    if command_next:
        sys.stderr.write("\nERROR: -c option used but no command specified\n")
        sys.stderr.write("Try rgma --help for usage information\n")
        sys.exit(1)

    if file_next:
        sys.stderr.write("\nERROR: -f option used but no file specified\n")
        sys.stderr.write("Try rgma --help for usage information\n")
        sys.exit(1)
       
    if len(commands) > 0 and batch_file:
        sys.stderr.write("\nERROR: cannot specify -c and -f together\n")
        sys.stderr.write("Try rgma --help for usage information\n")
        sys.exit(1)
       
    cli = rgma_cli(vdb)
    if len(commands) > 0:
        for command in commands: cli.onecmd(cli.precmd(command))
        cli.cleanup()
        sys.exit(cli.exitcode)
    elif batch_file:
        cli.do_read(batch_file)
        cli.cleanup()
        sys.exit(cli.exitcode)
    else:
        try:
            cli.cmdloop()
        except KeyboardInterrupt:
            cli.exit(None)

NUL = "NULL"

def printTable(data, header=None):
    width = []
    if not data:
        return
    if header:
        for c in header:
            width.append(len(c))
    else:
        for c in data[0]:
            width.append(0)
    for datum in data:
        for i in range(len(width)):
            if datum[i]:
                width[i] = max(width[i], len(datum[i]))
            else:
                width[i] = max(width[i], len(NUL))
    
    for w in width:
        print "+", w * "-",
    print "+"
    
    if header:
        for i in range(len(width)):
            print "|", header[i] + (width[i] - len(header[i])) * " ",
        print "|"
        for w in width:
            print "+", w * "-",
        print "+"
    
    for datum in data:
        for i in range(len(width)):
            if datum[i]:
                print "|", datum[i] + (width[i] - len(datum[i])) * " ",
            else:
                print "|", NUL + (width[i] - len(NUL)) * " ",
        print "|"
        
    for w in width:
        print "+", w * "-",
    print "+"
    
def getIntervalAsString(interval):
    num = interval.getValueAs()
    units = "seconds"
    if (num % 60) == 0:
        num = num / 60
        units = "minutes"
    if (num % 60) == 0:
        num = num / 60
        units = "hours"
    if (num % 24) == 0:
        num = num / 24
        units = "days"
    return str(num) + " " + units

def getStorageAsString(storage):
    if storage.logicalName:
        return "Permanent " + storage.storageType + " storage with logical name " + storage.logicalName
    else:
        return "Temporary " + storage.storageType + " storage"

         
class CommandLineException(Exception):
    """
    Recoverable exception thrown by the command line tool
    """

    def __init__(self, message):
        """
        Constructor.
        
        message - Message describing the error
        """
        Exception.__init__(self, message)

class rgma_cli(cmd.Cmd):

    def __init__(self, vdb):
        cmd.Cmd.__init__(self)
        
        if vdb == None: 
            self.vdbName = "DEFAULT"
        else:
            self.vdbName = vdb.upper()
        try:
            self.schema = rgma.Schema(self.vdbName)
            self.registry = rgma.Registry(self.vdbName)
            self.getTables()
        except Exception, e:
            sys.stderr.write(str(e) + "\n")
            sys.stderr.write("Try rgma --help for usage information\n")
            sys.exit(1)
        
        self.queryTypes = ["L", "H", "C", "S"]
        self.supportedQueryTypes = ["C", "CH", "CL", "CHL"]
        
        self.qt = rgma.QueryType.L
        self.qi = None
        self.timeout = rgma.TimeInterval(1, rgma.TimeUnit.MINUTES)
        self.resultsFile = sys.stdout
        
        self.pp = None
        self.ppLrp = rgma.TimeInterval(10, rgma.TimeUnit.MINUTES)
        self.ppHrp = rgma.TimeInterval(10, rgma.TimeUnit.MINUTES)
        self.ppTables = {}
        self.ppStorage = rgma.Storage.getMemoryStorage()
        self.ppDeclared = []

        self.sp = None
        self.spHrp = rgma.TimeInterval(10, rgma.TimeUnit.MINUTES)
        self.spTables = {}
        self.spStorage = rgma.Storage.getDatabaseStorage()

        self.directedEndpoints = None
        
        self.sessionHistory = []
        
        self.authzRules = []
        
        self.intro = "\nWelcome to the R-GMA virtual database for Virtual Organisations.\n" + \
                     "================================================================\n"

        hostname, port = rgma._state.host.split(":")
        self.intro = self.intro + "\nUsing server version %s at %s on port %s\n" % (rgma.RGMAService.getVersion(), hostname, port)
        
        self.intro = self.intro + "\nType \"help\" for a list of commands or \"help overview\" to get started.\n"

        self.prompt = "rgma> "
        self.exitcode = 0
        
        self.exampleDir = os.path.join(os.environ["RGMA_HOME"], "share", "doc", "rgma-command-line", "examples")
        self.examples = map(lambda x:x[:-5], filter(lambda x: x.endswith(".rgma"), os.listdir(self.exampleDir)))
        
    #
    # Implementations of commands
    #

    def cleanup(self):
        if self.pp:
           self.pp.close()

        if self.sp:
           self.sp.close()       

    def exit(self, args):
        try:
            self.cleanup()
        except (Exception), e:
            pass
        
        print "\nGoodbye\n" 
        sys.exit(0)
               
    SYSCOLS = ["RgmaTimestamp", "RgmaLRT", "RgmaOriginalServer", "RgmaOriginalClient"]
    def select(self, args):
        msg = "Usage: SELECT <columns> FROM <table> [WHERE  <predicate>] - try 'help select' for more information"
        try:
            uc = args.upper()
            nfrom = uc.find("FROM ")
            attributes = args[:nfrom].strip().split(",")
            nwhere = uc.find(" WHERE ", nfrom + 5)
            norder = uc.find(" ORDER ", nfrom + 5)
            ngroup = uc.find(" GROUP ", nfrom + 5)
            if nwhere == -1: nwhere = len(uc)
            if norder == -1: norder = len(uc)
            if ngroup == -1: ngroup = len(uc)
            n = min(nwhere, norder, ngroup)
            tables = args[nfrom + 5:n]
            afterTables = args[n + 1:]
            ntables = []
            nattributes = []
            for table in tables.split(","):
                vdbTableName, schema, registry, vdbName, tableName = self.parseTableName(table.strip())
                ntables.append(vdbTableName)
                if attributes == ["*"]:
                    nattributes.extend(self.getCols(schema, vdbName, tableName))
                elif attributes == ["**"]:
                    nattributes.extend(self.getCols(schema, vdbName, tableName) + self.SYSCOLS)
            if nattributes: attributes = nattributes 
            query = "SELECT " + ",".join(attributes) + " FROM " + ",".join(ntables) + " " + afterTables
            expanded = True
        except Exception:
            if args.find('.') == -1:
                raise CommandLineException("Query is invalid - either the syntax is wrong or a table is unknown. Try 'help select' for suggestions")
            query = "SELECT " + args
            attributes = None
            expanded = False
  
        consumer = None
        try:        
            consumer = rgma.Consumer(query, self.qt, self.qi, self.timeout, self.directedEndpoints)
            eof = False
            while not eof:
                ts = consumer.pop(50)
                data = ts.getData()
                warning = ts.getWarning()
                eof = ts.isEndOfResults()
                if len(data) > 0:
                    tuples = []
                    for datum in data:
                        tuples.append(datum.getPyTuple())
                    printTable(tuples, attributes)
                if warning: print SBOLD + "Warning: " + EBOLD + warning
                            
                if len(data) == 0: time.sleep(3)
                elif len(data) != 50: time.sleep(1)
            consumer.close()
        except:
            if consumer: consumer.close()
            if expanded:
                print "Query sent to server was:", query
                print "See 'help select' for possible solutions to the error shown below:"
            raise

    def set(self, args):
        args = args.split()
        if len(args) == 0:
            raise CommandLineException("Usage SET <property> <value> - try 'help set' for more information")
        else:
            property = args[0].lower()
            if property == "query":
                self.setQuery(args[1:])
            elif property == "timeout":
                self.setTimeout(args[1:])
            elif property == "pp":
                self.setPp(args[1:])
            elif property == "sp":
                self.setSp(args[1:])
            else:
                raise CommandLineException("Unrecognized property '%s' - try 'help set' for more information " % property)
            
    def alter(self, args):
        args = args.split()
        if len(args) < 4 or len(args) > 5:
            raise CommandLineException("Usage ALTER ... - try 'help alter' for more information")
        vdbTableName, schema, registry, vdbName, tname = self.parseTableName(args[1])
        if len(args) == 4:
            schema.alter(args[0], args[1], args[2], args[3])
        else:
            schema.alter(args[0], args[1], args[2], args[3], args[4])

    def setQuery(self, args):
        if len(args) < 1 or len(args) > 3:
            raise CommandLineException("Usage SET QUERY <query type> [<time> [<units>]] - try 'help set' for more information")
        queryType = args[0].upper().strip()
        if len(args) > 1:
            qi = self.getTimeInterval(args[1:])
            if hasattr(rgma.QueryTypeWithInterval, queryType):
                self.qi = qi
                self.qt = getattr(rgma.QueryTypeWithInterval, queryType)
            else:
                raise CommandLineException("Unrecognized query type with interval: '%s' - try 'help set' for usage information" % queryType)
        else:
            if hasattr(rgma.QueryType, queryType):
                self.qi = None
                self.qt = getattr(rgma.QueryType, queryType)
            else:
                raise CommandLineException("Unrecognized query type: '%s' - try 'help set' for usage information" % queryType)

    def setTimeout(self, args):
        if len(args) == 0 or len(args) > 2:
            raise CommandLineException("Usage SET TIMEOUT ... - try 'help set' for more information")
        if len(args) == 1 and args[0].lower() == "none":
            self.timeout = None
        timeout = self.getTimeInterval(args)
        self.timeout = timeout
            
    def setPp(self, args):
        if len(args) == 0:
            raise CommandLineException("Usage SET PP|SP ... - try 'help set' for more information")
 
        property = args[0].lower()    
        if property == "table":
            self.setProducerTable(self.ppTables, args[1:])
        elif property == "lrp":
            self.setProducerLrp(args[1:])
        elif property == "hrp":
            self.setProducerHrp("ppHrp", args[1:])
        elif property == "storage":
            self.setProducerStorage("ppStorage", args[1:])
        else:
            raise CommandLineException("Usage SET PP|SP ... - try 'help set' for more information")
        if self.pp:
            self.pp.close()
            self.pp = None
            
    def setSp(self, args):
        if len(args) == 0:
            raise CommandLineException("Usage SET SECONDARYPRODUCER <query types> - try 'help set' for more information")
 
        property = args[0].lower()
        if property == "table":
            self.setProducerTable(self.spTables, args[1:])
        elif property == "hrp":
            self.setProducerHrp("spHrp", args[1:])
        elif property == "storage":
            self.setProducerStorage("spStorage", args[1:])
        else:
            raise CommandLineException("Usage SET PP|SP ... - try 'help set' for more information")
        if self.sp:
            self.sp.close()
        self.createNewSecondaryProducer()
       
    def setProducerStorage(self, attrName, args):
        if len(args) != 1:
            raise CommandLineException("Usage SET PP|SP STORAGE DATABASE|MEMORY|<logical name> - try 'help set' for more information")
        property = args[0].lower()
        if property == "memory":
            setattr(self, attrName, rgma.Storage.getMemoryStorage())
        elif property == "database":
            setattr(self, attrName, rgma.Storage.getDatabaseStorage())
        else:
            setattr(self, attrName, rgma.Storage.getDatabaseStorage(args[0]))
               
    def setProducerTable(self, predicates, args):
        if len(args) < 1:
            raise CommandLineException("Usage SET PP|SP PREDICATE <table> WHERE <predicate> - try 'help set' for more information")
        
        vdbTableName = self.parseTableName(args[0])[0]
        if len(args) > 0:
            predicate = string.join(args[1:]).strip()
        else:
            predicate = "" 

        # Check for 'none' predicate, and add WHERE if necessary
        if predicate.lower() == "none":
            if vdbTableName in predicates:
                del predicates[vdbTableName]
        else:
            if predicate != "":
                if args[1].lower() != "where":
                    predicate = "WHERE " + predicate           
            predicates[vdbTableName] = predicate
            
    def setProducerHrp(self, attrName, args):
        if len(args) == 0 or len(args) > 2:
            raise CommandLineException("Usage SET PP|SP HRP <value> [<units>] - try 'help set' for more information")
        setattr(self, attrName, self.getTimeInterval(args))
      
    def setProducerLrp(self, args):
        if len(args) == 0 or len(args) > 2:
            raise CommandLineException("Usage SET PP LRP <value> [<units>] - try 'help set' for more information")
        lrp = self.getTimeInterval(args)
        self.ppLrp = lrp
                
    def getCols(self, schema, vdbName, tableName):
        cols = []
        if vdbName == self.vdbName:
            cols = self.rgmaTables[tableName]
        if not cols:
            columnDefs = schema.getTableDefinition(tableName).getColumns()
            for column in columnDefs[:-4]:
                cols.append(column.getName())
            if vdbName == self.vdbName:
                self.rgmaTables[tableName] = cols
        return cols
                   
    def insert(self, args):
        if self.pp == None: self.createNewPrimaryProducer()
        if not args.lower().startswith("into "):
            raise CommandLineException("Invalid INSERT statement")
        args = args[5:]
        t1 = args.split(" ", 1)[0]
        t2 = args.split("(", 1)[0]
        if len(t1) < len(t2): 
            t = t1
        else:
            t = t2
        args = args[len(t):].strip()
        vdbTableName, schema, registry, vdbName, tableName = self.parseTableName(t)
  
        if args[0] != "(":
            args = "(" + ",".join(self.getCols(schema, vdbName, tableName)) + ") " + args  
         
        if vdbTableName not in self.ppDeclared:
            if vdbTableName in self.ppTables:
                self.pp.declareTable(vdbTableName, self.ppTables[vdbTableName], self.ppLrp, self.ppHrp)
            else:
                self.pp.declareTable(vdbTableName, "", self.ppLrp, self.ppHrp)
            self.ppDeclared.append(vdbTableName)
        insert = "INSERT INTO " + vdbTableName + " " + args
        self.pp.insert(insert)
   
    def show(self, args):
        args = args.split()
        
        if len(args) < 1:
            raise CommandLineException("Usage: SHOW <property> [<args>] - try 'help show' for more information")
        
        command = args[0].lower()    
        if command == "tables":
            self.showTables(args[1:])
        elif command == "columns":
            self.showColumns(args[1:])
        elif command == "producers":
            self.showProducers(args[1:])
        elif command == "history":
            self.showHistory(args[1:])
        elif command == "pp":
            self.showPrimaryProducer(args[1:])
        elif command == "sp":
            self.showSecondaryProducer(args[1:])
        elif command == "query":
            self.showQuery(args[1:])
        elif command == "timeout":
            self.showTimeout(args[1:])
        else:
            raise CommandLineException("Unrecognized property '%s' - try 'help show' for more information" % command)

    def showTables(self, args):
        if len(args) != 0:
            raise CommandLineException("Usage: SHOW TABLES - try 'help show' for more information")
        else:
            self.getTables()
            if len(self.rgmaTables) > 0:
                print "Tables in " + self.vdbName + " VDB"
                for table in sorted(self.rgmaTables.keys()):
                    print " ", table
            else:
                print "No tables defined in " + self.vdbName + " VDB"
                
    def showColumns(self, args):
        if len(args) != 2 or args[0].lower() != "from":
            raise CommandLineException("Usage: SHOW COLUMNS FROM <table> - try 'help show' for more information")
        vdbTableName, schema, registry, vdbName, tableName = self.parseTableName(args[1])
        print ",".join(self.getCols(schema, vdbName, tableName))
  
    def showProducers(self, args):
        if len(args) != 2 or args[0].lower() != "of":
            raise CommandLineException("Usage: SHOW PRODUCERS OF <table> - try 'help show' for more information")
        vdbTableName, schema, registry, vdbName, tableName = self.parseTableName(args[1])
        producers = registry.getAllProducersForTable(tableName)
        tuples = []
        for producer in producers:
            sq = ""
            if producer.isContinuous(): sq = "C"
            if producer.isHistory(): sq += "H"
            if producer.isLatest(): sq += "L"
            if producer.isStatic(): sq += "S"
            
            t = (producer.getEndpoint().getUrlString(), \
                 str(producer.getEndpoint().getResourceId()), \
                 sq, \
                 str(producer.isSecondary()))
            tuples.append(t)

        if len(tuples) > 0:
            print "%i producers for table %s" % (len(tuples), vdbTableName)
            printTable(tuples, ["Endpoint", "ID", "Query types", "Secondary producer"])
        else:
            print "No producers for table %s" % vdbTableName
                
    def showPrimaryProducer(self, args):
        if len(args) != 0:
            raise CommandLineException("Usage: SHOW PP - try 'help show' for more information")
       
        tuples = []
        tuples.append(("HRP", getIntervalAsString(self.ppHrp)))
        tuples.append(("LRP", getIntervalAsString(self.ppLrp)))
        tuples.append(("Storage", getStorageAsString(self.ppStorage)))
        printTable(tuples)
           
        tuples = []
        for table, predicate in self.ppTables.items():
            tuples.append((table, predicate))
        printTable(tuples, ("Table name", "Predicate"))     
 
    def showSecondaryProducer(self, args):
        if len(args) != 0:
            raise CommandLineException("Usage: SHOW SP - try 'help show' for more information")
       
        tuples = []  
        tuples.append(("HRP", getIntervalAsString(self.spHrp)))
        tuples.append(("Storage", getStorageAsString(self.spStorage)))
        printTable(tuples)
           
        tuples = []
        for table, predicate in self.spTables.items():
            tuples.append((table, predicate))
        printTable(tuples, ("Table name", "Predicate"))     
 
    def showQuery(self, args):
        if len(args) != 0:
            raise CommandLineException("Usage: SHOW QT - try 'help show' for more information")
        else:
            for type in self.queryTypes:
                if self.qt == getattr(rgma.QueryType, type) or self.qt == getattr(rgma.QueryTypeWithInterval, type):
                    print "Query type:", type,
                    if self.qi: 
                        print "with interval of", getIntervalAsString(self.qi)
                    else:
                        print            
                    break
 
    def showTimeout(self, args):
        if len(args) != 0:
            raise CommandLineException("Usage: SHOW TIMEOUT - try 'help show' for more information")
        elif self.timeout:
            print "Timeout: ", getIntervalAsString(self.timeout)
        else:
            print "Timeout: None"

    def showHistory(self, args):
        for command in self.sessionHistory:
            print command
            
    def sleep(self, args):
        args = args.split()
        if len(args) < 1 or len(args) > 2:
            raise CommandLineException("Usage: SLEEP <time> [<units>] - try 'help sleep' for more information")
        time.sleep(self.getTimeInterval(args).getValueAs())
        
    def use(self, args):
        args = args.split()
        if len(args) < 1:
            raise CommandLineException("Usage: USE PRODUCER|MEDIATOR|VDB [<args>] - try 'help use' for more information")
        
        command = args[0].lower()    
        if command == "producer":
            self.useProducer(args[1:])            
        elif command == "mediator":
            self.useMediator(args[1:])
        elif command == "vdb":
            self.useVdb(args[1:])
        else:
            raise CommandLineException("Unrecognized USE command '%s' - try 'help use' for usage information" % command)

    def useVdb(self, args):
        if len(args) != 1:
            raise CommandLineException("Usage: USE VDB <vdbname> - try 'help use' for more information")
        vdbName = args[0].upper()
        if vdbName != self.vdbName:
            if vdbName in ["''", '""']: vdbName = ""
            try:
                self.schema = rgma.Schema(vdbName)
                self.registry = rgma.Registry(vdbName)
                self.getTables()
            except Exception, e:
                self.schema = rgma.Schema(self.vdbName)
                self.registry = rgma.Registry(self.vdbName)
                self.getTables()
                raise CommandLineException(str(e))
            self.vdbName = vdbName      

    def useProducer(self, args):
        if len(args) != 2:
            raise CommandLineException("Usage: USE PRODUCER <endpoint> <resource ID> - try 'help use' for more information")
        else:
            self.directedEndpoints = [rgma.ResourceEndpoint(args[0], args[1]), ]
            
    def useMediator(self, args):
        if len(args) != 0:
            raise CommandLineException("Usage: USE MEDIATOR - try 'help use' for more information")
        else:
            self.directedEndpoints = None

    def describe(self, tableName):
        vdbTableName, schema, registry, vdbName, tableName = self.parseTableName(tableName)
        tableDef = schema.getTableDefinition(tableName)
        viewfor = tableDef.getViewFor()
        columnDefs = tableDef.getColumns()
        if viewfor:
            print "View", tableName, "is on table", viewfor
        else:
            print "Table", tableName
        columnInfo = []
        for column in columnDefs:
            type = str(column.getType())
            if column.getSize() > 0: type = type + "(" + `column.getSize()` + ")"
            columnInfo.append((column.getName(), type, `column.isPrimaryKey()`, `column.isNotNull()`))                               
        printTable(columnInfo, ("Column name", "Type", "Primary key", "Not NULL"))

    def read(self, args):
        args = args.split()
        if len(args) != 1:
            raise CommandLineException("Usage: READ <filename> - try 'help read' for more information")

        try:
            command_file = None
            try:
                command_file = open(args[0], "r")
                commands = command_file.readlines()
            except IOError, e:
                raise CommandLineException("Could not read file '%s': %s" % (args[0], e))

            for command in commands:
                print command.strip()
                self.onecmd(self.precmd(command))
        finally:
            if command_file != None:
                command_file.close()

    def writeHistory(self, file):
        try:
            command_file = None
            try:
                command_file = open(file, "w")
                for command in self.sessionHistory:
                    command_file.write(command + "\n")
                print "Session history written to file '%s'" % file
            except IOError, e:
                raise CommandLineException("Could not write file '%s': %s" % (file, e))

        finally:
            if command_file != None:
                command_file.close()

    def write(self, args):
        args = args.split()
        if len(args) != 2:
            raise CommandLineException("Usage: WRITE HISTORY <filename> - try 'help write' for more information")
        elif args[0].lower() == "history":
            self.writeHistory(args[1])
        else:
            raise CommandLineException("Usage: WRITE HISTORY <filename> - try 'help write' for more information")

    def clear(self, args):
        args = args.split()
        if len(args) != 1 or args[0].lower() != "history":
            raise CommandLineException("Usage: CLEAR HISTORY - try 'help clear' for more information")
        self.sessionHistory = []

    def create(self, args):
        args = args.replace("(", " ( ").split()
        if args[0].lower() != "table": 
            raise CommandLineException("Usage: CREATE TABLE <table name> ... - try 'help create' for more information")
        tname = args[1] 
        vdbtname = tname.split(".")
        if len(vdbtname) == 1:
            schema = self.schema
            tname = vdbtname[0]
        elif len(vdbtname) == 2: 
            schema = rgma.Schema(vdbtname[0])
            tname = vdbtname[1]
        else: 
            raise CommandLineException("Table name may have at most one '.' to separate the VDB prefix")
        statement = "CREATE TABLE " + tname + " ".join(args[2:])
        schema.createTable(statement, self.authzRules)
        self.resultsFile.write("Table " + schema.vdbName + "." + tname + " created with authz rules:\n")
        for rule in schema.getAuthorizationRules(tname):
            self.resultsFile.write("  " + rule + '\n')
        self.rgmaTables[tname] = []
        self.rgmaUpperTableToMixed[tname.upper()] = tname
    
    def drop(self, args):
        args = args.split()
        if len(args) < 1:
            raise CommandLineException("Usage: DROP TABLE|TUPLESTORE [<args>] - try 'help drop' for more information")
        command = args[0].lower()
        if command == "table":
            self.dropTable(args[1:])
        elif command == "tuplestore":
            self.dropTupleStore(args[1:])
        else:
            raise CommandLineException("Unrecognized DROP command '%s' - try 'help drop' for usage information" % command)
        
    def dropTable(self, args):
        if len(args) != 1:
            raise CommandLineException("Usage: DROP TABLE <table name> - try 'help drop' for more information")
        vdbTableName, schema, registry, vdbName, tableName = self.parseTableName(args[0])
        schema.dropTable(tableName)
        if  vdbName == self.vdbName:
            del self.rgmaTables[tableName]
            del self.rgmaUpperTableToMixed[tableName.upper()]
        if vdbTableName in self.ppTables:
            del self.ppTables[vdbTableName]
        if vdbTableName in self.spTables:
            del self.spTables[vdbTableName]

    def dropTupleStore(self, args):
        if len(args) != 1:
            raise CommandLineException("Usage: DROP TUPLESTORE <tuplestore name> - try 'help drop' for more information")
        rgma.RGMAService.dropTupleStore(args[0])
        
    def rules(self, args):
        args = args.split()
        if len(args) < 1:
            raise CommandLineException("Usage: RULES LIST|ADD|DELETE|LOAD|STORE [<args>] - try 'help rules' for more information")
        command = args[0].lower()
        if command == "list":
            self.rulesList(args[1:])            
        elif command == "add":
            self.rulesAdd(args[1:])
        elif command == "delete":
            self.rulesDelete(args[1:])
        elif command == "load":
            self.rulesLoad(args[1:])
        elif command == "store":
            self.rulesStore(args[1:])
        else:
            raise CommandLineException("Unrecognized RULES command '%s' - try 'help rules' for usage information" % command)
        
    def rulesList(self, args):
        if len(args) != 0:
            raise CommandLineException("RULES LIST takes no parameters - try 'help rules' for usage information")
        i = 0
        if len(self.authzRules) == 0:
            self.resultsFile.write("No rules currently defined\n")
        for rule in self.authzRules:
            self.resultsFile.write(`i` + " - " + rule + "\n")
            i = i + 1
            
    def rulesAdd(self, args):
        self.authzRules.append(" ".join(args))
        
    def rulesDelete(self, args):
        if len(args) != 1:
            raise CommandLineException("RULES DELETE takes one parameters - try 'help rules' for usage information")
        try:
            n = int(args[0])
        except:
            raise CommandLineException("Rule number " + args[0] + " does not exist - try 'help rules' for usage information")
        if n < 0 or n >= len(self.authzRules):
            raise CommandLineException("Rule number " + `n` + " does not exist - try 'help rules' for usage information")
        del self.authzRules[n]
        
    def rulesLoad(self, args):
        if len(args) != 1:
            raise CommandLineException("Usage: RULES LOAD <table name> - try 'help rules' for more information")
        vdbTableName, schema, registry, vdbName, tname = self.parseTableName(args[0])
        self.authzRules = schema.getAuthorizationRules(tname)
        
    def rulesStore(self, args):
        if len(args) != 1:
            raise CommandLineException("Usage: RULES STORE <table name> - try 'help rules' for more information")
        vdbTableName, schema, registry, vdbName, tname = self.parseTableName(args[0])
        schema.setAuthorizationRules(tname, self.authzRules)
        
    def list(self, args):
        args = args.split()
        if len(args) < 1:
            raise CommandLineException("Usage: LIST TUPLSESTORES - try 'help list' for more information")
        command = args[0].lower()    
        if command == "tuplestores":
            self.listTupleStores(args[1:])
        else:
            raise CommandLineException("Unrecognized LIST command '%s' - try 'help use' for usage information" % command)
            
    def listTupleStores(self, args):
        if len(args) != 0:
            raise CommandLineException("Usage: LIST TUPLESTORES - try 'help list' for more information")
        rgma.RGMAService.listTupleStores()
        
    #
    # Helper methods
    #
    def parseTableName(self, s):
        vdbtname = s.split(".")
        if len(vdbtname) == 1:
            vdbname = self.vdbName
            schema = self.schema
            registry = self.registry
            tname = vdbtname[0]
        elif len(vdbtname) == 2:
            vdbname = vdbtname[0].upper()
            schema = rgma.Schema(vdbname)
            registry = rgma.Registry(vdbname)
            tname = vdbtname[1]
        else: 
            raise CommandLineException("Table name may have at most one '.' to separate the VDB prefix")
        if  vdbname == self.vdbName:
            ctname = self.rgmaUpperTableToMixed.get(tname.upper())
            if not ctname:
                self.getTables()
                ctname = self.rgmaUpperTableToMixed.get(tname.upper())
                if not ctname:
                    raise CommandLineException("No such table " + vdbname + "." + tname)
        else:
            ctname = None
            tables = schema.getAllTables()
            for table in tables:
                if table.upper() == tname.upper():
                    ctname = table
                    break
            if not ctname:
                raise CommandLineException("No such table " + vdbname + "." + tname) 
        
        return vdbname + "." + ctname, schema, registry, vdbname, ctname
 
    def getTables(self):
        tables = self.schema.getAllTables()
        self.rgmaTables = {}
        self.rgmaUpperTableToMixed = {}
        for table in tables:
            self.rgmaTables[table] = []
            self.rgmaUpperTableToMixed[table.upper()] = table
    
    tw = textwrap.TextWrapper(width=65)
    def ppar(self, par):
        if not self.heading: print
        for line in self.tw.wrap(" ".join(par.split())): print " ", line
        self.heading = False
        
    def phead(self, title):
        print "\n" + SBOLD + title + EBOLD
        self.heading = True
        
    def pcmd(self, line):
        print "rgma> " + SULINE + line + EULINE
        self.heading = False
        
    def getUnits(self, units):
        try:
            return getattr(rgma.TimeUnit, units.upper())
        except AttributeError:
            raise CommandLineException("Unrecognized time units: '%s' - must be seconds, minutes, hours or days" % units)

    def getTimeInterval(self, args):
        if len(args) > 2:
            raise CommandLineException("Too many arguments - time must be of the form <number> <optional units>")
        elif len(args) < 1:
            raise CommandLineException("Time value not specified")

        if len(args) >= 1:
            try:
                value = int(args[0])
            except:
                raise CommandLineException("Invalid time value '%s' - should be of the form <integer> <optional units>" % args[0])
            
        if len(args) == 2:
            try:
                units = self.getUnits(args[1])
            except rgma.RGMAException:
                raise CommandLineException("Unrecognized time units: '%s' - must be seconds, minutes, hours or days" % args[1])
        else:
             units = rgma.TimeUnit.SECONDS
            
        return rgma.TimeInterval(value, units)

    def createNewPrimaryProducer(self):
        self.pp = rgma.PrimaryProducer(self.ppStorage, rgma.SupportedQueries.CHL)
        for vdbTableName, predicate in self.ppTables.items():
            self.pp.declareTable(vdbTableName, predicate, self.ppHrp, self.ppLrp)
            self.ppDeclared.append(vdbTableName)

    def createNewSecondaryProducer(self):
        self.sp = rgma.SecondaryProducer(self.spStorage, rgma.SupportedQueries.CHL)
        for table, predicate in self.spTables.items():
            self.sp.declareTable(table, predicate, self.spHrp)

    #
    # Wrapper methods for commands which handle error uniformly
    #
    def wrap_errors(self, function, args):
        try:
            function(args)
            self.exitcode = 0
        except (rgma.RGMATemporaryException), e:
            sys.stderr.write("Temporary problem: %s\n" % e)
            self.exitcode = 1
        except (rgma.RGMAPermanentException, CommandLineException), e:
            sys.stderr.write("Error: %s\n" % e)
            self.exitcode = 1
        except KeyboardInterrupt:
            sys.stderr.write("Interrupted\n")
            self.exitcode = 1
        except (UnicodeError), e:
            sys.stderr.write("Error: Unexpected unicode %s\n" % e)
            self.exitcode = 1
        except SystemExit:
            raise
        except:
            sys.stderr.write("Error: %s\n" % sys.exc_info()[0])
            self.exitcode = 3
            raise
        
    def do_select(self, args):
        self.wrap_errors(self.select, args)
        
    def do_set(self, args):
        self.wrap_errors(self.set, args)
        
    def do_alter(self, args): 
        self.wrap_errors(self.alter, args)

    def do_insert(self, args):
        self.wrap_errors(self.insert, args)

    def do_create(self, args):
        self.wrap_errors(self.create, args)

    def do_drop(self, args):
        self.wrap_errors(self.drop, args)

    def do_show(self, args):
        self.wrap_errors(self.show, args)
        
    def do_sleep(self, args):
        self.wrap_errors(self.sleep, args)

    def do_use(self, args):
        self.wrap_errors(self.use, args)

    def do_describe(self, args):
        self.wrap_errors(self.describe, args)

    def do_read(self, args):
        self.wrap_errors(self.read, args)

    def do_write(self, args):
        self.wrap_errors(self.write, args)

    def do_clear(self, args):
        self.wrap_errors(self.clear, args)
        
    def do_rules(self, args):
        self.wrap_errors(self.rules, args)
        
    def do_list(self, args):
        self.wrap_errors(self.list, args)
        
    def do_help(self, arg):
        args = arg.split()
        if args:
            try:
                func = getattr(self, 'help_' + args[0])
            except AttributeError:
                try:
                    doc = getattr(self, 'do_' + args[0]).__doc__
                    if doc:
                        self.stdout.write("%s\n" % str(doc))
                        return
                except AttributeError:
                    pass
                self.stdout.write("%s\n" % str(self.nohelp % (arg,)))
                return
            if len(args) == 2:
                func(args[1])
            else:
                func()
        else:
            self.phead("Commands")
            self.ppar("To find more about the following commands type: 'help <command>'")
            
            self.ppar("  Schema operations - create, alter, drop, describe, rules, show")
            
            self.ppar("  Tuple stores - drop, list")
            
            self.ppar("  Producers and consumers - set, show, select, insert")
                       
            self.ppar("  Other operations - help, exit, read, clear, sleep, write, use")
            
            self.phead("Documentation")
            self.ppar("For an overview of the rgma tool type 'help overview'")
            self.ppar("For a set of examples type 'help example'")

    # Aliases of commands
    do_exit = exit
    do_eof = exit

    #
    # Tab-completion code
    #
    def complete_alter(self, text, line, begidx, endidx):
        args = line[:begidx].split()
        num_args = len(args) - 1
        if num_args > 0:
            torv = args[1].lower()
        if num_args > 1:
            tvName = args[2]
        if num_args > 2:
            action = args[3].lower()
        if num_args > 3:
            colName = args[4]
        if num_args > 4:
            type = args[5]
            
        if num_args == 0:
            return self.my_complete(text, ["table ", "view "])
        if num_args == 1:
            return self.my_complete(text, self.rgmaTables.keys())
        if num_args == 2:
            if torv == "table":
                return self.my_complete(text, ["add ", "drop ", "modify "])
            elif torv == "view":
                return self.my_complete(text, ["add ", "drop "])
            
    def complete_set(self, text, line, begidx, endidx):
        args = line[:begidx].split()
        num_args = len(args) - 1
        if num_args > 0:
            property = args[1].lower()
        if num_args > 1:
            subProperty = args[2].lower()
            
        if num_args == 0:
            return self.my_complete(text, ["query ", "timeout ", "pp", "sp"])
        elif property == "query":
            if num_args == 1:
                return self.my_complete(text, self.queryTypes)
            elif num_args == 3:
                return self.my_complete(text, ["seconds", "minutes", "hours", "days"])
        elif property == "timeout":
            if num_args == 1:
                return self.my_complete(text, ["none"])
            elif num_args == 2:
                return self.my_complete(text, ["seconds", "minutes", "hours", "days"])
        elif property == "pp":
            if num_args == 1:
                return self.my_complete(text, ["table ", "lrp ", "hrp ", "storage "])
            elif subProperty == "table":
                if num_args == 3:
                    return self.my_complete(text, ["where ", "none"])
                elif num_args == 2:
                    return self.my_complete(text, self.rgmaTables.keys())
            elif subProperty in ["hrp", "lrp"]:
                if num_args == 3:
                    return self.my_complete(text, ["seconds", "minutes", "hours", "days"])
            elif subProperty == "storage":
                return self.my_complete(text, ["memory", "database"])
        elif property == "sp":
            if num_args == 1:
                return self.my_complete(text, ["table ", "hrp ", "storage "])
            elif subProperty == "table":
                if num_args == 3:
                    return self.my_complete(text, ["where ", "none"])
                elif num_args == 2:
                    return self.my_complete(text, self.rgmaTables.keys())
            elif subProperty == "hrp":
                if num_args == 3:
                    return self.my_complete(text, ["seconds", "minutes", "hours", "days"])
            elif subProperty == "storage":
                return self.my_complete(text, ["memory", "database"])
    
    def complete_sleep(self, text, line, begidx, endidx):
        args = line[:begidx].split()
        num_args = len(args) - 1
        if num_args == 1:
            return self.my_complete(text, ["seconds", "minutes", "hours", "days"])
            
    def complete_show(self, text, line, begidx, endidx):
        args = line[:begidx].split()
        num_args = len(args) - 1
        if num_args > 0:
            property = args[1].lower()
        if num_args > 1:
            subProperty = args[2].lower()

        if num_args == 0:
            return self.my_complete(text, ["tables", "columns from ", "producers of ", "timeout", "query", "pp", "sp", "history"])
        elif num_args == 1:
            if property == "producers":
                return self.my_complete(text, ["of "])
            elif property == "columns":
                return self.my_complete(text, ["from "])
        elif num_args == 2:
            if property == "producers" and subProperty == "of":
                return self.my_complete(text, self.rgmaTables.keys())
            elif property == "columns" and subProperty == "from":
                return self.my_complete(text, self.rgmaTables.keys())
        else:
            return []
          
    def complete_use(self, text, line, begidx, endidx):
        existing_commands = line[:begidx].split()
        if len(existing_commands) == 1:
            return self.my_complete(text, ["producer ", "mediator ", "vdb "])
        else:
            return []
        
    def complete_select(self, text, line, begidx, endidx):
        line = line[:begidx]
        afterSelect = re.compile(r"select.*", re.IGNORECASE).match(line)
        afterFrom = re.compile(r"select\s+.*from.*", re.IGNORECASE).match(line)
        afterWhere = re.compile(r"select\s+.*from\s+.*where.*", re.IGNORECASE).match(line)
        
        if afterWhere:
            return self.my_complete(text, ["order by ", ""])
        elif afterFrom:
            return self.my_complete(text, self.rgmaTables.keys() + ["where ", "order by "])
        elif afterSelect:
            return self.my_complete(text, ["* ", "from "])
                
    def complete_help(self, text, line, begidx, endidx):
        args = line[:begidx].split()
        num_args = len(args) - 1
        if num_args == 0:
            return self.my_complete(text, ['clear', 'create', 'describe', 'drop', 'eof', 'example ',
                                           'exit', 'help', 'insert', 'list', 'overview', 'read',
                                           'rules', 'select', 'set', 'show', 'sleep', 'use', 'write'])
        elif num_args == 1 and args[1] == "example":
            return self.my_complete(text, self.examples)
    
    def complete_insert(self, text, line, begidx, endidx):
        existing_commands = line[:begidx].split()
        if len(existing_commands) == 1:
            return self.my_complete(text, ["into"])
        if existing_commands[len(existing_commands) - 1].lower() == "into":
            return self.my_complete(text, self.rgmaTables.keys())
        
    def complete_list(self, text, line, begidx, endidx):
        args = line[:begidx].split()
        num_args = len(args) - 1
        if num_args == 0:
            return self.my_complete(text, ["tuplestores"])
    
    def complete_describe(self, text, line, begidx, endidx):
        args = line[:begidx].split()
        num_args = len(args) - 1
        if num_args == 0:
            return self.my_complete(text, self.rgmaTables.keys())
 
    def complete_write(self, text, line, begidx, endidx):
        args = line[:begidx].split()
        num_args = len(args) - 1
        if num_args == 0:
            return self.my_complete(text, ["history "])

    def complete_clear(self, text, line, begidx, endidx):
        args = line[:begidx].split()
        num_args = len(args) - 1
        if num_args == 0:
            return self.my_complete(text, ["history"])
        
    def complete_rules(self, text, line, begidx, endidx):
        existing_commands = line[:begidx].split()
        if len(existing_commands) == 1:
            return self.my_complete(text, ["list", "add ", "delete ", "load ", "store "])
        if existing_commands[len(existing_commands) - 1].lower() in ["load", "store"]:
            return self.my_complete(text, self.rgmaTables.keys())
        if existing_commands[len(existing_commands) - 1].lower() == "delete":
            return map(str, range(len(self.authzRules)))
        
    def complete_drop(self, text, line, begidx, endidx):
        existing_commands = line[:begidx].split()
        if len(existing_commands) == 1:
            return self.my_complete(text, ["tuplestore ", "table "])
        if existing_commands[len(existing_commands) - 1].lower() == "table":
            return self.my_complete(text, self.rgmaTables.keys())
              
    def my_complete(self, text, known_commands):
        matches = []
        for command in known_commands:
            if command.startswith(text):
                matches.append(command)
        return matches

    #
    # Help documentation
    #
    
    def help_example(self, example=None):
          
        if not example:
            self.phead("Introduction")
            self.ppar("There are various example scripts in " + self.exampleDir + 
                      """. You may display the formatted script by the command 
                      'help example <example>' where <example> is chosen from: """ + 
                      `self.examples` + """, or run it with the 'read' command. To use the 'read' command please note that 
                      the examples are in the directory '""" 
                      + os.path.join(os.environ["RGMA_HOME"], "share", "doc", "rgma-command-line", "examples") + 
                      "' with an extension of '.rgma'.") 
            self.ppar(""" Some of the files include sleep commands 
                      where they are needed to improve reproducibility results as the R-GMA server 
                      is multi-threaded. However their precise behaviour depends upon the current state of 
                      the R-GMA system""")
        else:
            try:
                f = open(os.path.join(os.environ["RGMA_HOME"], "share", "doc", "rgma-command-line", "examples", example + ".rgma"))
            except:
                raise CommandLineException("This example does not exist")
            
            lines = f.readlines()
            f.close()
            parBuf = []
            for line in lines:
                line = line.strip()
                if line == "": continue
                elif line.startswith("##"):
                    if parBuf:
                        self.ppar(" ".join(parBuf))
                        parBuf = []                    
                    self.phead(line[2:].strip())
                elif line.startswith("#"):
                    if line == "#" and parBuf:
                        self.ppar(" ".join(parBuf))
                        parBuf = [] 
                    parBuf.append(line[1:])
                else:
                    if parBuf: 
                        self.ppar(" ".join(parBuf))
                        parBuf = []     
                    self.pcmd(line)
            if parBuf: 
                self.ppar(" ".join(parBuf))
            
    def help_select(self):
        self.phead("select <columns> from <table> [where <predicate>] [order by <column>]")
        
        self.ppar("""Query R-GMA for information contained in <columns> of <table>
        which satisfies <predicate>, ordering the results according to
        the values in <column>. The type of query can be changed
        using the 'set query' command. Use '*' to denote all the user columns 
        and '**' to include also the system maintained columns.""")
        
        self.ppar("""Note that 'order by' is not supported for continuous queries
        since in this case the tuples are returned in the order they are inserted.""")
        
        self.ppar("""More complex queries are also supported, but the currently used
        vdbName, and use of '*' or '**' may not be respected. The tool tries to understand the query so that it can expand 
        'SELECT *' and  prepend the current vdbName when needed. If the query is not understood it is sent directly to the server to try and process it.
        If you have a problem with a complex
        query try including the '<vdbName>.' prefix for each table in 
        the query and avoid the use of '*'.""")
        
        self.ppar("""A table which does not have :R rule allowing you to read the schema may still 
        have its data read provided it has an effective ::R rule giving access to the data however as the tool is not able to query 
        the schema to learn anything about the table you must including the '<vdbName>.' prefix for each table in 
        the query and avoid the use of '*'.""")
        
    def help_alter(self):
        self.phead("alter table <tableName> add <columnName> <columnType>")
        self.ppar("Adds a column to an existing table")
        
        self.phead("alter table <tableName> drop <columnName>")
        self.ppar("Drops a column from an existing table")
        
        self.phead("alter table <tableName> modify <columnName> <columnType>")
        self.ppar("Modifies the type of column in an existing table")
        
        self.phead("alter view <viewName> add <columnName>")
        self.ppar("Adds a column to an existing view")
        
        self.phead("alter view <viewName> drop <columnName>")
        self.ppar("Drops a column from an existing view")
        
    def help_set(self):
        self.phead("set query L|H|C|S [<time> [<units>]]")
        self.ppar("""Set the query type for SELECT statements.  L, H, C and S denote latest, history, continuous 
        and static queries respectively. If a query interval is specified <units> may be seconds, minutes, hours 
        or days - the default is seconds. 
        """)
               
        self.phead("set timeout <time> [<units>]")
        self.ppar(""""Set the timeout for queries in <units>.
        The <units> may be seconds, minutes, hours or days - the default 
        is seconds.""")
        
        self.phead("set timeout none")
        self.ppar("Clear the timeout for queries.")
                   
        self.phead("set pp|sp table <table>")
        self.ppar("Set the primary (pp) or secondary (sp) producer for <table> to have no predicate.")

        self.phead("set pp|sp table <table> [WHERE] <predicate>")
        self.ppar("""Set the primary (pp) or secondary (sp) producer predicate for <table>. 
        The WHERE keyword is optional.""")
        
        self.phead("set pp|sp table <table> none")
        self.ppar("Disassociate the table from the primary (pp) or secondary (sp) producer.")
        
        self.phead("set pp lrp <time value> [<units>]")
        self.ppar("""Set the latest retention period (lrp) for tuples published by the primary producer. <units> 
        may be seconds, minutes, hours or days - the default is seconds.""")
        
        self.phead("set pp|sp hrp <time value> [<units>]")
        self.ppar("""Set the history retention period (hrp) for the primary (pp) or secondary (sp) producer. <units> 
        may be seconds, minutes, hours or days - the default is seconds.""")
        
        self.phead("set pp|sp storage database|memory|<logical name>")
        self.ppar("""Set the storage for the primary (pp) or secondary (sp) producer.""")
        
    def help_sleep(self):
        self.phead("sleep <time> [<units>]")
        self.ppar(""""Sleep for the specified period.
        The <units> may be seconds, minutes, hours or days - the default 
        is seconds. This command is useful to allow demonstration scripts to have predictable 
        output as the R-GMA server is multi-threaded.""")
                   
    def help_insert(self):
        self.phead("insert into <table> [(<col1>, <col2>...)] values (<value1>, <value2>...)")
        self.ppar("""Inserts the specified values into the specified columns of <table> using a
        primary producer. If the columns are not specified then values for all user columns must be 
        provided in the order shown by the output of the the 'show columns from <table>' command. 
        The properties of the primary producer used to insert the data can be modified by the various 
        'set pp' commands""")

    def help_show(self):
        self.phead("show tables")
        self.ppar("Show a list of tables defined in the Schema for the current VDB")
        
        self.phead("show columns from <table>")
        self.ppar("Displays a comma separated list of column names in a table.")
        
        self.phead("show producers of <table>")
        self.ppar("Show a list of endpoints and resource IDs of producers that publish to <table>")
        
        self.phead("show timeout")
        self.ppar("Show current query timeout")
                                   
        self.phead("show query")
        self.ppar("Show current query type adn interval")
                  
        self.phead("show pp")
        self.ppar("Show primary producer properties")
        
        self.phead("show sp")
        self.ppar("Show secondary producer properties")
 
        self.phead("show history")
        self.ppar("""Each command issued is added to the session history
        (whether it is successful or not). The session history
        can be emptied using the 'clear history' command and
        written to a file using the 'write history' command""")

    def help_describe(self):
        self.phead("describe <table>")
        self.ppar("Show column names and types for <table>")
        
    def help_exit(self):
        self.phead("exit|eof")
        self.ppar("Exit the tool. An EOF (Ctrl-D on Unix) is the easiest way to exit.")
              
    def help_use(self):
        self.phead("use producer <url> <id>")
        self.ppar("""Force all subsequent SELECT queries to use only the specified
        producer. Only one producer may be specified.
        Note that if the producer is invalid or cannot answer the query
        type used, no results will be returned""")
        
        self.phead("use mediator")
        self.ppar("""Use the R-GMA mediator to select producers to answer queries. 
        This overrides any previous 'use producer' commands.""")
        
        self.phead("use vdb <vdbName>")
        self.ppar("""Use the specified VDB for all table names not in the form
        <vdbName>.<tableName> The initial value is 'DEFAULT' or as specified 
        on the command line.""")

    def help_read(self):
        self.phead("read <filename>")
        self.ppar("""Read in the specified file and execute the commands contained in it.
        One command must be specified per line.
        """)

    def help_write(self):
        self.phead("write history <filename>")
        self.ppar("Write the session command history to a file.")
        self.ppar("""
        Each command issued is added to the session history
        (whether it is successful or not). The session history
        can be displayed using the 'show history' command and
        emptied using the 'clear history' command""")
                
    def help_clear(self):
        self.phead("clear history")
        self.ppar("Reset the session command history.")
        self.ppar(""")
        Each command issued is added to the session history
        (whether it is successful or not). The session history
        can be displayed using the 'show history' command and written
        to a file using the 'write history' command""")

    def help_create(self):
        self.phead("create table <table name> (col1 type1 [PRIMARY KEY], col2...)")
        self.ppar("Create a table in the R-GMA virtual database schema")
        self.ppar("""The authorization rules currently defined will be applied to the table. 
        See 'help rules' for manipulating authorization rules""")

    def help_drop(self):
        self.phead("drop table <table name>")
        self.ppar("Delete a table from the R-GMA virtual database schema")
        self.phead("drop tuplestore <tuplestore name>")
        self.ppar("""
        Delete a tuplestore from the R-GMA virtual database schema
        The keyword 'tuplestore' may be abbreviated to 'ts'""")
        
    def help_rules(self):
        self.phead("rules list")
        self.ppar("""
        List authorization rules that will be applied to any 'create table' or 'rules 
        store <table name>' operation. The rules are displayed with a number that may 
        be used to delete the rule with the 'rules delete <rule number>' command.""")

        self.phead("rules add <rule>")
        self.ppar("Add the rule to the set of rules. No checking is performed at this time")

        self.phead("rules delete <rule number>")
        self.ppar("""Delete the rule with the specified sequence number starting from zero. When a 
        rule is deleted all rules with higher numbers are renumbered.""")
        
        self.phead("rules store <table name>")
        self.ppar("Store the current rules as a new set of authrization rules for the specified table.")

        self.phead("rules load <table name>")
        self.ppar("Load the current rules from those associated with the specified table.")
        
    def help_list(self):
        self.phead("list tuplestores")
        self.ppar("List available tuplestores.")
        
    def help_help(self):
        self.phead("help [<command>]")
        self.ppar("""Print help on the specified command. If the <command> 
        is omitted print a list of available commands.""")
        self.ppar("""Use 'help overview' for information on getting started
        and 'help examples' for a list of example commands""")
        
    help_eof = help_exit

    def help_overview(self):
        self.phead("rgma CLI overview")
        
        self.ppar("""It is strongly recommended that you read one of the user guides first
        to undertsand what R-GMA is all about. This tool looks rather similar to a stripped down RDBMS 
        CLI accepting restricted SQL SELECT, INSERT, CREATE, DROP and DESCRIBE commands as well as  
        some others specific to this tool. The command may be invoked with the name of a vdb (by default 'Default') 
        which may subsequently be changed by the command 'use vdb <vdbName>'.  Subsequently unqualified 
        table names are treated as though prepended by this vdb name. The tool also offers command line 
        completion and emacs style editing on Unix platforms.""")
        
        self.ppar("""At any one time there is at most one primary producer, one secondary 
        producer and one consumer in existence - so they do not need to be identified. The properties of these 
        producers and consumers are determined by various 'set' commands and may be queried by 'show timeout', 
        'show query', 'show sp' and 'show pp' where 'pp' and 'sp' denote primary and secondary producers 
        respectively.""")
        
        self.ppar("""When a 'select' command is issued a new consumer is created and data are popped and displayed. 
        The results of each 'pop' is formatted and displayed immediately. A simple parser takes the query 
        and, if it can understand it, translates * to a list of all the user column names and ** to also include the
        system columns. It will also prepend the current vdb name 
        to any unqualified table names. If the simple parser fails then the whole query is passed unchanged via an API 
        to the server which may then reject the query. If you get errors with a particular query then avoid the use of '*' and of 
        '**' and prepend all table names with the vdb name. The actual query passed to the API will be displayed in
        this case. The operation can be interrupted with a Ctrl-C.""")
        
        self.ppar("""The equivalent of the 'declareTable' call in the APIs is 'set pp table' and 'set sp table' for 
        the primary and secondary producer respectively. This command can be used to declare a table to be associated 
        with the producer, to specify a predicate or to remove the association. For an 'insert' call a primary producer
        is created if necessary and the table declared using any prespecified values from the 'set pp table' command. After 
        a 'set sp table' the secondary producer is created and the necessary declarations made. Changing a property of a 
        secondary producer - such as its storage mechanism causes the existing secondary producer to be closed 
        down and a new one created. In the APIs it is always necessary to list all the columns for an 'insert'. 
        Here the column list may be omitted and you must then give all the values for the user columns. If you want to 
        specify the RgmaTimestamp as well you must list all the column names.""")
        
        self.ppar("""There is no facility to send 'showSignOfLife' messages to the secondary producer 
        so it's lifetime will normally be determined by its history retention period (HRP) and when it picked 
        up its last tuple. This tool is not intended for managing secondary producers - but simply experimenting with them.""")
        
        self.ppar("""Type 'help' to get a list of commands and 'help <command.' to find out about a specific command. 
        There is a 'help overview' that displays the text you are reading now and 'help example' to provide a set 
        of examples and instructions upon how to run them.""") 
         
    #
    # Generic command processing functions
    #
    
    def precmd(self, command):
        """
        Remove white space from the ends and any trailing ";"
        
        Remove comments
        
        Convert first word to lower case
        
        Log all commands
        """
        processed_command = command.strip().rstrip(";").rstrip()
        
        if processed_command.startswith("#"): processed_command = ""
        
        splitCommand = processed_command.split(" ", 1)
        splitCommand[0] = splitCommand[0].lower()
        if splitCommand[0] == "help":
            if len(splitCommand) > 1:
                splitCommand[1] = splitCommand[1].lower()
        if processed_command: self.sessionHistory.append(command.rstrip())

        return " ".join(splitCommand)
    
    def default(self, line):
        """
        Unrecognized command
        """
        sys.stderr.write("ERROR: Unknown command: '%s' -try 'help' for a list of commands\n" % line)
        self.exitcode = 1
        
    def emptyline(self):
        return
    
#
# Startup and command-line argument handling code
#
    
def showHelp():
    print
    print "R-GMA command line tool"
    print "======================="
    print
    print "Provides command-line access to the R-GMA information and"
    print "monitoring system"
    print
    print "Usage: rgma [-h, --help] [-v, --version] [-c <command>] [-f <file>] [<vdbName>]"
    print
    print "  -h, --help    - Display usage information"
    print "  -v, --version - Display version"
    print "  -c <command>  - Execute <command> then exit. If specified"
    print "                  more than once, execute commands in the order"
    print "                  they appear, then exit"
    print "  -f <file>     - Execute commands in <file>, in batch"
    print "  <vdbName>     - Start with specified vdbName rather than the default"
    print
    
def showVersion():
    print
    print "R-GMA command line tool version @VERSION@"
    print

if __name__ == "__main__": main()

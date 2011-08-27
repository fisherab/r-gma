#!/usr/bin/env python

import sys, optparse, threading, os

def runSQL(sql):
    cmd = "mysql -u " + DBAdminUser + " --password=" +DBAdminPassword + ' -e "' + sql + '"' + " " + options.db
    stdin, stdout, stderr = os.popen3(cmd)        
    stdin.close()
    stdoutReader = NonBlockingReader(stdout)
    stderrReader = NonBlockingReader(stderr)
    stdoutReader.start()
    stderrReader.start()
    stdoutReader.join()
    stderrReader.join()
    err = stderrReader.read()
    if err: terminate(err)
    return stdoutReader.read().split("\n")[1:-1]
    
class NonBlockingReader(threading.Thread):
    def __init__(self,file):
        threading.Thread.__init__(self)
        self.file = file
        
    def run(self):
        self.result = self.file.read()

    def read(self):
        return self.result

def terminate(msg):
    print >> sys.stderr, msg
    sys.exit(1)

def add(sql, name, value):
    if value:
        if sql[1]:
            sep = "AND"
        else:
            sep = "WHERE"
        sql[0] = sql[0] + " " + sep + " " + name + " LIKE '" + value + "'"
        sql[1] = 1

parser = optparse.OptionParser(usage="""

    rgma-show-db [options] <DBAdminUser>  <DBAdminPassword>
    
    Find information on permanent storage at the DB level

    Note that SQL wildcards may be used with the logicalName and dn restrictions.
    These are:

        % to match any number of characters - including none
        _ to match exactly one character
    """, description="")

parser.add_option("--logicalName", help="restrict to the specified logical name - with SQL wildcards")
parser.add_option("--dn", help = "restrict to the specified DN - with SQL wildcards")
parser.add_option("--db", help = "name of the RGMA DB [_RGMA_]", default = "_RGMA_")
parser.add_option("--pretty", help = "wrap the output onto multiple lines", action = "store_true")
options, args = parser.parse_args()
if (len(args) != 2): terminate("There must be exactly two arguments")
DBAdminUser, DBAdminPassword = args

sql = ["SELECT logicalName, vdbTableName, physicalTableName, ownerDN, tableType FROM TupleStore_Mapping", 0]
add(sql, "logicalName", options.logicalName)
add(sql, "ownerDN", options.dn)

out = runSQL(sql[0])

for t in out:
    logicalName, vdbTableName, physicalTableName, ownerDN, tableType = t.split()
    out = runSQL("SELECT COUNT(*) FROM " + physicalTableName)
    count = int(out[0])
    
    if options.pretty:
        print "Owner DN     :", ownerDN
        print "Logical Name :", logicalName
        print "Table Name   :", vdbTableName
        if tableType == "H":
            print "Storage for  : History and continuous queries"
        else:
            print "Storage for  : Latest queries"
        print "DB table name:" ,physicalTableName
        print "Row count    :", count
        if count > 0:
            dateTime  = runSQL("SELECT max(RgmaTimestamp) FROM " + physicalTableName)[0]
            print "Last write  :", dateTime
        print
    else:
        print ownerDN + "/" + logicalName + " " + vdbTableName + "(" + tableType + ") as", physicalTableName, "contains", count, "rows",
        if count > 0:
            dateTime  = runSQL("SELECT max(RgmaTimestamp) FROM " + physicalTableName)[0]
            print "Last write:", dateTime,
        print


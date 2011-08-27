#!/usr/bin/env python

# editvdb
#
#This script modifies a VDB definition file
#

import cmd, sys, os, xml.dom.minidom, re, textwrap, time

class CommandLineException(Exception):

    def __init__(self, message):
        Exception.__init__(self, message)
    

class EditVDB(cmd.Cmd):

    hostNamePattern = re.compile("[a-zA-Z]\\w*\\.([a-zA-Z]\\w*\\.)+[a-zA-Z]\\w*");

    vdbNamePattern = re.compile("[a-zA-Z][0-9a-zA-Z\\$_]*$");

    RSPattern = re.compile("R|S|RS|SR$");

    VDBFileNamePattern = re.compile("[a-z][0-9a-z\\$_]*\\.xml$");

    prompt = "> "

    cdir = os.getcwd()

    tw = textwrap.TextWrapper(width=65)

    def __init__(self):
        cmd.Cmd.__init__(self)
        self.init()

    def precmd(self, line):
        line = line.split()
        if len(line) == 0: return ""
        cmd = line[0].lower()
        return cmd + " " + " ".join(line[1:])

    def ppar(self, par):
        print
        for line in self.tw.wrap(" ".join(par.split())): print " ", line
        
    def emptyline(self):
        pass
 
    def init(self):
        self.dom = xml.dom.minidom.parseString('<?xml version="1.0" encoding="UTF-8" standalone="yes"?><vdb/>')
        self.vdb = self.dom.documentElement
        self.ruleNum = 0;
        self.rules = {}
        self.hosts = {}
        self.modified = False

    def checkOK(self, op):
        if not self.modified: return True
        print "You will lose any changes to the current buffer in memory"
        print "Enter 'y' to " + op + " - anything else to abandon the operation > ",
        response = sys.stdin.readline()
        return response.strip() == "y"

    def checkHost(self, host):
        name = host.getAttribute("name")
        if not self.hostNamePattern.match(name):
            print "host name", name, "must be a fully qualified hostname"
            return False
        port = host.getAttribute("port")
        if not port.isdigit():
            print "host port", port, "must be an integer"
            return False
        return True
    
    def checkAction(self, action):
        c = action.count("C")
        w = action.count("W")
        r = action.count("R")
        if c > 1 or w > 1 or r > 1 or c + w + r != len(action):
            print "action", action, "must contain at most one of C R and W and nothing else."
            return False
        return True
    
    def checkVDB(self):
        schemas = []
        registry = False
        ruleFound = False
        for node in self.vdb.childNodes:
            if node.nodeType == xml.dom.Node.ELEMENT_NODE:
                if node.nodeName == "host":
                    host = node
                    if not self.checkHost(host): return False
                    if host.getAttribute("masterSchema") == "true": schemas.append(host.getAttribute("name"))
                    if host.getAttribute("registry") == "true": registry = True
                else:
                    rule = node
                    action = rule.getAttribute("action")
                    if not self.checkAction(action): return False
                    if action.find("C") >= 0:
                        ruleFound = True

        if len(schemas) != 1:
            print "There must be exactly one machine configured as master schema.\nYou have:",
            if len(schemas) == 0:
                print "none defined"
            else:
                print ", ".join(schemas)
            return False
        
        if not registry:
            print "There must be at least one machine configured as a registry"
            return False

        name = self.vdb.getAttribute("name")    
        if not self.vdbNamePattern.match(name):
            print "Name of vdb is: '" + name + "'. Name is not good - see 'help set name'"
            return False

        if not ruleFound:
            print "At least one rule with a 'C' action must be defined"
            return False

        return True

    ###########################################################################################       

    def help_help(self):
        print "\nhelp <command>"

    def help_overview(self):
        print "\nEditVDB: a tool for editing a file defining a VDB."
        
        self.ppar("""
        You can start by reading an existing file or build from scratch within the
        tool. After editing you can write it out to a file - it will be checked before
        writing so that it should not be possible to write out a file which is not
        at least syntactically correct. There is also a manual operation (the command
        'check')to confirm while editing that a file could be written.
        """)
        
        self.ppar("""
        You must define exactly one master schema and at least one registry. 
        It is recommended to define two or three registries.
        """)
        
        self.ppar("""
        You must also define at least one schema authorization rule that includes a 'C' action. For example
        'add rule C' will allow anyone to create tables. See 'help add'.""")
        
        self.ppar("""
        Each command has a short help. For example to get help on the 'read' command type
        'help read'.
        """)
        
    ###########################################################################################       

    def do_read(self, args):
        try:
            if not self.checkOK("read the file"):
                return
            fullname = os.path.join(self.cdir, args)
            fname = os.path.basename(fullname)
            if not self.VDBFileNamePattern.match(fname):
                raise CommandLineException("File " + fname + " does not have name a valid name (lower case letter, then lower case letters, digits, _ and $ then '.xml')")
            if not os.path.exists(fullname):
                raise CommandLineException("File " + fullname + " does not exist")
            try:
                self.dom = xml.dom.minidom.parse(fullname)
                self.modified = True
                self.vdb = self.dom.documentElement
                
                OK = True
                if not (self.vdb.getAttribute("name") + ".xml").lower() == fname:
                    print "File name must be lower case of internal name with '.xml' extension"
                    OK = False
                    
                self.ruleNum = 0;
                self.rules = {}
                self.hosts = {}
                badNodes = []
                for node in self.vdb.childNodes:
                    if node.nodeType == xml.dom.Node.ELEMENT_NODE:
                        if node.nodeName == "host":
                            if not self.checkHost(node): OK = False
                            name = node.getAttribute("name")
                            if name in self.hosts:
                                print "Host", name, "is defined more than once"
                                OK = False
                            else:
                                self.hosts[name] = node
                        elif node.nodeName == "rule":
                            self.rules[self.ruleNum] = node
                            self.ruleNum += 1
                        else: badNodes.append(node)
                    else:
                        badNodes.append(node)

                for node in badNodes:
                    self.vdb.removeChild(node)
    
                if not OK:
                    self.init()
                else:
                    self.modified = False;
                    self.cdir = os.path.dirname(fullname)
                    return True
        
            except (Exception), e:
                print "Unable to parse xml:", e
        except (Exception), e:
            print e

    def help_read(self):
        print "\nread <file>"
        self.ppar("""Read the specified file. It must be lower case and have
        an extension of '.xml'. A relative path may be specified. This is relative to
        the default directory.  The default directory is initialised to the current working
        directory and is updated by each succesful read or write operation.""")
    
    ######################################################################################################

    def do_exit(self, args):
        try:
            if self.checkOK("exit"):
                print "Bye"
                sys.exit()
        except (CommandLineException), e:
            print e
            
    def do_eof(self, args):
        self.do_exit(args)

    def help_exit(self):
        print
        print "exit"
        print "This exits the program. If you have unsaved changes you will be asked what to do"

    ######################################################################################################

    def do_set(self, args):
        try:
            args = args.split()
            if len(args) < 1:
                raise CommandLineException("Usage: set name <value> - try 'help set' for more information")
            command = args[0].lower()
            if command == "name":
                self.setName(args[1:])
            else:
                raise CommandLineException("Unrecognized SET command '%s' - try 'help set' for usage information" % command)
        except (Exception), e:
            print e

    def setName(self, args):
        if len(args) != 1:
            raise CommandLineException("Usage: set name <value> - try 'help set' for more information")
        name = args[0]
        if not self.vdbNamePattern.match(name):
            raise CommandLineException("Name of vdb is not good - try 'help set' for more information")

        self.vdb.setAttribute("name", name)
        self.modified = True;

    def help_set(self):
        print "\nset name <name>"
        self.ppar("""
        Sets the name of the vdb. The name must start with a letter
        and be followed by any combination of letters, digits, the dollar sign
        or the underscore character.""")
        
    ######################################################################################################

    def do_clear(self, args):
        try:
            if self.checkOK("clear"):
                self.init()
        except (Exception), e:
            print e

    def help_clear(self):
        print "\nclear"
        self.ppar("Re-initialise the vdb")

    ######################################################################################################

    def do_write(self, args):
        try:
            args = args.split()
            if len(args) == 0:
                wdir = self.cdir
            elif len(args) == 1:
                wdir = os.path.join(self.cdir, args[0])
                if not os.path.isdir(wdir): raise CommandLineException(wdir + " is not a directory")
            else:
                raise CommandLineException("Usage: write [<directory>] - try 'help write' for more information")
            if not self.checkVDB(): raise CommandLineException("Not writing because of errors listed above")
            name = self.vdb.getAttribute("name").lower() + ".xml"
            fullname = os.path.join(wdir, name)
            f = open(fullname, "w")
            f.write(self.dom.toprettyxml())
            f.close()
            self.cdir = os.path.dirname(fullname)
            self.modified = False
            print "vdb definition for '" + self.vdb.getAttribute("name") + "' stored in '" + self.cdir + "'"
        except (Exception), e:
            print e
          
    def help_write(self):
        print "\nwrite [<directory>]"
        self.ppar("""Writes the vdb to a file in the specified directory after checking that it has 
            all the information to produce a syntactically correct file. If no directory is specified it is
            written to the current default directory.""")
        self.ppar("""An absolute or relative path may be specified. If it is relative then it is relative to
            the default directory. The default directory is initialised to the current working
            directory and is updated by each succesful read or write operation.""")

    ######################################################################################################

    def do_check(self, args):
        try:
            if self.checkVDB(): print "It looks good!"
        except (Exception), e:
            print e
        
    def help_check(self):
        print "\ncheck"
        self.ppar("""Check that the vdb is free of errors and so could be written out. When a file
            is read in to the program, only minimal checks are made. So to check a
            manually edited file 'read' it and then use the 'check' command.""")

    ######################################################################################################
    
    def do_show(self, args):
        try:
            args = args.split()
            if len(args) < 1:
                raise CommandLineException("No SHOW command - try 'help show' for more information")
            command = args[0].lower()
            if command == "summary":
                self.showSummary(args[1:])
            elif command == "hosts":
                self.showHosts(args[1:])
            elif command == "rules":
                self.showRules()
            else:
                raise CommandLineException("Unrecognized SHOW command '%s' - try 'help show' for usage information" % command)
        except (Exception), e:
            print e

    def showHosts(self, args):
        if len(args) == 0: filterString = None
        elif len(args) == 1: filterString = args[0]
        else: raise CommandLineException("Usage: show hosts [<endingWith>] - try 'help show' for more information")
        for node in self.vdb.childNodes:
            if node.nodeType == xml.dom.Node.ELEMENT_NODE and node.nodeName == "host":
                host = node
                if not filterString or host.getAttribute("name").endswith(filterString):
                    line = host.getAttribute("name") + " " + host.getAttribute("port")
                    r = host.getAttribute("registry") == "true"
                    s = host.getAttribute("masterSchema") == "true"
                    if s or r:
                        line = line + " ("
                        if r: line = line + ("R")
                        if s: line = line + ("S")
                        line = line + ")"
                    print line
                    
    def showRules(self):
        keys = self.rules.keys()
        keys.sort()
        for key in keys:
            rule = self.rules[key]
            line = `key`.center(3) + " - " + rule.getAttribute("action") + " " + rule.getAttribute("credentials")
            print line
                    
    def masters(self):
        schemas = []
        for node in self.vdb.childNodes:
            if node.nodeType == xml.dom.Node.ELEMENT_NODE and node.nodeName == "host": 
                s = node.getAttribute("masterSchema") == "true"
                if s: schemas.append(node.getAttribute("name"))
        return schemas
               
                    
    def showSummary(self, args):
        try:
            print "Default directory is", self.cdir
            name = self.vdb.getAttribute("name")
            if name == "": name = "with name not yet set"
            hosts = 0
            rules = 0
            for node in self.vdb.childNodes:
                if node.nodeType == xml.dom.Node.ELEMENT_NODE:
                    if node.nodeName == "host":
                        hosts += 1
                    elif node.nodeName == "rule":
                        rules += 1
                        
            print "VDB", name, "has", `hosts`, "hosts and", rules, "rules"
        except (Exception), e:
            print e

                    
    def help_show(self):
        print "\nshow summary"
        self.ppar("Shows summary of the vdb")
        
        print "\nshow hosts [<endingWith>]"
        self.ppar("""Show currently defined hosts with names ending in the specified string. For
            example 'show hosts .ac.uk' will list those hosts such as wibble.rl.ac.uk.""")
                
        print "\nshow rules"
        self.ppar("""List the schema authorization rules. The number preceding each rule is 
        used to label a rule so that it can be replaced or deleted - it is not stored in the VDB definition.""")
        
    ######################################################################################################

    def help_add(self):
        print "\nadd host <name> <port> [<RSvalues>]"
        self.ppar("""Add a server to the vdb as a registry, master schema or both and with the
            specified port value.""")

        self.ppar("""RSValues should be 'R', 'S' or 'RS' where the presence of 'R' denotes registry and
            'S' the master schema.""")
        
        print "\nadd rule action <credential>"
        self.ppar("""Add a schema authz to the vdb defining what actions can be performed by 
        those with the specified credentials.
        If the credentials are omitted any authenticated user can perform the action. Actions can 
        be any combination of C R and W. At least one rule must be defined with a C action to allow 
        creation of tables.""")
        
        self.ppar("""The rules are cumulative and can only be used to grant access and not to deny 
        it. These rules will be combined with table specifie rules.""")


    def do_add(self, args):
        try:
            args = args.split()
            if len(args) < 1:
                raise CommandLineException("Usage: add ...  - try 'help add' for more information")
            command = args[0].lower()
            if command == "host":
                self.addHost(args[1:], "add")
            elif command == "rule":
                self.addRule(args[1:])
            else:
                raise CommandLineException("Usage: add ...  - try 'help add' for more information")
        except (Exception), e:
            print e

    def addHost(self, args, op):
        if len(args) < 2 or len(args) > 3: raise CommandLineException("Usage: add host <name> <port> [<RSvalues>]  - try 'help add' for more information")
        name = args[0]
        port = args[1]
        if len(args) == 3:
            rs = args[2].upper()
        else:
            rs = None

        host = self.dom.createElement("host")
        host.setAttribute("name", name)
        host.setAttribute("port", port)

        if rs != None:
            if not self.RSPattern.match(rs): raise CommandLineException("RSvalues must be 'R', 'S' or 'RS'")
            if rs.find("R") >= 0: host.setAttribute("registry", "true")
            if rs.find("S") >= 0: host.setAttribute("masterSchema", "true")
                   
        if self.checkHost(host):
            if op == "add":
                if name in self.hosts: raise CommandLineException("Host " + name + " is defined more than once")
                if host.getAttribute("masterSchema") == "true":
                    masters = self.masters()
                    if len(masters) > 0: 
                         raise CommandLineException("There must be exactly one machine configured as master schema.\nYou have:" + ", ".join(masters))
            else:
                if name not in self.hosts: raise CommandLineException("Host " + name + " is not present in the vdb")
                self.vdb.removeChild(self.hosts[name])

            self.vdb.appendChild(host)
            self.hosts[name] = host
            self.modified = True
        
    def addRule(self, args):
        if len(args) < 1 : raise CommandLineException("Usage: add rule <action> [<credentials>]  - try 'help add' for more information")
        
        rule = self.dom.createElement("rule")
        action = args[0].upper()
        
        if self.checkAction(action):
                   
            rule.setAttribute("action", action)
            if len(args) > 1:
                rule.setAttribute("credentials", " ".join(args[1:]))
                
            self.rules[self.ruleNum] = rule
            self.ruleNum += 1 
            self.vdb.appendChild(rule)
            self.modified = True

    ######################################################################################################

    def help_delete(self):
        print "\ndelete host <name>"
        self.ppar("""Remove a server from the vdb.""")
        
        print "\ndelete rule <number>"
        self.ppar("""Remove a schema authz rule from the vdb.""")

    def do_delete(self, args):
        try:
            args = args.split()
            if len(args) < 1:
                raise CommandLineException("Usage: delete host - try 'help delete' for more information")
            command = args[0].lower()
            if command == "host":
                self.deleteHost(args[1:])
            if command == "rule":
                self.deleteRule(args[1:])
            else:
                raise CommandLineException("Usage: delete host - try 'help delete' for more information")
        except (Exception), e:
            print e

    def deleteHost(self, args):
        if len(args) != 1: raise CommandLineException("Usage: delete host ... - try 'help delete' for more information")
        name = args[0]
        if name not in self.hosts: raise CommandLineException("Host " + name + " is not present in the vdb")
        self.vdb.removeChild(self.hosts[name])
        del self.hosts[name]
        self.modified = True
        
    def deleteRule(self, args):
        if len(args) != 1: raise CommandLineException("Usage: delete rule ... - try 'help delete' for more information")
        try:
            num = int(args[0])
        except:
            raise CommandLineException("Usage: delete rule ... - try 'help delete' for more information")
        if num not in self.rules: raise CommandLineException("Rule " + num + " is not present in the vdb")
        self.vdb.removeChild(self.rules[num])
        del self.rules[num]
        self.modified = True
 
    ######################################################################################################

    def help_replace(self):
        print "\nreplace host <name> <port> [<RSvalues>]"
        self.ppar("""Replace a server in the vdb as a registry, master schema or both and with the
            specified port value.""")

        self.ppar("""RSValues should be 'R', 'S' or 'RS' where the presence of 'R' denotes registry and
            'S' the master schema.""")
        
        print "\nReplace rule <number> <action> [<credentials>]"
        
        self.ppar("""replace a rule, identified by its number, in the vdb.""")


    def do_replace(self, args):
        try:
            args = args.split()
            if len(args) < 1:
                raise CommandLineException("Usage: replace host <name> <port> [<RSvalues>]  - try 'help replace' for more information")
            command = args[0].lower()
            if command == "host":
                self.addHost(args[1:], "replace")
            if command == "rule":
                self.replaceRule(args[1:])
            else:
                raise CommandLineException("Usage: replace host <name> <port> [<RSvalues>]  - try 'help replace' for more information")
        except (Exception), e:
            print e
        
    def replaceRule(self, args):
        if len(args) < 2 : raise CommandLineException("Usage: replace rule <number> <action> [<credentials>]  - try 'help replace' for more information")
        try:
            num = int(args[0])
        except:
            raise CommandLineException("Usage: replace rule <number> <action> [<credentials>]  - try 'help replace' for more information")
        if num not in self.rules: raise CommandLineException("Rule " + num + " is not present in the vdb")
        action = args[1].upper()
        self.checkAction(action)
        rule = self.rules[num]
        rule.setAttribute("action", action)
        if len(args) > 2:
            rule.setAttribute("credentials", " ".join(args[2:]))
        elif rule.hasAttribute("credentials"):
            rule.removeAttribute("credentials")
        self.modified = True

    ######################################################################################################
 
def main():
    print "editvdb - type 'help overview' to get started"
    ed = EditVDB()
    args = sys.argv[1:]
    if len(args) > 1:
         print "Must have at most one argument"
         sys.exit(1)
    if len(args) == 1:
        if not ed.onecmd("read " + args[0]):
            sys.exit(1)
        
    while True:
        try:
            ed.cmdloop()
        except KeyboardInterrupt:
            ed.onecmd("exit")
 
if __name__ == "__main__": main()

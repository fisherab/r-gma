#!/usr/bin/env python

# This utility will fetch vdb xml files from given URLs.
# The URLs are extracted from all vdb_url files found in 
# "$RGMA_HOME/etc/rgma-server/vdb and the downloaded xml
# "files are placed in the directory $RGMA_HOME/etc/rgma-server/vdb.
# "The prefix of the vdb_url file name must match the prefix of the
# "xml file name, these prefixes are the name of the vdb

import os, sys, re, socket, urlparse, httplib, getopt, stat, glob, time
      
def get_xml_file(url, http_proxy):
    data = ""
    returnCode = 0
    conn = None
    socket.setdefaulttimeout(60)
    try:
        if http_proxy:
            hostPortUrl = http_proxy
        else:
            hostPortUrl = url

        hostPort = urlparse.urlparse(hostPortUrl)[1]
        conn = httplib.HTTPConnection(hostPort)
        headers = {"Accept": "*/*"}
        conn.request("GET", url, None, headers)
        response = conn.getresponse()
        if response.status != 200:
            returnCode = response.status
            data = "ERROR: " + response.reason
        else:
            data = response.read()
            
    except:
        data = "ERROR: " + url + " " + str(sys.exc_info()[0])
        if http_proxy:
            data = data + " proxy was " + http_proxy
        returnCode = 1

    if conn:
        conn.close()
        
    return returnCode, data;    
        
def now():
    return time.strftime("%Y-%m-%d %H:%M:%S")
    
def main():    
    rgma_home = sys.argv[1]
    
    try:
        http_proxy = os.environ['http_proxy']
    except KeyError:
        http_proxy = None
    
    xml_file_path = os.path.join(rgma_home, 'var', 'rgma-server', 'vdb')

    for filename in glob.glob(os.path.join(rgma_home, 'etc', 'rgma-server', 'vdb', "*")):
        if filename.endswith("~"):
            continue
        if not filename.endswith(".vdb_url"):
            print >> sys.stderr,  now(), "Unexpected file found", filename
            continue
        url_file = open(filename, "r")
        lines = url_file.readlines()
        url_file.close()
        found = False
        for line in lines:
            line = line.strip()
            if (re.search("^http", line) != None) & (re.search("\.xml", line) != None):
                if found:
                    print >> sys.stderr, now(), filename, "has bad contents - only one line per file is expected"
                    continue
                found = True

                head, tail = os.path.split(filename)
                vdb_name = tail[:-8]

                url = line
                path = urlparse.urlparse(url)[2]
                head, tail = os.path.split(path)
                vdb_name2 = tail[:-4]
                 
                if vdb_name != vdb_name2:
                    print >>sys.stderr,  now(), "Name of VDB '" + vdb_name + "' extracted from the file name " + filename \
                        + " does not match the name of VDB '" + vdb_name2 + "' extracted from the URL " + url 
                    continue        
                
                returncode, xml_data = get_xml_file(url, http_proxy)
                filename = os.path.join(xml_file_path, vdb_name + ".xml")
        
                if returncode == 0:
                    # See if anything has changed
                    changed = not os.path.isfile(filename)
                    if not changed:
                        f = open(filename)
                        old_data = f.read()
                        f.close()
                        changed = old_data != xml_data
                    if changed:
                        xml_file = open(filename, "w")
                        xml_file.write(xml_data)
                        xml_file.close()
                        os.chmod(filename, 0644)
                        print now(), filename, "has been written"


                elif returncode == 404:
                    os.remove(filename)
                    print >>sys.stderr, now(), filename, "has been removed"
                else:
                    print >>sys.stderr, now(), "problem getting xml file", xml_data
        if not found:
            print >> sys.stderr, now(), filename, "has no recognised content"
            
main()

"""
R-GMA API for Python

Most calls are capable or throwing an RGMATemporaryException of an RGMAPermananentException.
"""

import httplib
import os
import signal
import socket
import string
import sys
import time
import types
import urllib
import urlparse
import warnings
import xml.parsers.expat
import threading

__version__ = '@VERSION@'

# 2.4 is needed for decorators
_version = map(int, ((sys.version.split())[0]).split("."))
if _version < [2, 4]:
    raise Exception("This version of Python is too old - you need at least version 2.4")

# Hardcoded five minute connection timeout
_connection_timeout = 300 
socket.setdefaulttimeout(_connection_timeout)

# Some classes defined first so they can be used later

class _Service(object):
    
    def checkOK(self, tupleSet):
        if not tupleSet.getData()[0].getString(0) == "OK":
            raise RGMAPermanentException("Failed to return status of OK");

    def setServiceName(self, serviceName):
        if _state.exception: raise _state.exception        
        self.name = serviceName
        self.path = _state.path + serviceName
        self.url = _state.url + serviceName
           
    def _callServiceMethod(self, command, **params):
  
        for param in params.keys():
            if params[param] == None:
                del params[param]

        try:
            try:
                conn = _state.getConnection()
                
                headers = {"Cache-Control" : "no-cache",
                           "Connection" : "keep-alive"}
                methodAndCommand = command.split(":")
                if len(methodAndCommand) == 1:
                    method = "GET"
                elif len(methodAndCommand) == 2:
                    method = methodAndCommand[0]
                    command = methodAndCommand[1]
                else: 
                    raise Exception("Internal error in the API")
                urlPart = "%s/%s?%s" % (self.path, command, _urlencode(params))
                try:
                    conn.request(method, urlPart, None, headers)
                    resp = conn.getresponse()
                    if resp.status != 200:
                        raise RGMATemporaryException("https://" + _state.host + urlPart, "HTTP error %i (%s)" % (resp.status, resp.reason))
                    xmlData = resp.read()
                except(httplib.HTTPException, AssertionError), e: #Its possible the connection has died
                    conn = _state.refreshConnection(conn)
                    conn.request(method, urlPart, None, headers)
                    resp = self.conn.getresponse()
                    if resp.status != 200:
                         raise RGMATemporaryException("https://" + _state.host + urlPart, "HTTP error %i (%s)" % (resp.status, resp.reason))
                    xmlData = resp.read()
                return _ResponseParser(xmlData).resultSet
            
            except IOError, e:
                conn = _state.refreshConnection(conn)
                raise RGMATemporaryException(_state.host, e.strerror)
            except httplib.HTTPException, e:
                conn = _state.refreshConnection(conn)
                raise RGMATemporaryException(_state.host, "HTTP connection failed: " + `e`)
            except socket.sslerror, e:
                conn = _state.refreshConnection(conn)
                raise RGMATemporaryException("Secure connection failed: " + `e.args`)
            except socket.error, e:
                conn = _state.refreshConnection(conn)
                raise RGMATemporaryException(_state.host, e)
            except AssertionError:
                # This is a workaround for python bug #666219 which can cause AssertionError to be thrown
                # when the server closes unexpectedly.
                conn = _state.refreshConnection(conn)
                raise RGMATemporaryException(_state.host)
        finally:
            _state.releaseConnection(conn)
            
class Resource(_Service):
    """
    A producer or consumer.
    """
    def __init__(self):
        self.dead = False
        
    def _setResourceId(self, resourceId):
        self.resourceId = resourceId

    def _callResourceMethod(self, method, **params):
        if (self.dead): raise RGMAPermanentException("This resource cannot be reused after you have closed or destroyed it.")
        params['connectionId'] = self.resourceId
        return self._callServiceMethod(method, **params)
            
    def close(self):
        """
        Closes the resource. The resource will no longer be available through this API and will be
        destroyed once it has finished interacting with other components. It is not an error if the
        resource does not exist.
        """
        self.checkOK(self._callServiceMethod("close", connectionId=self.resourceId))
        self.dead = True
                      
    def destroy(self):
        """
        Closes and destroys the resource. The resource will no longer be available to any component.
        It is not an error if the resource does not exist.
        """
        self.checkOK(self._callServiceMethod("destroy", connectionId=self.resourceId))
        self.dead = True
        
class RGMAException(Exception):
    """
    Exception thrown when an error occurs in the RGMA application.
    """
    
    def getMessage(self):
        """
        Return the message associated with the exception
        
        :return: the message as a `string`
        """
        return self.args[0]
           
    def getNumSuccessfulOps(self):
        """
        Returns the number of successful operations.
      
        :return: the number of successful operations as an `integer`
        """
        if hasattr(self, "numSuccessfulOps"):
            return self.numSuccessfulOps
        else:
            return 0

    def _setNumSuccessfulOps(self, numSuccessfulOps):
        """
        Sets the number of successful operations (for batch commands).
        
        @param numSuccessfulOps: Number of successful operations (for batch commands).
        """
        self.numSuccessfulOps = int(numSuccessfulOps);
        
            
class RGMATemporaryException(RGMAException):
    """
    Exception thrown when an R-GMA call fails, but calling the method again after a delay may be
    successful.
    """
    pass


class RGMAPermanentException(RGMAException):
    """
    Exception thrown when it is unlikely that repeating the call will be successful.
    """
    pass

class _UnknownResourceException(Exception):
    """
    Exception thrown when it is unlikely that repeating the call will be successful.
    """
    def getMessage(self):
        return "Unknown resource."

class Producer(Resource):
    """
    A producer.
    """
    
    def __init__(self):
        Resource.__init__(self)
    
    def getResourceEndpoint(self):
        """
        Return the resource endpoint to be used in the constructor of a consumer to make a "directed query".
        
        :return: :class:`rgma.ResourceEndpoint`
        """
                               
        return ResourceEndpoint(self.url, self.resourceId)

class RGMAType(object):
    """
    Constants for SQL column types.
    """
    
    def __init__(self, num, name):
        self.num = num
        self.name = name
        self.mapNum[num] = self
        self.mapName[name] = self
        
    def __str__(self):
        return self.name
        
    @staticmethod    
    def _getType(thing):
        if type(thing) == types.IntType:
            return RGMAType.mapNum[thing]
        else:
            return RGMAType.mapName[thing]

RGMAType.mapNum = {}
RGMAType.mapName = {}

RGMAType.INTEGER = RGMAType(4, "INTEGER")
RGMAType.REAL = RGMAType(7, "REAL")
RGMAType.DOUBLE = RGMAType(8, "DOUBLE")
RGMAType.CHAR = RGMAType(1, "CHAR")
RGMAType.VARCHAR = RGMAType(12, "VARCHAR")
RGMAType.TIMESTAMP = RGMAType(93, "TIMESTAMP")
RGMAType.DATE = RGMAType(91, "DATE")
RGMAType.TIME = RGMAType(92, "TIME")
   
class TimeUnit(object):
    """
    Time units.
    """
    
    def __init__(self, seconds):
        """
        This should be treated as a private constructor
        """
        self.seconds = float(seconds)
 
    def _getSecs(self):
        return self.seconds
    
TimeUnit.SECONDS = TimeUnit(1)
TimeUnit.MINUTES = TimeUnit(60)
TimeUnit.HOURS = TimeUnit(60 * 60)
TimeUnit.DAYS = TimeUnit(24 * 60 * 60)

# Rest of classes should be in alphabetical order

class Consumer(Resource):
    """
    Retrieve data from one or more producers by executing a specified SQL query.
    """
    
    def __init__(self, query, queryType, queryInterval=None, timeout=None, producers=None):
        """
        Creates a consumer with the specified query and query type. If a query interval is specified 
        the query will return queries up to some time in the past. 
        If a timeout is specified the query will terminate after that time interval. If the query was still running it will
        have ``aborted`` status. Nomrmally the mediator is used to select producers however a list of producers may 
        also be specified explicitly. 
        
        :arg query: a SQL select statement
        :type query: `string`
        :arg queryType: the type of the query
        :type queryType: :class:`rgma.QueryType` or :class:`rgma.QueryTypeWithInterval`
        :arg queryInterval: the time interval is subtracted from the current time to give a time in the past. The query result will then use data from tuples published after this time.
        :type queryInterval: :class:`rgma.TimeInterval`
        :arg timeout: time interval after which the query will be aborted - if ``None`` the query will never be aborted.
        :type timeout: :class:`rgma.TimeInterval`
        :arg producers: list of producers to contact.
        :type producers: [ :class:`rgma.ResourceEndpoint` ]
        """
 
        Resource.__init__(self)
        self.setServiceName("ConsumerServlet")
        
        _checkObjectType("queryType", queryType, QueryType, QueryTypeWithInterval)
        
        if queryInterval:
            _checkObjectType("queryInterval", queryInterval, TimeInterval)
            timeIntervalSec = queryInterval.getValueAs(TimeUnit.SECONDS)
        else:
            timeIntervalSec = None
  
        if (timeout):
            _checkObjectType("timeout", timeout, TimeInterval)
            timeoutSec = timeout.getValueAs(TimeUnit.SECONDS)
        else:
            timeoutSec = None
           
        producerConnections = []
        if producers:
            for producer in producers:
                producerConnections.append(str(producer.getResourceId()) + " " + producer.getUrlString())
        
        if type(queryType) == QueryType and queryInterval:
            raise RGMAPermanentException("Only a QueryTypeWithInterval may have a queryInterval")
        
        if  type(queryType) == QueryTypeWithInterval and not queryInterval:
             raise RGMAPermanentException("A QueryTypeWithInterval must have a queryInterval")
            
        tupleSet = self._callServiceMethod("createConsumer",
                                          select=query,
                                          queryType=queryType._getName(),
                                          timeIntervalSec=timeIntervalSec,
                                          timeoutSec=timeoutSec,
                                          producerConnections=producerConnections)
                           
        self._setResourceId(tupleSet.getData()[0].getInt(0));
        self.query = query
        self.queryType = queryType
        self.queryInterval = queryInterval
        self.timeout = timeout
        if producers:
            self.producers = producers[:]
        else:
            self.producers = None
        self.eof = False
        
    def abort(self):
        """
        Aborts the query. Pop can still be called to retrieve existing data.
        """
        try:
            self._doAbort()
        except _UnknownResourceException:
            try:
                self._restore();
                self._doAbort();
            except _UnknownResourceException, e:
                raise RGMATemporaryException(e.getMessage())

    def _doAbort(self):
        self.checkOK(self._callResourceMethod("abort"))

    def hasAborted(self):
        """
        Determines if the query has aborted.
        
        :return: boolean ``True`` if the query was aborted either by a user call or a timeout event

        """
        try:
            return self._doHasAborted()
        except _UnknownResourceException:
            try:
                self._restore()
                return self._doHasAborted()
            except _UnknownResourceException, e:
                raise RGMATemporaryException(e.getMessage())
            
    def _doHasAborted(self):
        tupleSet = self._callResourceMethod("hasAborted")
        return tupleSet.getData()[0].getBool(0)
        
    def pop(self, maxCount):
        """
        Retrieves tuples from the result of the query.
        
        :arg maxCount: the maximum number of tuples to retrieve
        :type maxCount: `integer`
        :return: :class:`TupleSet` containing the received tuples. A set with an empty list of tuples is returned if none were found.
        """
        maxCount = int(maxCount)
        try:
            return self._doPop(maxCount)
        except _UnknownResourceException:
            try:
                self._restore();
                tupleSet = self._doPop(maxCount)
                tupleSet._appendWarning("The query was restarted - many duplicates may be returned.")
                return ts
            except _UnknownResourceException, e:
                raise RGMATemporaryException(e.getMessage())

    def _doPop(self, maxCount):
        tupleSet = self._callResourceMethod("pop", maxCount=maxCount)
        if self.eof:
            tupleSet.appendWarning("You have called pop again after end of results returned.");
        eof = tupleSet.isEndOfResults()
        return tupleSet

    def _restore(self):
        c = Consumer(self.query, self.queryType, self.queryInterval, self.timeout, self.producers)
        self._setResourceId(c.resourceId)   

class OnDemandProducer(Producer):
    """
    A client uses an on-demand producer to publish data into R-GMA when the cost of creating each message is high. The
    on-demand producer only generates messages when there is a specific query from a consumer.
    """
    
    def __init__(self, hostName, port):
        """
        :arg hostName: the host name of the system that will respond to queries
        :type hostName: `string`
        :arg port: the port on the specified host that will respond to queries
        :type port: `integer` 
        """
        
        Producer.__init__(self)
        self.setServiceName("OnDemandProducerServlet")
          
        tupleSet = self._callServiceMethod("createOnDemandProducer",
                      hostName=hostName,
                      port=port)
        
        self._setResourceId(tupleSet.getData()[0].getInt(0))
        self.hostName = hostName
        self.port = port
        self.tables = []
        
    class _Table(object):
        def __init__(self, name, predicate):
            self.name = name
            self.predicate = predicate
        
    def declareTable(self, name, predicate):
        """
        :arg name: the name of the table to declare
        :type name: `string`
        :arg predicate: an SQL WHERE clause defining the subset of a table that this producer will publish. To publish to the whole table, an empty predicate can be used.
        :type predicate: `string`
        """
        try:
            self._doDeclareTable(name, predicate);
        except _UnknownResourceException:
            try:
                self._restore();
                self._doDeclareTable(name, predicate)
            except _UnknownResourceException, e:
                raise RGMATemporaryException(e.getMessage())
        self.tables.append(OnDemandProducer._Table(name, predicate));
 
        
    def _doDeclareTable(self, name, predicate):
        self.checkOK(self._callResourceMethod("declareTable", tableName=name, predicate=predicate))
     
    def _restore(self):
        p = OnDemandProducer(self.hostName, self.port)
        for t in self.tables:
            p._doDeclareTable(t.name, t.predicate)
        self._setResourceId(p.resourceId)        
         
class PrimaryProducer(Producer):
    """
    A primary producer publishes information into R-GMA.
    """
 
    def __init__(self, storage, supportedQueries):
        """
        :arg storage: a storage object to define the type and, if permanent, the name of the storage to be used
        :type storage: :class:`rgma.Storage`
        :arg supportedQueries: a supported queries object to define what query types the producer will be able to support.
        :type supportedQueries: :class:`rgma.SupportedQueries` 
        """
        Producer.__init__(self)
        self.setServiceName("PrimaryProducerServlet")
        
        _checkObjectType("storage", storage, Storage)
        _checkObjectType("supportedQueries", supportedQueries, SupportedQueries)
          
        tupleSet = self._callServiceMethod("createPrimaryProducer",
                      type=storage.storageType,
                      logicalName=storage.logicalName,
                      isLatest=["false", "true"][supportedQueries._isLatest()],
                      isHistory=["false", "true"][supportedQueries._isHistory()])
        
        self._setResourceId(tupleSet.getData()[0].getInt(0));
        self.storage = storage
        self.supportedQueries = supportedQueries
        self.tables = []
        
    class _Table(object):
        def __init__(self, name, predicate, historyRetentionPeriod, latestRetentionPeriod):
            self.name = name
            self.predicate = predicate
            self.historyRetentionPeriod = historyRetentionPeriod
            self.latestRetentionPeriod = latestRetentionPeriod
        
    def declareTable(self, name, predicate, historyRetentionPeriod, latestRetentionPeriod):
        """
        :arg name: the name of the table to declare
        :type name: `string`
        :arg predicate: an SQL WHERE clause defining the subset of a table that this producer will publish. To publish to the
                whole table, an empty predicate can be used.
        :type predicate: `string`
        :arg historyRetentionPeriod:
                the history retention period for this table
        :type historyRetentionPeriod: :class:`rgma.TimeInterval`
        :arg latestRetentionPeriod:
            the default latest retention period for tuples inserted into this table. It can be overridden when
            inserting tuples.
        :type latestRetentionPeriod: :class:`rgma.TimeInterval`
        """
        _checkObjectType("historyRetentionPeriod", historyRetentionPeriod, TimeInterval)
        _checkObjectType("latestRetentionPeriod", latestRetentionPeriod, TimeInterval)
        try:
            self._doDeclareTable(name, predicate, historyRetentionPeriod, latestRetentionPeriod);
        except _UnknownResourceException:
            try:
                self._restore();
                self._doDeclareTable(name, predicate, historyRetentionPeriod, latestRetentionPeriod);
            except _UnknownResourceException, e:
                raise RGMATemporaryException(e.getMessage())
        self.tables.append(PrimaryProducer._Table(name, predicate, historyRetentionPeriod, latestRetentionPeriod));

        
    def _doDeclareTable(self, name, predicate, historyRetentionPeriod, latestRetentionPeriod):
        self.checkOK(self._callResourceMethod("declareTable", tableName=name,
                 predicate=predicate,
                 hrpSec=int(historyRetentionPeriod.getValueAs()),
                 lrpSec=int(latestRetentionPeriod.getValueAs())))        

    def _restore(self):
        p = PrimaryProducer(self.storage, self.supportedQueries)
        for t in self.tables:
            p._doDeclareTable(t.name, t.predicate, t.historyRetentionPeriod, t.latestRetentionPeriod)
        self._setResourceId(p.resourceId)        
        
    def insert(self, insertStatement, latestRetentionPeriod=None):
        """
        Publishes a single tuple or a list of tuples into a table.
        
        :arg insertStatement:  a string or list of strings representing a SQL INSERT statement providing the data to publish and the table into which to put it
        :type insertStatement: `string` or `[str]`

        :arg latestRetentionPeriod: latest retention period for this tuple or tuples (overrides LRP defined for table)
        :type latestRetentionPeriod: :class:`rgma.TimeInterval`
        """
        try:
            self._doInsert(insertStatement, latestRetentionPeriod);
        except _UnknownResourceException:
            try:
                self._restore();
                self._doInsert(insertStatement, latestRetentionPeriod);
            except _UnknownResourceException, e:
                raise RGMATemporaryException(e.getMessage());
        
    def _doInsert(self, insertStatement, latestRetentionPeriod=None):
        if latestRetentionPeriod:
            _checkObjectType("latestRetentionPeriod", latestRetentionPeriod, TimeInterval)
            lrpSec = int(latestRetentionPeriod.getValueAs(TimeUnit.SECONDS))
        else:
            lrpSec = None
        self.checkOK(self._callResourceMethod("POST:insert", insert=insertStatement, lrpSec=lrpSec))

class ProducerTableEntry(object):
    def __init__(self, endpoint, secondary, continuous, static, history, latest, predicate, hrpSec):
        self.endpoint = endpoint
        self.secondary = secondary
        self.continuous = continuous
        self.static = static
        self.history = history  
        self.latest = latest
        self.predicate = predicate  
        self.hrp = TimeInterval(hrpSec)
        
    def getEndpoint(self):
        """
        Returns the producer's endpoint.
        
        return: :class:`rgma.ResourceEndpoint`
        """
        return self.endpoint

    def getPredicate(self):
        """
        Return the predicate.
        
        :return: `string`
        """
        return self.predicate;
    
    def getRetentionPeriod(self):
        """
        Returns the producer's history retention period.
        
        :return: :class:`rgma.TimeInterval`
        """
        return self.hrp
 
    def isContinuous(self):
        """
        Does the producer support continuous queries?
        
        :return: boolean ``True`` if producer supports continuous queries
        """
        return self.continuous

    def isHistory(self):
        """
        Does the producer support history queries?
        
        :return: boolean ``True`` if producer supports history queries
        """
        return self.history
    
    def isLatest(self):
        """
        Does the producer support latest queries?
        
        :return: boolean ``True`` if producer supports latest queries
        """
        return self.latest

    def isSecondary(self):
        """
        Is the producer a secondary producer?
        
        :return: boolean ``True`` if producer is secondary
        """
        return self.secondary
    
    def isStatic(self):
        """
        Does the producer support static queries?
        
        :return: boolean ``True`` if producer supports static queries
        """
        return self.static;

    def __str__(self):
        s = "ProducerTableEntry[endpoint=" + str(self.endpoint) + ", type="
        if self.isContinuous: s = s + 'C'
        else: s = s + '-'
        if self.isHistory: s = s + 'H'
        else: s = s + '-'
        if self.isLatest: s = s + 'L'
        else: s = s + '-'
        if self.isSecondary: s = s + 'R'
        else: s = s + '-'
        if self.isStatic: s = s + 'S'
        else: s = s + '-'
        return s + ", predicate=\"" + self.predicate + "\", retention period=" + `self.hrpSec` + "]"
            
class _Properties(object):

    def __init__(self, filename):
        """
        Loads properties from a name=value format props file
        """
        try:
            file = open(filename)
        except:
            raise RGMAPermanentException("Property file " + filename + " does not exist")
        
        lines = file.readlines()
        file.close()

        self.properties = {}
        for line in lines:
            line = line.strip()
            if line == "" or line.startswith("#"): continue
            equals_index = line.find("=")
            
            if equals_index >= 0:
                property = line[:equals_index].strip()
                value = line[equals_index + 1:].strip()
                self.properties[property] = (value)
            else:
                raise RGMAPermanentException("Property file " + filename + " has a bad entry: " + line)

    def getProperty(self, property):
        try:
            return self.properties[property]
        except KeyError:
            return None
            
class QueryType(object):
    """
    Permitted query type for a consumer.
    """
    
    def __init__(self, name):
        """
        Private constructor
        """
        self.name = name
        
    def _getName(self):
        return self.name

QueryType.C = QueryType("continuous")
QueryType.H = QueryType("history")
QueryType.L = QueryType("latest")
QueryType.S = QueryType("static")

class QueryTypeWithInterval(object):
    """
    Permitted query type for a consumer where an interval for the query should be specified.
    """
    def __init__(self, name):
        """
        Private constructor
        """
        self.name = name
        
    def _getName(self):
        return self.name

QueryTypeWithInterval.C = QueryTypeWithInterval("continuous")
QueryTypeWithInterval.H = QueryTypeWithInterval("history")
QueryTypeWithInterval.L = QueryTypeWithInterval("latest")

class Registry(_Service):
    """
    A registry provides information about available producers.
    """
    
    def __init__(self, vdbName):
        """
        :arg vdbName: name of VDB
        :type vdbName: `string`
        """
        self.setServiceName("RegistryServlet")
        self.vdbName = vdbName

    def getAllProducersForTable(self, tableName):
        """
        Returns a list of all producers for a table with their registered information.
        
        :arg name: name of table
        :type name: `string`
        :return: a list of :class:`ProducerTableEntry` objects, one for each producer
    
        """
        tupleSet = self._callServiceMethod("getAllProducersForTable",
                        vdbName=self.vdbName,
                        tableName=tableName,
                        canForward="true")
        producers = []        
        for producer in tupleSet.getData():
            endpoint = ResourceEndpoint(producer.getString(0), producer.getInt(1))
            pte = ProducerTableEntry(endpoint,
                                     producer.getBool(2), producer.getBool(3), producer.getBool(4), producer.getBool(5), producer.getBool(6),
                                     producer.getString(7), producer.getInt(8))
            producers.append(pte)
        return producers
                
class ResourceEndpoint(object):
    """
    A resource endpoint contains a URL and a resource ID.
    """
 
    def __init__(self, urlString, resourceId):
        """
        :arg urlString: the service URL
        :type urlString: `string`
        :arg resourceId: the resource identifier
        :type resourceId: `int`
        """
        self.urlString = str(urlString)
        self.resourceId = int(resourceId)
        
    def getUrlString(self):
        """
        Gets the resource URL.
        
        :return: the resource URL as a string
        """
        return self.urlString
        
    def getResourceId(self):
        """
        Gets the resource identifier.
        
        :return: return the resourceId as an int
        """
        return self.resourceId

    def __str__(self):
        return "ResourceEndpoint[" + self.urlString + ":" + str(self.resourceId) + "]"

class _ResponseParser(object):
    """
    Parser for XML responses from the RGMA server
    """
    
    def __init__(self, xmlData):
        self.exception = None
        self.endOfResults = False
        self.chars = ""
         
        p = xml.parsers.expat.ParserCreate()
        p.StartElementHandler = self.startElement
        p.EndElementHandler = self.endElement
        p.CharacterDataHandler = self.charData
        try:
            p.Parse(xmlData)
        except Exception, e:
            raise RGMAPermanentException("Internal error: " + `e`)
        
        if self.exception: raise self.exception
        
        if hasattr(self, "data"):         
            self.resultSet = TupleSet(self.data, self.endOfResults, self.warning)
        else:
            raise RGMAPermanentException("Bad XML returned by server")
            
    def startElement(self, qName, attributes):
        if self.exception: 
            return
        if qName == 'r':
            if "c" in attributes:
                self.numCols = int(attributes["c"])
            else:
                self.numCols = 1;
               
            if "r" in attributes:
                self.numRows = int(attributes["r"])
            else:
                self.numRows = 1;
               
            if "m" in attributes:
                self.warning = attributes["m"];
            else:
                self.warning = ""
                
            self.data = []
            self.row = []
            
        elif qName == 'v' or qName == 'n':
            self.chars = ""
        elif qName == 'e':
            self.endOfResults = True
        elif (qName == 't' or qName == 'p'):
            if qName == 't' :
                self.exception = RGMATemporaryException(attributes["m"])
            else:
                self.exception = RGMAPermanentException(attributes["m"])
            if 'o' in attributes:
                self.exception._setNumSuccessfulOps(int(attributes['o']))
        elif qName == 'u':
            self.exception = _UnknownResourceException("Unknown resource.")
        else:
            self.exception = RGMAPermanentException("Unexpected tag " + qName + " in XML from server.")
            

    def endElement(self, qName):
        if self.exception: return 
        if qName == 'v' or qName == 'n':
            if (qName == 'v'):
                self.row.append(self.chars)
                self.chars = ""
            else:
                self.row.append(None)

            if len(self.row) == self.numCols:
                self.data.append(Tuple(tuple(self.row)))
                self.row = []
                
    def charData(self, data):
        """
        Called when character data is found
        """
        self.chars = self.chars + data
        
        
 
class RGMAService(object):

    @staticmethod
    def listTupleStores():
        """
        Returns a list of existing tuple stores.
    
        :return: [ :class:`rgma.TupleStore` ] objects that belong to you
        """
        service = _Service()
        service.setServiceName("RGMAService")
        tupleSet = service._callServiceMethod("listTupleStores")
        stores = []
        for tuple in tupleSet.getData():
            stores.append(TupleStore(tuple.getString(0), tuple.getBool(1), tuple.getBool(2))) 
        return stores
    
    @staticmethod
    def dropTupleStore(logicalName):
        """
        Permanently deletes a named tuple store.
        
        :arg logicalName: name of tuple store
        :type logicalName: `string`
        """
        service = _Service()
        service.setServiceName("RGMAService")
        service.checkOK(service._callServiceMethod("dropTupleStore", logicalName=logicalName))
        
    @staticmethod
    def getVersion():
        """
        Returns the value of the version of the RGMA server in use.
     
        :return: the server version as a `string`
        """
        service = _Service()
        service.setServiceName("RGMAService")
        tupleSet = service._callServiceMethod("getVersion")
        return tupleSet.getData()[0].getString(0)
    
    @staticmethod
    def getTerminationInterval():
        """
        Returns the termination interval that will be applied to all resources.
     
        :return: the termination interval as a :class:`rgma.TimeInterval`
        """
        service = _Service()
        service.setServiceName("RGMAService")
        tupleSet = service._callServiceMethod("getTerminationInterval")
        return TimeInterval(tupleSet.getData()[0].getInt(0))

class Schema(_Service):
    """
    A schema allows tables to be created and their definitions manipulated for a specific VDB.
    """
 
    def __init__(self, vdbName):
        """
        :arg vdbName: name of VDB
        :type vdbName: `string`
        """
        self.setServiceName("SchemaServlet")
        self.vdbName = vdbName
        
    def createTable(self, createTableStatement, authzRules):
        """
        Creates a table in the schema.
        
        :arg createTableStatement: SQL CREATE TABLE statement
        :type createTableStatement: `string`
        :arg authzRules: authorization rule or list of authorization rules 
        :type authzRules: `string` or [ `string` ]
        """
        self._callServiceMethod("createTable",
                       vdbName=self.vdbName,
                       canForward="true",
                       createTableStatement=createTableStatement,
                       tableAuthz=authzRules)

    def dropTable(self, tableName):
        """
        Drops a table from the schema.
        
        :arg tableName: name of table to drop
        :type tableName: `string`
        """
        self._callServiceMethod("dropTable",
                       vdbName=self.vdbName,
                       canForward="true",
                       tableName=tableName)
        
    def alter(self, torv, tableName, action, colName, type=None):
        self._callServiceMethod("alter",
                                vdbName=self.vdbName,
                                canForward="true",
                                tableOrView=torv,
                                tableName=tableName,
                                action=action,
                                name=colName,
                                type=type)
                             
    def createIndex(self, createIndexStatement):
        """
        Creates an index on a table.
        
        :arg createIndexStatement: SQL CREATE INDEX statement
        :type createIndexStatement: `string`
        """
        self._callServiceMethod("createIndex",
                       vdbName=self.vdbName,
                       canForward="true",
                       createIndexStatement=createIndexStatement)

    def dropIndex(self, tableName, indexName):
        """
        Drops an index from a table.
        
        :arg tableName: name of table with index to drop
        :type tableName: `string`
        :arg indexName: name of index to drop
        :type indexName: `string`
        """
        self._callServiceMethod("dropIndex",
                       vdbName=self.vdbName,
                       canForward="true",
                       tableName=tableName,
                       indexName=indexName)

    def createView(self, createViewStatement, authzRules):
        """
        Creates a view on a table.
        
        :arg createViewStatement: SQL CREATE VIEW statement
        :type createViewStatement: `string`
        :arg authzRules: authorization rules or list of authorization rules 
        :type authzRules: `string` or [ `string` ]
        """
        self._callServiceMethod("createView",
                       vdbName=self.vdbName,
                       canForward="true",
                       createViewStatement=createViewStatement,
                       viewAuthz=authzRules)

    def dropView(self, viewName):
        """
        Drops a view from the schema.
        
        :arg viewName: name of view to drop
        :type viewName: `string`
        """
        self._callServiceMethod("dropView",
                       vdbName=self.vdbName,
                       canForward="true",
                       viewName=viewName)

    def getAllTables(self):
        """
        Gets a list of all tables and views in the schema.
        
        :return: list of `string` with names of all tables and views in the schema
        """
        tupleSet = self._callServiceMethod("getAllTables", vdbName=self.vdbName, canForward="true")
        tables = []
        for tuple in tupleSet.getData():
            tables.append(tuple.getString(0))
        return tables

    def getTableDefinition(self, tableName):
        """
        Gets the definition for the named table or view.
        
        :arg viewName: name of view to drop
        :type viewName: `string`
        :return: :class:`rgma.TableDefinition`
        """
        tableNameO, columnNameO, typeO, sizeO, notNullO, primaryKeyO, viewForO = range(7) 
        tuples = self._callServiceMethod("getTableDefinition",
                        tableName=tableName, vdbName=self.vdbName, canForward="true").getData()
                        
        viewFor = tuples[0].getString(viewForO)
        tableName = tuples[0].getString(tableNameO)
        columns = []
        for column in tuples:
            type = RGMAType._getType(column.getString(typeO))
            notNull = column.getBool(notNullO)
            primaryKey = column.getBool(primaryKeyO)
            width = column.getInt(sizeO)
            columns.append(ColumnDefinition(column.getString(columnNameO), type, width, notNull, primaryKey))           
        return TableDefinition(tableName, viewFor, columns)

    def getTableIndexes(self, tableName):
        """
        Gets the list of indexes for a table.
        
        :arg tableName: name of table
        :type tableName: `string`
        :return: :class:`rgma.Index`
        """
        tuples = self._callServiceMethod("getTableIndexes",
                        tableName=tableName, vdbName=self.vdbName, canForward="true").getData()
        indexColumns = {}
        for index in tuples:
            indexName = index.getString(0)
            if not indexColumns.has_key(indexName):
                indexColumns[indexName] = []
            indexColumns[indexName].append(index.getString(1))
        indexes = []
        for indexName in indexColumns:
            indexes.append(Index(indexName, indexColumns[indexName]))
        return indexes
    
    def setAuthorizationRules(self, tableName, authzRules):
        """
        Sets the authorization rules for the given table or view.
        
        :arg tableName: name of table
        :type tableName: `string`
        :arg authzRules: authorization rules or list of authorization rules 
        :type authzRules: `string` or [ `string` ]
        """
        self._callServiceMethod("setAuthorizationRules",
                       tableName=tableName,
                       tableAuthz=authzRules,
                       vdbName=self.vdbName,
                       canForward="true")

    def getAuthorizationRules(self, tableName):
        """
        Gets the authorization rules for a table or view.
        
        :arg tableName: name of table
        :type tableName: `string`
        :return: [ `string` ]
        """
        tuples = self._callServiceMethod("getAuthorizationRules",
                        tableName=tableName, vdbName=self.vdbName, canForward="true")
        rules = []
        for rule in tuples.getData():
            rules.append(rule.getString(0))
        return rules
   
class SecondaryProducer(Producer):
    """
    Republish information from other producers.
    """
    
    def __init__(self, storage, supportedQueries):
        """
        :arg storage: a storage object to define the type and, if permanent, the name of the storage to be used
        :type storage: :class:`rgma.Storage`
        :arg supportedQueries: a supported queries object to define what query types the producer will be able to support.
        :type supportedQueries: :class:`rgma.SupportedQueries` 
        """
        
        _checkObjectType("storage", storage, Storage)
        _checkObjectType("supportedQueries", supportedQueries, SupportedQueries)
        
        Producer.__init__(self)
        self.setServiceName("SecondaryProducerServlet")
          
        tupleSet = self._callServiceMethod("createSecondaryProducer",
                      type=storage.storageType,
                      logicalName=storage.logicalName,
                      isLatest=["false", "true"][supportedQueries._isLatest()],
                      isHistory=["false", "true"][supportedQueries._isHistory()])
        
        self._setResourceId(tupleSet.getData()[0].getInt(0));
        self.storage = storage
        self.supportedQueries = supportedQueries
        self.tables = []
        
    class _Table(object):
        def __init__(self, name, predicate, historyRetentionPeriod):
            self.name = name
            self.predicate = predicate
            self.historyRetentionPeriod = historyRetentionPeriod
        
    def declareTable(self, name, predicate, historyRetentionPeriod):
        """
        :arg name: the name of the table to declare
        :type name: `string`
        :arg predicate: an SQL WHERE clause defining the subset of a table that this producer will publish. To publish to the
                whole table, an empty predicate can be used.
        :type predicate: `string`
        :arg historyRetentionPeriod:
                the history retention period for this table
        :type historyRetentionPeriod: :class:`rgma.TimeInterval`
        :arg latestRetentionPeriod:
            the default latest retention period for tuples inserted into this table. It can be overridden when
            inserting tuples.
        :type latestRetentionPeriod: :class:`rgma.TimeInterval`
        """
        _checkObjectType("historyRetentionPeriod", historyRetentionPeriod, TimeInterval)
        try:
            self._doDeclareTable(name, predicate, historyRetentionPeriod);
        except _UnknownResourceException:
            try:
                self._restore();
                self._doDeclareTable(name, predicate, historyRetentionPeriod);
            except _UnknownResourceException, e:
                raise RGMATemporaryException(e.getMessage())
        self.tables.append(SecondaryProducer._Table(name, predicate, historyRetentionPeriod));
        
    def _doDeclareTable(self, name, predicate, historyRetentionPeriod):
        self.checkOK(self._callResourceMethod("declareTable", tableName=name,
                                              predicate=predicate,
                                              hrpSec=int(historyRetentionPeriod.getValueAs())))
                        
    def _restore(self):
        s = SecondaryProducer(self.storage, self.supportedQueries)
        for t in self.tables:
            s._doDeclareTable(t.name, t.predicate, t.historyRetentionPeriod)
        self._setResourceId(s.resourceId)
        
    def showSignOfLife(self):
        """
        Contacts the secondary producer resource to check that it is still alive and prevent it from being timed out.
        """
        try:
            self.checkOK(self._callResourceMethod("showSignOfLife"))
        except _UnknownResourceException:
            try:
                # No need to send a showSignOfLife - just restore
                self._restore()
            except _UnknownResourceException, e:
                raise RGMATemporaryException(e.getMessage())

    def getResourceId(self):
        """
        Return the resouceId to be used as the argument to subsequent static showSignOfLife calls.
     
        :return: the resourceId as an int
        """
        return self.resourceId

    @staticmethod
    def staticShowSignOfLife(resourceId):
        """
        Contacts the secondary producer resource to check that it is still alive and prevent it from being timed out.
     
        :arg resourceId: the identifier of the resource to be checked
        :type resourceId: `int`
        :return: bool ``True`` if resource is still alive otherwise ``False``
        """
        service = _Service()
        service.setServiceName("SecondaryProducerServlet")
        try:
            service.checkOK(service._callServiceMethod("showSignOfLife", connectionId=resourceId))
            return True
        except _UnknownResourceException:
            return False
        
    @staticmethod    
    def staticClose(resourceId):
        """
        Contacts the secondary producer resource to close it.
     
        :arg resourceId: the identifier of the resource to be checked
        :type resourceId: `int`
        """
        service = _Service()
        service.setServiceName("SecondaryProducerServlet")
        service.checkOK(service._callServiceMethod("close", connectionId=resourceId))
        

class _State(object):
    
    def __init__(self, maxConnections):
        self.connectionPool = []
        self.connections = {}
        self.maxConnections = maxConnections
        self.semaphore = threading.Semaphore()
        
    def getConnection(self):
        self.semaphore.acquire()
        if len(self.connectionPool) == 0:
            if len(self.connections) >= self.maxConnections:
                raise RGMATemporaryException("Too many http connections are open")
            conn = httplib.HTTPSConnection(self.host, key_file=self.key, cert_file=self.cert)
        else:
            conn = self.connectionPool.pop()
        self.connections[conn] = None
        self.semaphore.release()
        return conn
        
    def releaseConnection(self, conn):
        conn.close()
        self.semaphore.acquire()
        del self.connections[conn]
        self.connectionPool.append(conn)
        self.semaphore.release()
        
    def refreshConnection(self, conn):
        conn.close()
        self.semaphore.acquire()
        del self.connections[conn]
        conn = httplib.HTTPSConnection(self.host, key_file=self.key, cert_file=self.cert)
        self.connections[conn] = None
        self.semaphore.release()
        return conn
                
class Storage(object):
    """
    Storage location for tuples.
    """
    
    def __init__(self, storageType, logicalName=None):
        """
        Private constructor
        """
        if storageType not in ("database", "memory"):
            raise RGMAPermanentException("Internal error - invalid storage type")
        
        if logicalName:
            self.logicalName = str(logicalName)
        else:
            self.logicalName = None
        self.storageType = storageType
    
    @staticmethod
    def getDatabaseStorage(logicalName=None):
        """
        Gets a database storage object. If no logical name is given the storage cannot be reused.
        
        :return: a database :class:`rgma.Storage` object
        """
        if logicalName:
            return Storage("database", logicalName)
        else:
            return _databaseStorage

    @staticmethod
    def getMemoryStorage():
        """
        Gets a temporary memory storage object. This storage cannot be reused.
        
        :return: a temporary memory :class:`rgma.Storage` object
        """
        return _memoryStorage
    
_databaseStorage = Storage("database")
_memoryStorage = Storage("memory")
     
class SupportedQueries(object):
    """
    Types of query supported by a primary or secondary producer
    """
    
    def __init__(self, type):
        """
        Private constructor
        """
        self.type = type
        
    def _isContinuous(self):
        return ("C" in self.type)
    
    def _isHistory(self):
        return ("H" in self.type)
 
    def _isLatest(self):
        return ("L" in self.type)
    
    def __str__(self):
        return self.type
       
SupportedQueries.C = SupportedQueries("C")
SupportedQueries.CH = SupportedQueries("CH")
SupportedQueries.CL = SupportedQueries("CL")
SupportedQueries.CHL = SupportedQueries("CHL")
            
class TimeInterval(object):
    """
    Encapsulates a time value and the units being used.

    Time intervals may not be negative
    """

    def __init__(self, value, units=TimeUnit.SECONDS):
        """
        :arg value: length of the time interval
        :type value: `int`
        
        :arg units: the units of the measurement
        :type units: :class:`rgma.TimeUnit`
        """

        if value < 0:
            raise RGMAPermanentException("Time interval may not be negative");
        
        if value > self.maxJavaInt / units._getSecs():
            raise RGMAPermanentException("Interval is larger than " + _maxJavaInt + " seconds");
        
        self.value = int(value) * units._getSecs();
   
    def getValueAs(self, units=TimeUnit.SECONDS):
        """
        Returns the length of the time interval in the specified units.
        
        :arg units: the units of the measurement
        :type units: :class:`rgma.TimeUnit`
        :returns: an int with the length of the time interval in the specified units
        """
        return int(self.value / units._getSecs())

TimeInterval.maxJavaInt = int(2 ** 31 - 1)
       
class TupleStore(object):
    """
    A named database used by a Producer to store tuples.
    """
    def __init__(self, logicalName, isHistory, isLatest):
        """
        Private constructor
        """
            
        self.logicalName = logicalName
        self.history = isHistory
        self.latest = isLatest
        
    def getLogicalName(self):
        """
        Returns the logical name for this tuple store.
        
        :return: logical name as a string
        """
        return self.logicalName

    def isHistory(self):
        """
        Determines if this tuple store supports history queries.
        
        :return: ``True`` if this tuple store supports history queries
        """
        return self.history

    def isLatest(self):
        """
        Determines if this tuple store supports latest queries.
        
        :return: ``True`` if this tuple store supports latest queries
        """
        return self.latest
 
        
class Tuple(object):
    """
    Represents a tuple (or row of a table).
    """
    
    def __init__(self, data):
        """
        Private constructor. Note that the data is expected to be of type tuple - so immutable.
        """
        self.data = data;
    
    def getInt(self, columnOffset):
        """
        Returns the integer representation of the specified column.
        
        :arg columnOffset: offset of the column within the tuple
        :type columnOffset: `integer`
        :return: integer representation of the column. Nulls will be returned as 0.
        """
        s = self._get(columnOffset)
        if s == None: return 0
        try:
            return int(s)
        except ValueError, e:
            raise RGMAPermanentException("Value '" + s + "' does not represent an Integer")
            
    def getBool(self, columnOffset):
        """
        Returns the boolean representation of the specified column.
        
        :arg columnOffset: offset of the column within the tuple
        :type columnOffset: `integer`
        :return: ``True`` if the internal representation is equal, ignoring case, to the string "true". Nulls will be returned as ``False``.
        """
        s = self._get(columnOffset).upper()
        if s == None: return False
        return s == "TRUE"

 
    def  getFloat(self, columnOffset):
        """
        Returns the float representation of the specified column.
        
        :arg columnOffset: offset of the column within the tuple
        :type columnOffset: `integer`
        :return: Float representation of the column. Nulls will be returned as 0.
        """
        s = self._get(columnOffset)
        if s == None: return 0.
        try:
            return float(s)
        except ValueError, e:
            raise RGMAPermanentException("Value '" + s + "' does not represent a Float")
        
    def getString(self, columnOffset):
        """
         Returns the string representation of the specified column.
        
        :arg columnOffset: offset of the column within the tuple
        :type columnOffset: `integer`
        :return: string representation of the column. An empty string will be returned for nulls.
        """
        s = self._get(columnOffset)
        if s == None: return ""
        return s
        
    def isNull(self, columnOffset):
        """
        Returns the null status of the specified column.
        
        :arg columnOffset: offset of the column within the tuple
        :type columnOffset: `integer`
        :return: ``True`` if the column holds a null value otherwise ``False``.
        """
        return self._get(columnOffset) == None

    def _get(self, columnOffset):
        if columnOffset < 0 or columnOffset >= len(self.data):
            raise RGMAPermanentException("column offset must be between 0 and " + (len(self.data) - 1))
        return self.data[columnOffset]
    
    def getPyTuple(self):
        """
        Return the contents of the tuple as a Python tuple. Values are represented as 
        strings - except for NULLs which are returned as ``None`` 
        
        :return: A python tuple
        """
        return self.data

class TupleSet(object):
    """
    Results returned by a pop call on a consumer.
    """
    
    def __init__(self, data, endOfData, warning):
        self.data = data
        self.endOfData = endOfData
        self.warning = warning
        
    def getData(self):
        """
        Gets the list of tuples. It is possible to modify the list and even include tuples from different tables.

        :return: [ :class:`rgma.Tuple` ] The list may be empty.
        """
        return self.data
    
    def getWarning(self):
        """
        Gets the warning string associated with this tuple set.
        
        :return: the warning string associated with this tuple set. It will be an empty string if no warning has been set.
        """
        return self.warning;
    
    def isEndOfResults(self):
        """
        Reports whether this tuple set is the last one to be returned by a consumer.
        
        :return: ``True`` if there are no more tuple sets that need to be popped for this query
        """
        return self.endOfData
    
    def _appendWarning(warning):
        if len(self.warning) == 0:
            self.warning = warning
        else:
            self.warning = self.warning + " " + warning                   

def _getSecurityInfo():
    """
    Get the certificate and key files to be used for secure connections.

    Returns cert_file, key_file
    """
    if os.environ.has_key('X509_USER_PROXY'):
        proxy = os.environ['X509_USER_PROXY']
        if len(proxy) > 0:
            if not os.path.exists(proxy):
                raise RGMAPermanentException("X509_USER_PROXY set to file %s which does not exist" % proxy)
            if not os.access(proxy, os.R_OK):
                raise RGMAPermanentException("X509_USER_PROXY set to file %s which is not readable" % proxy)
            return (proxy, proxy)
        
    if os.environ.has_key('TRUSTFILE'):
        trustfile = os.environ['TRUSTFILE']
        if len(trustfile) > 0:
            try:
                props = _Properties(trustfile)
            except Exception, e:
                raise RGMAPermanentException("Couldn't open TRUSTFILE: " + trustfile + " " + `e`)
            cert_file = props.getProperty("sslCertFile")
            key_file = props.getProperty("sslKey")
            proxy_file = props.getProperty("gridProxyFile")
            if cert_file and key_file:
                if not os.path.exists(cert_file):
                    raise RGMAPermanentException("sslCertFile in TRUSTFILE (%s) set to file %s which does not exist" % \
                                    (trustfile, cert_file))
                if not os.path.exists(key_file):
                    raise RGMAPermanentException("sslKey in TRUSTFILE (%s) set to file %s which does not exist" % \
                                    (trustfile, key_file))
                return (cert_file, key_file)
            elif proxy_file:
                if not os.path.exists(proxy_file):
                    raise RGMAPermanentException("gridProxyFile in TRUSTFILE (%s) set to file %s which does not exist" % \
                                    (trustfile, proxy_file))
                return (proxy_file, proxy_file)
            else:
                raise RGMAPermanentException("Either sslCertFile and sslKey or gridProxyFile must be set in TRUSTFILE (%s)" % \
                                                    trustfile)                          

    raise RGMAPermanentException("Neither TRUSTFILE nor X509_USER_PROXY is set")

def _urlencode(params):
    paramsList = []
    for key, value in params.items():
        if type(value) in (types.ListType, types.TupleType):
            for item in value:
                paramsList.append("%s=%s" % (urllib.quote_plus(key), urllib.quote_plus(item)))
        else:
            paramsList.append("%s=%s" % (urllib.quote_plus(key), urllib.quote_plus(str(value))))
            
    return string.join(paramsList, "&")
                
def _checkObjectType(name, var, *args):
    if type(var) not in args:
        if len(args) == 1:
            raise RGMAPermanentException(name + " must be an instance of: " + args[0].__name__)
        else:
            raise RGMAPermanentException(name + " must be an instance of one of: " + `map(lambda x:x.__name__, args)`)

class Index(object):
    """
    Information about an index on a table.
    """

    def __init__(self, name, columns):
        """
        Private constructor
        """
        self.name = str(name)
        self.columns = columns
        
    def getColumnNames(self):
        """
        Returns the list of column names used by this index.
        
        :return: [ `string` ]
        """
        return self.columns;

    def getIndexName(self):
        """
        Returns the name of this index.
        
        :return: `string`
        """
        return self.name        

    def __str__(self):
        columns = string.join(self.columns, " ")
        return "%s [%s]" % (self.name, columns)
            
class TableDefinition(object):
    """
    Definitions of a table, including table name and column details.
    """
    
    def __init__(self, tableName, viewFor, columns):
        """
        Private constructor
        """
        self.tableName = tableName
        self.viewFor = viewFor
        self.columns = columns
        
    def getColumns(self):
        """
        Returns the column definitions for this table.
        
        :return: [ :class:`rgma.ColumnDefinition` ] column definitions for this table
        """
        return self.columns

    def getTableName(self):
        """
        Returns the name of this table.
        
        :return: a string with the name of this table
        """
        return self.tableName;
 
    def getViewFor(self):
        """
        Returns name of table for which this is a view or ``None``.
        """
        return self.viewFor;
  
    def isView(self):
        """
        Returns true if this is a view rather than a table.
        
        :return: ``True`` if this is a view rather than a table
        """
        return self.viewFor != None
   
    def __str__(self):
        if self.viewFor:
            str = self.name + " view for " + self.viewFor + " ("
        else:
            str = self.name + " ("
        comma = ''
        for column in self.columns:
            str += "%s%s" % (comma, column)
            comma = ', '
        return str + ")"
        
class ColumnDefinition(object):
    """
    Definition of a column which may be used inside a :class:`rgma.TableDefinition`.
    """
    
    def __init__(self, name, type, size, notNull, primaryKey):
        """
        Private constructor
        """
        self.name = name
        self.type = type
        self.size = size
        self.notNull = notNull
        self.primaryKey = primaryKey
    
    def getName(self):
        """
        Returns the name of the column.
        
        :return: a string with the name of the column
        """
        return self.name

    def getSize(self):
        """
        Returns the size of the column type.
        
        :return: the size of the column type (or 0 if none is specified)
        """
        return self.size;

    def getType(self):
        """
        Returns the type of the column.
        
        :return: :class:`rgma.RGMAType` as the type of the column
        """
        return self.type

    def isNotNull(self):
        """
        Returns the NOT NULL flag.
        
        :return: ``True`` if column is ``NOT NULL``
        """
        return self.notNull;

    def isPrimaryKey(self):
        """
        Returns the PRIMARY KEY flag.
        
        :return: ``True`` if column is ``PRIMARY KEY``
        """
        return self.primaryKey;
   
    def __str__(self):
        s = self.name + " " + str(self.type)
        if self.size > 0: s = s + "(" + `self.size` + ")"
        if self.primaryKey: s += " PRIMARY KEY"
        if self.notNull: s += " NOT NULL"
        return s
                    
def setup(maxConnections):
    
    global _state
    _state = _State(maxConnections)
    _state.exception = None
    
    try:
        try:
            rgmaHome = os.environ['RGMA_HOME']
        except KeyError:
            raise RGMAPermanentException("Environment variable RGMA_HOME is not set")
        filename = os.path.join(rgmaHome, "etc", "rgma", "rgma.conf")
        properties = _Properties(filename)
        _state.cert, _state.key = _getSecurityInfo()
    except RGMAException, e:
        _state.exception = e
        return
    
    hostname = properties.getProperty("hostname")
    if hostname == None:
        _state.exception = RGMAException("No entry for hostname in rgma.conf")
        return
    
    port = properties.getProperty("port")
    if port == None: 
        _state.exception = RGMAException("No entry for port in rgma.conf")
        return
    
    prefix = properties.getProperty("prefix")
    if not prefix: prefix = "R-GMA"
    
    _state.url = "https://" + hostname + ":" + port + "/" + prefix + "/"
    
    try:
        parsedUrl = urlparse.urlparse(_state.url)
    except Exception, e:
        _state.exception = RGMAPermanentException("Invalid service URL: %s" % self.url, 0, e)
        
    _state.host = parsedUrl[1]
    _state.path = parsedUrl[2]
    
setup(5)    

def main():    
    pp = PrimaryProducer(Storage.getMemoryStorage(), SupportedQueries.CH)
    not42 = "not42"
    predicate = "WHERE userId = '" + not42 + "'"
    historyRP = TimeInterval(10, TimeUnit.MINUTES)
    latestRP = TimeInterval(10, TimeUnit.MINUTES)
    pp.declareTable("default.userTable", predicate, historyRP, latestRP)
    insert = "INSERT INTO default.userTable (userId, aString, aReal, anInt) VALUES ('" + not42 + "', 'Java producer', 3.1415926, 42)"
    pp.insert(insert)
    pp.close()
    
    c = Consumer("SELECT * From DEFault.usERtABlE", QueryType.H)
    
    print RGMAService.getVersion()
    
    print RGMAService.getTerminationInterval().getValueAs(TimeUnit.MINUTES)
    
    RGMAService.dropTupleStore("fred")
    
    print RGMAService.listTupleStores()
    
    sp = SecondaryProducer(Storage.getDatabaseStorage(), SupportedQueries.CHL)
    sp.declareTable("userTable", "", TimeInterval(600))
    i = sp.getResourceId()
    print i
    sp.showSignOfLife()
    print SecondaryProducer.staticShowSignOfLife(i)
    print SecondaryProducer.staticShowSignOfLife(1)
    
    odp = OnDemandProducer("wibble", 12)
    odp.declareTable("default.userTable", "")
    
    r = Registry("DefaulT")
    for pte in r.getAllProducersForTable("userTable"): print pte
    
    s = Schema("dEFAULT")
    s.createTable("create table c (a int primary key)", ["::R", "::W"])
    for t in s.getAllTables(): print t
    print s.getTableDefinition("c");

    print "S U C C E S S"

if __name__ == "__main__":
    main()

#include "rgma/Consumer.h"
#include "rgma/Properties.h"
#include "rgma/ServletConnection.h"

#include <sstream>

namespace glite {
namespace rgma {

int Consumer::create() throw(RGMAPermanentException, RGMATemporaryException) {
    m_connection.addParameter("select", m_query);
    if (m_queryType == QueryType::C || m_queryTypeWithInterval == QueryTypeWithInterval::C) {
        m_connection.addParameter("queryType", std::string("continuous"));
    } else if (m_queryType == QueryType::H || m_queryTypeWithInterval == QueryTypeWithInterval::H) {
        m_connection.addParameter("queryType", std::string("history"));
    } else if (m_queryType == QueryType::L || m_queryTypeWithInterval == QueryTypeWithInterval::L) {
        m_connection.addParameter("queryType", std::string("latest"));
    } else if (m_queryType == QueryType::S) {
        m_connection.addParameter("queryType", std::string("static"));
    }
    if (!(m_queryInterval == TimeInterval::NONE)) {
        m_connection.addParameter("timeIntervalSec", m_queryInterval.getValueAs(TimeUnit::SECONDS));
    }
    if (!(m_timeout == TimeInterval::NONE)) {
        m_connection.addParameter("timeoutSec", m_timeout.getValueAs(TimeUnit::SECONDS));
    }
    std::vector<ResourceEndpoint>::const_iterator rit = m_endpoints.begin();
    while (rit != m_endpoints.end()) {
        std::ostringstream buff;
        buff << rit->getResourceId() << " " << rit->getUrlString();
        m_connection.addParameter("producerConnections", buff.str());
        ++rit;
    }
    TupleSet result;
    m_connection.connect("createConsumer", result);
    TupleSet::const_iterator it = result.begin();
    return it->getInt(0);
}

void Consumer::checkProducerList() const throw (RGMAPermanentException) {
    if (m_endpoints.size() == 0) {
        throw RGMAPermanentException("Consumer constructor with list of producer endpoints given an empty list");
    }
}

Consumer::Consumer(const std::string & query, const QueryType & queryType, const TimeInterval & timeout, std::vector<
        ResourceEndpoint> producers) throw (RGMAPermanentException, RGMATemporaryException) :
    Resource("Consumer"), m_query(query), m_queryType(queryType), m_timeout(timeout), m_queryTypeWithInterval(
            QueryTypeWithInterval::NONE), m_endpoints(producers), m_queryInterval(TimeInterval::NONE), m_eof(false) {
    checkProducerList();
    m_endPoint.setResourceId(create());
}

Consumer::Consumer(const std::string & query, const QueryType & queryType, const TimeInterval & timeout)
        throw (RGMAPermanentException, RGMATemporaryException) :
    Resource("Consumer"), m_query(query), m_queryType(queryType), m_timeout(timeout), m_queryTypeWithInterval(
            QueryTypeWithInterval::NONE), m_endpoints(), m_queryInterval(TimeInterval::NONE), m_eof(false) {
    m_endPoint.setResourceId(create());
}

Consumer::Consumer(const std::string & query, const QueryType & queryType, std::vector<ResourceEndpoint> producers)
        throw (RGMAPermanentException, RGMATemporaryException) :
    Resource("Consumer"), m_query(query), m_queryType(queryType), m_timeout(TimeInterval::NONE),
            m_queryTypeWithInterval(QueryTypeWithInterval::NONE), m_endpoints(producers), m_queryInterval(
                    TimeInterval::NONE), m_eof(false) {
    checkProducerList();
    m_endPoint.setResourceId(create());
}

Consumer::Consumer(const std::string & query, const QueryType & queryType) throw (RGMAPermanentException,
        RGMATemporaryException) :
    Resource("Consumer"), m_query(query), m_queryType(queryType), m_timeout(TimeInterval::NONE),
            m_queryTypeWithInterval(QueryTypeWithInterval::NONE), m_endpoints(), m_queryInterval(TimeInterval::NONE),
            m_eof(false) {
    m_endPoint.setResourceId(create());
}

Consumer::Consumer(const std::string & query, const QueryTypeWithInterval & queryType,
        const TimeInterval & queryInterval, const TimeInterval & timeout, std::vector<ResourceEndpoint> producers)
        throw (RGMAPermanentException, RGMATemporaryException) :
    Resource("Consumer"), m_query(query), m_queryType(QueryType::NONE), m_timeout(timeout), m_queryTypeWithInterval(
            queryType), m_endpoints(producers), m_queryInterval(queryInterval), m_eof(false) {
    checkProducerList();
    m_endPoint.setResourceId(create());
}

Consumer::Consumer(const std::string & query, const QueryTypeWithInterval & queryType,
        const TimeInterval & queryInterval, const TimeInterval & timeout) throw (RGMAPermanentException,
        RGMATemporaryException) :
    Resource("Consumer"), m_query(query), m_queryType(QueryType::NONE), m_timeout(timeout), m_queryTypeWithInterval(
            queryType), m_endpoints(), m_queryInterval(queryInterval), m_eof(false) {
    m_endPoint.setResourceId(create());
}

Consumer::Consumer(const std::string & query, const QueryTypeWithInterval & queryType,
        const TimeInterval & queryInterval, std::vector<ResourceEndpoint> producers) throw (RGMAPermanentException,
        RGMATemporaryException) :
    Resource("Consumer"), m_query(query), m_queryType(QueryType::NONE), m_timeout(TimeInterval::NONE),
            m_queryTypeWithInterval(queryType), m_endpoints(producers), m_queryInterval(queryInterval), m_eof(false) {
    checkProducerList();
    m_endPoint.setResourceId(create());
}

Consumer::Consumer(const std::string & query, const QueryTypeWithInterval & queryType,
        const TimeInterval & queryInterval) throw (RGMAPermanentException, RGMATemporaryException) :
    Resource("Consumer"), m_query(query), m_queryType(QueryType::NONE), m_timeout(TimeInterval::NONE),
            m_queryTypeWithInterval(queryType), m_endpoints(), m_queryInterval(queryInterval), m_eof(false) {
    m_endPoint.setResourceId(create());
}

Consumer::~Consumer() {
}

void Consumer::abort() throw(RGMATemporaryException, RGMAPermanentException) {
    try {
        doAbort();
    } catch (UnknownResourceException e) {
        try {
            restore();
            doAbort();
            return;
        } catch (UnknownResourceException e1) {
            throw RGMATemporaryException(e1.getMessage());
        }
    }
}

void Consumer::doAbort() throw(RGMAPermanentException, RGMATemporaryException, UnknownResourceException) {
    clearServletConnection();
    TupleSet result;
    m_connection.connect("abort", result);
    checkOK(result);
}

bool Consumer::hasAborted() throw(RGMATemporaryException, RGMAPermanentException) {
    try {
        return doHasAborted();
    } catch (UnknownResourceException e) {
        try {
            restore();
            return doHasAborted();
        } catch (UnknownResourceException e1) {
            throw RGMATemporaryException(e1.getMessage());
        }
    }
}

bool Consumer::doHasAborted() throw (RGMAPermanentException, RGMATemporaryException, UnknownResourceException) {
    clearServletConnection();
    TupleSet result;
    m_connection.connect("hasAborted", result);
    TupleSet::const_iterator it = result.begin();
    return it->getBool(0);
}

void Consumer::pop(int maxCount, TupleSet& results) throw(RGMATemporaryException, RGMAPermanentException) {
    try {
        doPop(maxCount, results);
    } catch (UnknownResourceException e) {
        try {
            restore();
            doPop(maxCount, results);
            results.appendWarning("The query was restarted - many duplicates may be returned.");
        } catch (UnknownResourceException e1) {
            throw RGMATemporaryException(e1.getMessage());
        }
    }
}

void Consumer::doPop(int maxCount, TupleSet& results) throw(RGMAPermanentException, RGMATemporaryException,
        UnknownResourceException) {
    clearServletConnection();
    m_connection.addParameter("maxCount", maxCount);
    m_connection.connect("pop", results);
    if (m_eof) {
        results.appendWarning("You have called pop again after end of results returned.");
    }
    m_eof = results.isEndOfResults();
}

void Consumer::restore() throw (RGMAPermanentException, RGMATemporaryException, UnknownResourceException) {
    Consumer c(m_query, m_queryType, m_timeout, m_queryTypeWithInterval, m_endpoints, m_queryInterval);
    m_endPoint.setResourceId(c.create());
}

Consumer::Consumer(const std::string & query, const QueryType & queryType, const TimeInterval & timeout,
        const QueryTypeWithInterval & queryTypeWithInterval, const std::vector<ResourceEndpoint> & endpoints,
        const TimeInterval & queryInterval) :
    Resource("Consumer"), m_query(query), m_queryType(queryType), m_timeout(timeout), m_queryTypeWithInterval(
            queryTypeWithInterval), m_endpoints(endpoints), m_queryInterval(queryInterval) {
}

}
}

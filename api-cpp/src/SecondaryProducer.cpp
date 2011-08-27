#include "rgma/SecondaryProducer.h"
#include "rgma/ResourceEndpoint.h"
#include "rgma/TimeInterval.h"
#include "rgma/URLBuilder.h"
#include "rgma/XMLConverter.h"
#include "rgma/ServletConnection.h"

#include <string>
#include <math.h>

namespace glite {
namespace rgma {
SecondaryProducer::SecondaryProducer(const Storage & storage, const SupportedQueries & supportedQueries)
        throw(RGMATemporaryException, RGMAPermanentException) :
    Producer("SecondaryProducer"), m_storage(storage), m_supportedQueries(supportedQueries) {
    std::string type = storage.isDatabase() ? "database" : "memory";
    m_connection.addParameter("type", type);
    if (storage.getLogicalName() != "") {
        m_connection.addParameter("logicalName", storage.getLogicalName());
    }
    m_connection.addParameter("isLatest", supportedQueries.isLatest());
    m_connection.addParameter("isHistory", supportedQueries.isHistory());
    TupleSet result;
    m_connection.connect("createSecondaryProducer", result);

    TupleSet::const_iterator it = result.begin();
    m_endPoint.setResourceId(it->getInt(0));
}

void SecondaryProducer::declareTable(const std::string & name, const std::string & predicate,
        const TimeInterval & historyRetentionPeriod) throw(RGMATemporaryException, RGMAPermanentException) {
    try {
        doDeclareTable(name, predicate, historyRetentionPeriod);
    } catch (UnknownResourceException e) {
        try {
            restore();
            doDeclareTable(name, predicate, historyRetentionPeriod);
        } catch (UnknownResourceException e1) {
            throw RGMATemporaryException(e1.getMessage());
        }
    }
    m_tables.push_back(Table(name, predicate, historyRetentionPeriod));
}

void SecondaryProducer::doDeclareTable(const std::string & tableName, const std::string & predicate,
        const TimeInterval & hrp) throw(RGMATemporaryException, RGMAPermanentException, UnknownResourceException) {
    clearServletConnection();
    m_connection.addParameter("tableName", tableName);
    m_connection.addParameter("predicate", predicate);
    m_connection.addParameter("hrpSec", hrp.getValueAs(TimeUnit::SECONDS));
    TupleSet result;
    m_connection.connect("declareTable", result);
    checkOK(result);
}

void SecondaryProducer::restore() throw (RGMAPermanentException, RGMATemporaryException, UnknownResourceException) {
    SecondaryProducer p(m_storage, m_supportedQueries);
    tables_iterator ti = m_tables.begin();
    while (ti != m_tables.end()) {
        p.doDeclareTable(ti->m_name, ti->m_predicate, ti->m_hrp);
        ti++;
    }
    m_endPoint.setResourceId(p.m_endPoint.getResourceId());
}

void SecondaryProducer::showSignOfLife() throw(RGMATemporaryException, RGMAPermanentException) {
    clearServletConnection();
    try {
        TupleSet result;
        m_connection.connect("showSignOfLife", result);
        checkOK(result);
    } catch (UnknownResourceException e) {
        try {
            // No need to send a showSignOfLife - just restore
            restore();
        } catch (UnknownResourceException e1) {
            throw RGMATemporaryException(e1.getMessage());
        }
    }
}

int SecondaryProducer::getResourceId() {
    return m_endPoint.getResourceId();
}

bool SecondaryProducer::showSignOfLife(int resourceId) throw (RGMAPermanentException, RGMATemporaryException) {
    ServletConnection connection(URLBuilder::getURL("SecondaryProducer"));
    connection.addParameter("connectionId", resourceId);
    try {
        TupleSet result;
        connection.connect("showSignOfLife", result);
        TupleSet::const_iterator it = result.begin();
        if ((it->getString(0)) != "OK") {
            throw RGMAPermanentException("Failed to return status of OK");
        }
        return true;
    } catch (UnknownResourceException e) {
        return false;
    }
}

SecondaryProducer::Table::Table(const std::string & name, const std::string & predicate, const TimeInterval & hrp) :
    m_name(name), m_predicate(predicate), m_hrp(hrp) {
}

}
}

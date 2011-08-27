#include  "rgma/PrimaryProducer.h"
#include "rgma/URLBuilder.h"

#include <math.h>
#include <iostream>

namespace glite {
namespace rgma {

PrimaryProducer::~PrimaryProducer() {
}

PrimaryProducer::PrimaryProducer(const Storage & storage, const SupportedQueries & supportedQueries)
        throw(RGMATemporaryException, RGMAPermanentException) :
    Producer("PrimaryProducer"), m_storage(storage), m_supportedQueries(supportedQueries) {
    std::string type = storage.isDatabase() ? "database" : "memory";
    m_connection.addParameter("type", type);
    if (storage.getLogicalName() != "") {
        m_connection.addParameter("logicalName", storage.getLogicalName());
    }
    m_connection.addParameter("isLatest", supportedQueries.isLatest());
    m_connection.addParameter("isHistory", supportedQueries.isHistory());
    TupleSet result;
    m_connection.connect("createPrimaryProducer", result);

    TupleSet::const_iterator it = result.begin();
    m_endPoint.setResourceId(it->getInt(0));
}

void PrimaryProducer::declareTable(const std::string & name, const std::string & predicate,
        const TimeInterval & historyRetentionPeriod, const TimeInterval & latestRetentionPeriod)
        throw(RGMATemporaryException, RGMAPermanentException) {
    try {
        doDeclareTable(name, predicate, historyRetentionPeriod, latestRetentionPeriod);
    } catch (const UnknownResourceException & e) {
        try {
            restore();
            doDeclareTable(name, predicate, historyRetentionPeriod, latestRetentionPeriod);
        } catch (const UnknownResourceException & e1) {
            throw RGMATemporaryException(e1.getMessage());
        }
    }
    m_tables.push_back(Table(name, predicate, historyRetentionPeriod, latestRetentionPeriod));
}

PrimaryProducer::Table::Table(const std::string & name, const std::string & predicate, const TimeInterval & hrp,
        const TimeInterval & lrp) :
    m_name(name), m_predicate(predicate), m_hrp(hrp), m_lrp(lrp) {
}

void PrimaryProducer::doDeclareTable(const std::string & tableName, const std::string & predicate,
        const TimeInterval & historyRetentionPeriod, const TimeInterval & latestRetentionPeriod)
        throw(RGMATemporaryException, RGMAPermanentException, UnknownResourceException) {
    clearServletConnection();
    m_connection.addParameter("tableName", tableName);
    m_connection.addParameter("predicate", predicate);
    m_connection.addParameter("hrpSec", historyRetentionPeriod.getValueAs(TimeUnit::SECONDS));
    m_connection.addParameter("lrpSec", latestRetentionPeriod.getValueAs(TimeUnit::SECONDS));
    TupleSet result;
    m_connection.connect("declareTable", result);
    checkOK(result);
}

void PrimaryProducer::restore() throw (RGMAPermanentException, RGMATemporaryException, UnknownResourceException) {
    PrimaryProducer p(m_storage, m_supportedQueries);
    tables_iterator ti = m_tables.begin();
    while (ti != m_tables.end()) {
        p.doDeclareTable(ti->m_name, ti->m_predicate, ti->m_hrp, ti->m_lrp);
        ti++;
    }
    m_endPoint.setResourceId(p.m_endPoint.getResourceId());
}

void PrimaryProducer::insert(const std::string & insertStatement) throw(RGMATemporaryException, RGMAPermanentException) {
    std::vector<std::string> inserts;
    inserts.push_back(insertStatement);
    insert(inserts);
}

void PrimaryProducer::insert(const std::vector<std::string> & insertStatements) throw(RGMATemporaryException,
        RGMAPermanentException) {
    try {
        doInsert(insertStatements);
    } catch (const UnknownResourceException & e) {
        try {
            restore();
            doInsert(insertStatements);
        } catch (const UnknownResourceException & e1) {
            throw RGMATemporaryException(e1.getMessage());
        }
    }
}

void PrimaryProducer::doInsert(const std::vector<std::string> & insertStatements) throw(RGMATemporaryException,
        RGMAPermanentException, UnknownResourceException) {
    clearServletConnection();
    m_connection.setRequestMethodPost();
    std::vector<std::string>::const_iterator it = insertStatements.begin();
    while (it != insertStatements.end()) {
        m_connection.addParameter("insert", *it);
        ++it;
    }
    TupleSet result;
    m_connection.connect("insert", result);
    checkOK(result);
}

void PrimaryProducer::insert(const std::string & insertStatement, const TimeInterval & lrp)
        throw(RGMATemporaryException, RGMAPermanentException) {
    std::vector<std::string> inserts;
    inserts.push_back(insertStatement);
    insert(inserts, lrp);
}

void PrimaryProducer::insert(const std::vector<std::string> & insertStatements, const TimeInterval & lrp)
        throw(RGMATemporaryException, RGMAPermanentException) {
    try {
        doInsert(insertStatements, lrp);
    } catch (const UnknownResourceException & e) {
        try {
            restore();
            doInsert(insertStatements, lrp);
        } catch (const UnknownResourceException & e1) {
            throw RGMATemporaryException(e1.getMessage());
        }
    }
}

void PrimaryProducer::doInsert(const std::vector<std::string> & insertStatements, const TimeInterval & lrp)
        throw(RGMATemporaryException, RGMAPermanentException, UnknownResourceException) {
    clearServletConnection();
    m_connection.setRequestMethodPost();
    std::vector<std::string>::const_iterator it = insertStatements.begin();
    while (it != insertStatements.end()) {
        m_connection.addParameter("insert", *it);
        ++it;
    }
    TupleSet result;
    m_connection.addParameter("lrpSec", lrp.getValueAs(TimeUnit::SECONDS));
    m_connection.connect("insert", result);
    checkOK(result);
}

}
}


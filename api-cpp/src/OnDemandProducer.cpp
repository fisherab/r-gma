#include  "rgma/OnDemandProducer.h"

#include "rgma/ServletConnection.h"
#include "rgma/XMLConverter.h"

namespace glite {
namespace rgma {

OnDemandProducer::~OnDemandProducer() {
}

OnDemandProducer::OnDemandProducer(const std::string & hostName, int port) throw(RGMATemporaryException,
        RGMAPermanentException) :
    Producer("OnDemandProducer"), m_hostName(hostName), m_port(port) {
    m_connection.addParameter("hostName", hostName);
    m_connection.addParameter("port", port);
    TupleSet result;
    m_connection.connect("createOnDemandProducer", result);
    TupleSet::const_iterator it = result.begin();
    m_endPoint.setResourceId(it->getInt(0));
}

void OnDemandProducer::declareTable(const std::string & name, const std::string & predicate)
        throw(RGMATemporaryException, RGMAPermanentException) {
    try {
        doDeclareTable(name, predicate);
    } catch (UnknownResourceException e) {
        try {
            restore();
            doDeclareTable(name, predicate);
        } catch (UnknownResourceException e1) {
            throw RGMATemporaryException(e1.getMessage());
        }
    }
    m_tables.push_back(Table(name, predicate));
}

void OnDemandProducer::doDeclareTable(const std::string & tableName, const std::string & predicate)
        throw(RGMATemporaryException, RGMAPermanentException, UnknownResourceException) {
    clearServletConnection();
    m_connection.addParameter("tableName", tableName);
    m_connection.addParameter("predicate", predicate);
    TupleSet result;
    m_connection.connect("declareTable", result);
    checkOK(result);
}

void OnDemandProducer::restore() throw (RGMAPermanentException, RGMATemporaryException, UnknownResourceException) {
    OnDemandProducer p(m_hostName, m_port);
    tables_iterator ti = m_tables.begin();
    while (ti != m_tables.end()) {
        p.doDeclareTable(ti->m_name, ti->m_predicate);
        ti++;
    }
    m_endPoint.setResourceId(p.m_endPoint.getResourceId());
}

OnDemandProducer::Table::Table(const std::string & name, const std::string & predicate) :
    m_name(name), m_predicate(predicate) {
}

}
}

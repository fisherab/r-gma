#include "rgma/Registry.h"
#include "rgma/TupleSet.h"
#include "rgma/URLBuilder.h"
#include "rgma/ResourceEndpoint.h"

namespace glite {
namespace rgma {

Registry::Registry(const std::string & vdbName) throw(RGMATemporaryException, RGMAPermanentException) :
    m_vdbName(vdbName), m_connection(URLBuilder::getURL("Registry")) {
}

void Registry::clearServletConnection() {
    m_connection.clear();
    m_connection.addParameter("vdbName", m_vdbName);
}

std::vector<ProducerTableEntry> Registry::getAllProducersForTable(const std::string & tableName)
        throw(RGMATemporaryException, RGMAPermanentException) {
    clearServletConnection();
    m_connection.addParameter("tableName", tableName);
    m_connection.addParameter("canForward", true);
    TupleSet result;
    m_connection.connect("getAllProducersForTable", result);
    TupleSet::const_iterator it(result.begin());
    std::vector<ProducerTableEntry> pdl;
    while (it != result.end()) {
        ResourceEndpoint endpoint(it->getString(0), it->getInt(1));
        ProducerTableEntry pd(endpoint, it->getBool(2), it->getBool(3), it->getBool(4), it->getBool(5), it->getBool(6),
                it->getString(7), it->getInt(8));
        pdl.push_back(pd);
        ++it;
    }
    return pdl;
}

}
}

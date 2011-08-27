#include "rgma/Resource.h"
#include "rgma/XMLConverter.h"

namespace glite {
namespace rgma {

Resource::~Resource() {
}

Resource::Resource(std::string const & servletName) :
    m_endPoint(servletName), m_connection(m_endPoint.getUrlString()), m_dead(false) {
}

void Resource::clearServletConnection() {
    if (m_dead) {
        throw RGMAPermanentException("This resource cannot be reused after you have closed or destroyed it.");
    }
    m_connection.clear();
    m_connection.addParameter("connectionId", m_endPoint.getResourceId());
}

void Resource::destroy() throw(RGMATemporaryException, RGMAPermanentException) {
    m_connection.clear();
    m_connection.addParameter("connectionId", m_endPoint.getResourceId());
    TupleSet result;
    m_connection.connect("destroy", result);
    checkOK(result);
    m_dead = true;
}

void Resource::close() throw(RGMATemporaryException, RGMAPermanentException) {
    m_connection.clear();
    m_connection.addParameter("connectionId", m_endPoint.getResourceId());
    TupleSet result;
    m_connection.connect("close", result);
    checkOK(result);
    m_dead = true;
}

void Resource::checkOK(TupleSet& ts) const throw (RGMAPermanentException) {
    TupleSet::const_iterator it = ts.begin();
    if ((it->getString(0)) != "OK") {
        throw RGMAPermanentException("Failed to return status of OK");
    }
}

}
}

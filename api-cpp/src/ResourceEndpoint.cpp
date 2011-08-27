/*
 *  Copyright (c) 2004 on behalf of the EU EGEE Project:
 *  The European Organization for Nuclear Research (CERN),
 *  Istituto Nazionale di Fisica Nucleare (INFN), Italy
 *  Datamat Spa, Italy
 *  Centre National de la Recherche Scientifique (CNRS), France
 *  CS Systeme d'Information (CSSI), France
 *  Royal Institute of Technology, Center for Parallel Computers (KTH-PDC), Sweden
 *  Universiteit van Amsterdam (UvA), Netherlands
 *  University of Helsinki (UH.HIP), Finland
 *  University of Bergen (UiB), Norway
 *  Council for the Central Laboratory of the Research Councils (CCLRC), United Kingdom
 */
#include "rgma/ResourceEndpoint.h"
#include "rgma/URLBuilder.h"
#include <iostream>

namespace glite {
namespace rgma {

ResourceEndpoint::ResourceEndpoint(std::string const & servletName) :
    m_url(URLBuilder::getURL(servletName)), m_resourceId(0) {
}

ResourceEndpoint::ResourceEndpoint(const std::string & urlString, int resourceId) :
    m_url(urlString), m_resourceId(resourceId) {
}

int ResourceEndpoint::getResourceId() const {
    return m_resourceId;
}

void ResourceEndpoint::setResourceId(int id) {
    m_resourceId = id;
}

const std::string & ResourceEndpoint::getUrlString() const {
    return m_url;
}

std::ostream& operator<<(std::ostream& stream, const ResourceEndpoint & rs) {
    stream << "ResourceEndpoint[" << rs.getUrlString() << ":" << rs.getResourceId() << "]";
    return stream;
}

}
}

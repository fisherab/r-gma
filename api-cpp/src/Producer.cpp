#include  "rgma/PrimaryProducer.h"
#include "rgma/URLBuilder.h"

#include <math.h>
#include <iostream>

namespace glite {
namespace rgma {

Producer::~Producer() {
}

Producer::Producer(std::string const & servletName) :
    Resource(servletName) {
}

ResourceEndpoint Producer::getResourceEndpoint() {
    return m_endPoint;
}

}
}


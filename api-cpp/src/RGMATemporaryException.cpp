#include "rgma/RGMATemporaryException.h"

namespace glite {
namespace rgma {

RGMATemporaryException::RGMATemporaryException(const std::string & message, int nsop) :
    RGMAException(message, nsop) {
}

RGMATemporaryException::~RGMATemporaryException() throw() {
}
}
}

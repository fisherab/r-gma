#include "rgma/RGMAPermanentException.h"

namespace glite {
namespace rgma {

RGMAPermanentException::RGMAPermanentException(const std::string & message, int nsop)
    : RGMAException(message, nsop) {
}

RGMAPermanentException::~RGMAPermanentException() throw() {
}
}
}

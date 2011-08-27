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
#include "rgma/RGMAException.h"

namespace glite {
namespace rgma {

RGMAException::RGMAException(std::string message, int nsop) :
    m_message(message), m_nsop(nsop) {
}

RGMAException::~RGMAException() throw() {
}

const std::string & RGMAException::getMessage() const {
    return m_message;
}

const char* RGMAException::what() const throw() {
    return getMessage().c_str();
}

int RGMAException::getNumSuccessfulOps() const {
    return m_nsop;
}

}
}

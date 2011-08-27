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
#include "rgma/Storage.h"
#include "rgma/RGMAPermanentException.h"

#include <string>
#include <sstream>
#include <iostream>

namespace glite {
namespace rgma {

const Storage Storage::MEMORY(Storage::M);
const Storage Storage::DATABASE(Storage::D);

Storage::Storage(const std::string & logicalName) :
    m_logicalName(logicalName), m_mord(D) {
    if (logicalName == "") {
        throw RGMAPermanentException("Logical name may not be empty string");
    }
}

Storage::Storage(MD mord) :
    m_mord(mord) {
}

bool Storage::isDatabase() const {
    return m_mord == Storage::D;
}

bool Storage::isMemory() const {
    return m_mord == Storage::M;
}

const std::string & Storage::getLogicalName() const {
    return m_logicalName;
}

}
}

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
#include "rgma/TimeUnit.h"

#include <sstream>
#include <string>

namespace glite {
namespace rgma {

const TimeUnit TimeUnit::SECONDS("SECONDS", 1);
const TimeUnit TimeUnit::MINUTES("MINUTES", 60);
const TimeUnit TimeUnit::HOURS("HOURS", 3600);
const TimeUnit TimeUnit::DAYS("DAYS", 86400);

TimeUnit::TimeUnit(std::string name, int ratio) :
    m_name(name), m_ratio(ratio) {
}

int TimeUnit::getSecs() const {
    return m_ratio;
}

std::ostream& operator<<(std::ostream& stream, const TimeUnit & tu) {
    return stream << tu.m_name;
}

}
}

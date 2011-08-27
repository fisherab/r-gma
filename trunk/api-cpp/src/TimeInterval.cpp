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
#include "rgma/TimeInterval.h"

#include <limits>

namespace glite {
namespace rgma {

const TimeInterval TimeInterval::NONE(0);

std::ostream& operator<<(std::ostream& stream, const TimeInterval & ti) {
    if (ti.m_value != -1) {
        return stream << ti.getValueAs(ti.m_units) << std::string(" ") << ti.m_units;
    } else {
        return stream << std::string("NONE");
    }
}

TimeInterval::TimeInterval(int value, const TimeUnit & units) throw(RGMAPermanentException) :
    m_units(units) {
    if (value < 0) {
        throw new RGMAPermanentException("Time interval may not be negative");
    }
    if (value > std::numeric_limits<int>::max() / units.getSecs()) {
        throw RGMAPermanentException("Interval is too large to express as an int in seconds");
    }
    m_value = value * units.getSecs();
}

TimeInterval::TimeInterval() :
    m_value(-1), m_units(TimeUnit::SECONDS) {
}

int TimeInterval::getValueAs(const TimeUnit & units) const throw(RGMAPermanentException) {
    return m_value / units.getSecs();
}

bool TimeInterval::operator==(const TimeInterval & ti) const {
    return ti.m_value == m_value;
}

}
}

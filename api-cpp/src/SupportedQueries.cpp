
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
#include "rgma/SupportedQueries.h"

namespace glite {
namespace rgma {
const SupportedQueries SupportedQueries::C("C");
const SupportedQueries SupportedQueries::CH("CH");
const SupportedQueries SupportedQueries::CHL("CHL");
const SupportedQueries SupportedQueries::CL("CL");

SupportedQueries::SupportedQueries(std::string query) :
    m_query(query) {
}

bool SupportedQueries::isLatest() const {
    return m_query.find('L') != std::string::npos;
}

bool SupportedQueries::isHistory() const {
    return m_query.find('H') != std::string::npos;
}

}
}

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
#include "rgma/QueryType.h"

namespace glite {
namespace rgma {
const QueryType QueryType::C("C");
const QueryType QueryType::L("L");
const QueryType QueryType::H("H");
const QueryType QueryType::S("S");
const QueryType QueryType::NONE("");

QueryType::QueryType(std::string query) :
    m_query(query) {
}

bool QueryType::operator==(const QueryType & qt) const {
    return qt.m_query == m_query;
}

}
}

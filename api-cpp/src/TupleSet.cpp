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
#include "rgma/TupleSet.h"
#include <ctype.h>
#include <stdlib.h>
#include <vector>
#include <sstream>
#include <iostream>

namespace glite {
namespace rgma {

TupleSet::TupleSet() :
    m_endOfResults(false), m_warning("") {
}

const std::string & TupleSet::getWarning() const {
    return m_warning;
}

TupleSet::const_iterator TupleSet::begin() const {
    return m_tuples.begin();
}

TupleSet::const_iterator TupleSet::end() const {
    return m_tuples.end();
}

int TupleSet::size() const {
    return m_tuples.size();
}

bool TupleSet::isEndOfResults() const {
    return m_endOfResults;
}

void TupleSet::setEndOfResults(bool endOfResults) {
    m_endOfResults = endOfResults;
}

void TupleSet::addRow(Tuple row) {
    m_tuples.push_back(row);
}
void TupleSet::setWarning(const std::string & warning) {
    m_warning = warning;
}

void TupleSet::appendWarning(const std::string & warning) {
    if (m_warning == "") {
        m_warning = warning;
    } else {
        m_warning = m_warning + " " + warning;
    }
}

}
}

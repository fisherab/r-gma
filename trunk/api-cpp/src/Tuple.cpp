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
#include "rgma/Tuple.h"

#include <algorithm>
#include <sstream>

namespace glite {
namespace rgma {

/**
 * Function to return value from a string. A check is made that there is nothing else after valid input apart from white space.
 */
template<class T>
bool from_string(T& t, const std::string& s) {
    std::istringstream iss(s);
    if ((iss >> t).fail()) {
        return false;
    }
    int eof;
    iss >> eof;
    return iss.fail() && iss.eof();
}

const std::string Tuple::s_emptyString("");

void Tuple::checkOffset(unsigned columnOffset) const throw(RGMAPermanentException) {
    if (columnOffset < 0 || columnOffset >= m_values.size()) {
        throw RGMAPermanentException("column offset must be between 0 and " + (m_values.size() - 1));
    }
}

double Tuple::getDouble(unsigned columnOffset) const throw(RGMAPermanentException) {
    checkOffset(columnOffset);
    if (m_isNulls[columnOffset]) {
        return 0;
    }
    double val;
    if (!from_string<double> (val, m_values[columnOffset].c_str())) {
        std::ostringstream o;
        o << columnOffset;
        throw RGMAPermanentException("Column " + o.str() + " (" + m_values[columnOffset]
                + ") is not representable as type 'double'");
    }
    return val;
}

float Tuple::getFloat(unsigned columnOffset) const throw(RGMAPermanentException) {
    checkOffset(columnOffset);
    if (m_isNulls[columnOffset]) {
        return 0;
    }
    float val;
    if (!from_string<float> (val, m_values[columnOffset].c_str())) {
        std::ostringstream o;
        o << columnOffset;
        throw RGMAPermanentException(std::string("Column ") + o.str() + " (" + m_values[columnOffset]
                + ") is not representable as type 'float'");
    }
    return val;
}

const std::string & Tuple::getString(unsigned columnOffset) const throw(RGMAPermanentException) {
    checkOffset(columnOffset);
    return m_isNulls[columnOffset] ? s_emptyString : m_values[columnOffset];
}

int Tuple::getInt(unsigned columnOffset) const throw(RGMAPermanentException) {
    checkOffset(columnOffset);
    if (m_isNulls[columnOffset]) {
        return 0;
    }
    int val;
    if (!from_string<int> (val, m_values[columnOffset].c_str())) {
        std::ostringstream o;
        o << columnOffset;
        throw RGMAPermanentException("Column " + o.str() + " (" + m_values[columnOffset]
                + ") is not representable as type 'int'");
    }
    return val;
}

bool Tuple::getBool(unsigned columnOffset) const throw(RGMAPermanentException) {
    checkOffset(columnOffset);
    if (m_isNulls[columnOffset]) {
        return false;
    }
    std::string uvalue(m_values[columnOffset]);
    std::transform(uvalue.begin(), uvalue.end(), uvalue.begin(), ::toupper);
    if (uvalue == "TRUE") {
        return true;
    } else if (uvalue == "FALSE") {
        return false;
    } else {
        std::ostringstream o;
        o << columnOffset;
        throw RGMAPermanentException("Column " + o.str() + " (" + m_values[columnOffset]
                + ") is not representable as type 'bool'");
    }
}

bool Tuple::isNull(unsigned columnOffset) const throw(RGMAPermanentException) {
    checkOffset(columnOffset);
    return m_isNulls[columnOffset];
}

void Tuple::addItem(const std::string & value, const bool isNull) {
    m_values.push_back(value);
    m_isNulls.push_back(isNull);
}

}
}

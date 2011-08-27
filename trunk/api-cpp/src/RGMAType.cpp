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
#include "rgma/RGMAType.h"

#include <sstream>

namespace glite {
namespace rgma {
const RGMAType RGMAType::CHAR("CHAR", 1);
const RGMAType RGMAType::DATE("DATE", 91);
const RGMAType RGMAType::DOUBLE("DOUBLE", 8);
const RGMAType RGMAType::INTEGER("INTEGER", 4);
const RGMAType RGMAType::REAL("REAL", 7);
const RGMAType RGMAType::TIME("TIME", 92);
const RGMAType RGMAType::TIMESTAMP("TIMESTAMP", 93);
const RGMAType RGMAType::VARCHAR("VARCHAR", 12);

/**
 * initialisation functions that construct a map
 * describing the mapping between sql types and c++ types
 */
std::map<int, const RGMAType *> RGMAType::initialiseIntMap() {
    std::map<int, const RGMAType *> names;
    names[1] = &RGMAType::CHAR;
    names[91] = &RGMAType::DATE;
    names[8] = &RGMAType::DOUBLE;
    names[4] = &RGMAType::INTEGER;
    names[7] = &RGMAType::REAL;
    names[92] = &RGMAType::TIME;
    names[93] = &RGMAType::TIMESTAMP;
    names[12] = &RGMAType::VARCHAR;
    return names;
}

std::map<std::string, const RGMAType *> RGMAType::initialiseStringMap() {
    std::map<std::string, const RGMAType *> names;
    names["CHAR"] = &RGMAType::CHAR;
    names["DATE"] = &RGMAType::DATE;
    names["DOUBLE"] = &RGMAType::DOUBLE;
    names["INTEGER"] = &RGMAType::INTEGER;
    names["REAL"] = &RGMAType::REAL;
    names["TIME"] = &RGMAType::TIME;
    names["TIMESTAMP"] = &RGMAType::TIMESTAMP;
    names["VARCHAR"] = &RGMAType::VARCHAR;
    return names;
}

RGMAType::RGMAType(const std::string & query, int value) :
    m_name(query), m_value(value) {
}

const RGMAType & RGMAType::getFromValue(int i) throw (RGMAPermanentException) {
    static std::map<int, const RGMAType *> fromInt(initialiseIntMap());
    std::map<int, const RGMAType *>::const_iterator it = fromInt.find(i);
    if (it == fromInt.end()) {
        std::ostringstream buff;
        buff << "Integer value " << i  << " does not correspond to an RGMAType";
        throw RGMAPermanentException(buff.str());
    }
    return *(it->second);
}

const RGMAType & RGMAType::getFromValue(std::string s) throw (RGMAPermanentException) {
    static std::map<std::string, const RGMAType *> fromString(initialiseStringMap());
    std::map<std::string, const RGMAType *>::const_iterator it = fromString.find(s);
    if (it == fromString.end()) {
        throw RGMAPermanentException("String value " + s + " does not correspond to an RGMAType");
    }
    return *(it->second);
}

std::ostream& operator<<(std::ostream& stream, const RGMAType & rs) {
    stream << rs.m_name;
    return stream;
}


}
}

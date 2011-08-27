/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef RGMA_XMLCONVERTER_H
#define RGMA_XMLCONVERTER_H

#include "rgma_parsexml.h"
#include "rgma/RGMATemporaryException.h"
#include "rgma/RGMAPermanentException.h"
#include "rgma/UnknownResourceException.h"
#include "rgma/TupleSet.h"
#include "rgma/Tuple.h"

#include <stack>
#include <string>

namespace glite {
namespace rgma {
/**
 * Parses XML and constructs ResultSet
 */
class XMLConverter {

    public:

        static void convertXMLResponse(const std::string& xml, TupleSet & resultSet) throw (RGMATemporaryException,
                RGMAPermanentException, UnknownResourceException);

    private:

        static void extract_exception(ELEMENT *, char) throw (RGMATemporaryException, RGMAPermanentException,
                UnknownResourceException);

        static void extract_TupleSet(TupleSet &, ELEMENT *) throw (RGMATemporaryException, RGMAPermanentException,
                UnknownResourceException);
};
}
}
#endif

/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef RGMATYPE_H
#define RGMATYPE_H

#include "rgma/RGMAPermanentException.h"

#include <string>
#include <map>

namespace glite {
namespace rgma {

/**
 * Constants for SQL column types.
 */
class RGMAType {

    public:
        /** SQL CHAR. */
        static const RGMAType CHAR;

        /** SQL DATE. */
        static const RGMAType DATE;

        /** SQL DOUBLE. */
        static const RGMAType DOUBLE;

        /** SQL INTEGER. */
        static const RGMAType INTEGER;

        /** SQL REAL. */
        static const RGMAType REAL;

        /** SQL TIME. */
        static const RGMAType TIME;

        /** SQL TIMESTAMP. */
        static const RGMAType TIMESTAMP;

        /** SQL VARCHAR. */
        static const RGMAType VARCHAR;

        /** The operator for stream output */
        friend std::ostream& operator<<(std::ostream& stream, const RGMAType & rs);

    private:

        friend class Schema;
        friend class XMLConverter;

        RGMAType(const std::string & t, int value);

        static const RGMAType & getFromValue(int i) throw (RGMAPermanentException);
        static const RGMAType & getFromValue(std::string s) throw (RGMAPermanentException);

        static std::map<int, const RGMAType *> initialiseIntMap();
        static std::map<std::string, const RGMAType *> initialiseStringMap();

        // Data

        std::string m_name;
        int m_value;

};


}
}
#endif


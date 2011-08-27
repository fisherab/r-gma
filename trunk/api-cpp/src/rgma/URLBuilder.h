/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */

// When making changes to this class, note that it is used by SSLContextProperties
// as well as APIBase

#ifndef RGMA_URLBUILDER_H
#define RGMA_URLBUILDER_H

#include "rgma/RGMAPermanentException.h"

namespace glite {
namespace rgma {
/**
 *
 * R-GMA properties
 */
class URLBuilder {
    public:
        /**
         * Gets a property with the specified key. This method
         * throws an exception if the property is not found.
         * @param key the hashtable key
         * @return property value
         */
        static std::string getURL(const std::string & key) throw (RGMAPermanentException);

    private:
        static std::string initialiseURLBuilder() throw (RGMAPermanentException);
};
}
}
#endif

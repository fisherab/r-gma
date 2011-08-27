/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef UNKNOWNRESOURCEEXCEPTION_H
#define UNKNOWNRESOURCEEXCEPTION_H

#include "rgma/RGMAException.h"

#include <string>

namespace glite {
namespace rgma {

class UnknownResourceException: public RGMAException {

    public:
        /**
         * @param message which describes the error
         * @param nsop number of succesful operations
         */
        UnknownResourceException(std::string message, int nsop = 0);

        /**
         * Destruct the exception
         */
        virtual ~UnknownResourceException() throw();
};
}
}
#endif

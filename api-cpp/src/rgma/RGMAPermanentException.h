/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef RGMA_BUSYEXCEPTION_H
#define RGMA_BUSYEXCEPTION_H

#include "rgma/RGMAException.h"

namespace glite {
namespace rgma {

/**
 * Exception thrown when it is unlikely that repeating the call will be successful.
 */
class RGMAPermanentException: public RGMAException {

    public:

        /**
         * Creates a new RGMAPermanentException object.
         *
         * @param message which describes the error
         * @param nsop number of succesful operations
         */
        RGMAPermanentException(const std::string & message, int nsop = 0);

        virtual ~RGMAPermanentException() throw();
};
}
}
#endif

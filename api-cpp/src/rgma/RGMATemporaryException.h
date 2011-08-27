/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef RGMA_TEMPORARYEXCEPTION_H
#define RGMA_TEMPORARYEXCEPTION_H

#include "rgma/RGMAException.h"

#include <string>

namespace glite {
namespace rgma {

/**
 * Exception thrown when an R-GMA call fails, but calling the method again after a delay may be successful.
 */
class RGMATemporaryException: public RGMAException {

    public:
        /**
         * Creates a new RGMATemporaryException object.
         *
         * @param message which describes the error
         * @param nsop number of succesful operations
         */
        RGMATemporaryException(const std::string & message, int nsop = 0);

        virtual ~RGMATemporaryException() throw();

};
}
}
#endif

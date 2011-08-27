/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef RGMA_EXCEPTION_H
#define RGMA_EXCEPTION_H

#include <exception>
#include <string>

namespace glite {
namespace rgma {

/**
 * Exception thrown when an error occurs in the RGMA application.
 */
class RGMAException: public std::exception {

    public:
        /**
         * Getter for message
         * @return the message
         */
        const std::string & getMessage() const;

        /**
         * Return descriptive message
         */
        const char* what() const throw();

        /**
         * Returns the number of successful operations.
         *
         * @return the number of successful operations
         */
        int getNumSuccessfulOps() const;

        virtual ~RGMAException() throw();

    private:

        friend class RGMATemporaryException;
        friend class RGMAPermanentException;
        friend class UnknownResourceException;
        RGMAException(std::string message, int nsop);
        std::string m_message;
        int m_nsop;

};
}
}
#endif

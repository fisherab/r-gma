/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef INFO_SERVLETCONNECTION_H
#define INFO_SERVLETCONNECTION_H
#define OPENSSL_NO_KRB5

#include "rgma/RGMATemporaryException.h"
#include "rgma/RGMAPermanentException.h"
#include "rgma/UnknownResourceException.h"
#include "rgma/SSLContext.h"
#include "rgma/SSLSocket.h"
#include "rgma/TupleSet.h"

#include <openssl/ssl.h>
#include <string>

namespace glite {
namespace rgma {

/**
 * Servlet interaction
 */
class ServletConnection {

    public:

        explicit ServletConnection(const std::string & servletURL) throw(RGMAPermanentException);

        void clear();

        void setRequestMethodPost();

        virtual ~ServletConnection();

        /**
         * add a string parameter (name value pair).
         * @param name name of parameter
         * @param value value of parameter
         */
        void addParameter(const std::string & name, const std::string & value) throw (RGMATemporaryException,
                RGMAPermanentException);

        /**
         * add an integer parameter (name value pair).
         * @param name name of parameter
         * @param value value of parameter
         */
        void addParameter(const std::string & name, int value) throw (RGMATemporaryException, RGMAPermanentException);

        /**
         * add a boolean parameter (name value pair).
         * @param name name of parameter @param value value of parameter
         */
        void addParameter(const std::string & name, bool value) throw (RGMATemporaryException, RGMAPermanentException);

        /**
         * Connect with specified operation.
         */
        void connect(const std::string & operation, TupleSet& result) throw (RGMATemporaryException,
                RGMAPermanentException, UnknownResourceException);

        std::string toString() const;

    private:
        /**
         * Make HTTP or HTTPS request and obtain response.
         * Used by connect() method.
         * @param url HTTP or HTTPS request
         * @param response reference to string to store the response in
         */
        void httpRequest(const std::string & url, std::string& response) throw (RGMATemporaryException,
                RGMAPermanentException);

        /*
         * Return the HTML document with the HTTP header stripped off, plus any chunking
         * removed.  It assumes that there is no HTTP trailer.
         *
         * @param document HTML document including HTTP header
         * @return HTML document with header stripped off
         */
        std::string stripHeader(const std::string & document);

        void addCheckedParameter(const std::string & name, const std::string & value);

        // Data
        std::string m_parameters;
        std::string m_servletURL;
        SSLSocket* m_sock;
        std::string m_submitMethod;

};

}
}
#endif

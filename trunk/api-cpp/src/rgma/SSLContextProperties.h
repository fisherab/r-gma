/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef INFO_SSLCONTEXTPROPERTIES_H
#define INFO_SSLCONTEXTPROPERTIES_H

#include "rgma/RGMAPermanentException.h"
#include <string>

namespace glite {
namespace rgma {

/**
 * Simple class for encapuslating the handling of the properties needed
 * to establish the SSL context.  In general these hold files that contain
 * keys and certificates.
 */

class SSLContextProperties {
        //static const APILogging& cat;
        std::string theGridProxyFile;
        std::string theSslCertFile;
        std::string theSslKey;
        std::string theSslCAFiles;
        std::string theSslKeyPassword;
        bool theSslKeyPasswordPresent;
        bool useCertificateAndKeyFlag;

    public:

        SSLContextProperties() throw (RGMAPermanentException);
        virtual ~SSLContextProperties();

        bool useCertificateAndKey() const;
        std::string gridProxyFile() const;
        std::string sslCAFiles() const;
        std::string sslCertFile() const;
        std::string sslKey() const;
        bool sslKeyPasswordPresent(std::string& value) const;
};
}
}
#endif

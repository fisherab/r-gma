/* Copyright (c) 2003 EU DataGrid. */
/* For license conditions see http://www.eu-datagrid.org/license.html */

//#include "rgma/APILogging.hh"
#include "rgma/Properties.h"
#include "rgma/SSLContextProperties.h"
#include <stdlib.h>

namespace glite {
namespace rgma {

/**
 * This class handles the SSL context properties.  These are read from
 * a properties file specified by the environment variable TRUSTFILE.
 *
 * If the environment variable/file doesn't exist then the grid proxy file
 * is used, and its definition is taken from the environment variable
 * X509_USER_PROXY.
 *
 * It contains properties in the usual format:
 * property=value
 *
 * The properties recognised are:
 * gridProxyFile - where the grid proxy file is located.  If TRUSTFILE
 * absent, then environment variable X509_USER_PROXY replaces the property.
 * sslCAFiles - where the CA certificates are located.  Default is
 *   /etc/grid-security
 * sslCertFile - The SSL certificate file name.
 * sslKey - The SSL key file name.
 * sslKeyPassword - The passphrase for the SSL key if it is password
 * protected.
 */

SSLContextProperties::SSLContextProperties() throw (RGMAPermanentException) {
    // Set up some defaults
    theSslCAFiles = "/etc/grid-security/certificates";
    useCertificateAndKeyFlag = false;
    theSslKeyPasswordPresent = false;

    const char* gridProxyFile = ::getenv("X509_USER_PROXY");
    const char* trustfile = ::getenv("TRUSTFILE");
    if (gridProxyFile) {
        const char* caDir = ::getenv("X509_CERT_DIR");
        theGridProxyFile = static_cast<std::string> (gridProxyFile);
        //cat.debug("Grid proxy file is " + theGridProxyFile);
        if (caDir) {
            theSslCAFiles = static_cast<std::string> (caDir);
            //cat.debug("CA certificate dir is " + theSslCAFiles);
        }
    } else if (trustfile) {
        try {
            Properties properties(trustfile);

            theSslCAFiles = properties.getProperty("sslCAFiles", theSslCAFiles);

            // hack to make compatible with the incorrect java
            //strip off the *. from the theSslCAFiles if its present
            size_t index = theSslCAFiles.find("*.");
            //cat.debug(std::string("sslCAFiles is ") + theSslCAFiles);

            if (index != std::string::npos) {
                //cat.debug(std::string("Stripping off *.  "));
                // found *. now strip it off
                theSslCAFiles = theSslCAFiles.substr(0, index);
                //cat.debug(std::string("sslCAFiles is now ") + theSslCAFiles);
            }

            // certificate and private key overrides gridProxyFile
            if (properties.present("sslCertFile") && properties.present("sslKey")) {
                theSslCertFile = properties.getProperty("sslCertFile");
                theSslKey = properties.getProperty("sslKey");
                useCertificateAndKeyFlag = true;

                // hack to make compatible with the incorrect java
                // which looks for sslKeyPasswd instaead of sslKeyPassword

                if (properties.present("sslKeyPasswd")) {
                    theSslKeyPassword = properties.getProperty("sslKeyPasswd");
                    theSslKeyPasswordPresent = true;
                } else if (properties.present("sslKeyPassword")) {
                    theSslKeyPassword = properties.getProperty("sslKeyPassword");
                    theSslKeyPasswordPresent = true;
                } else {
                    theSslKeyPasswordPresent = false;
                }
            } else if (properties.present("gridProxyFile")) {
                theGridProxyFile = properties.getProperty("gridProxyFile");
            } else {
                throw RGMAPermanentException("Neither gridProxyFile nor sslCertFile/sslKey specified in "
                        + std::string(trustfile));
            }
        } catch (...) {
            //cat.debug("Unable to read client authentication properties file " +
            // static_cast<std::string>(trustfile));
            throw RGMAPermanentException("Unable to read TRUSTFILE: " + std::string(trustfile));
        }
    } else {
        throw RGMAPermanentException("Neither TRUSTFILE nor X509_USER_PROXY environment variable set.");
    }
}

SSLContextProperties::~SSLContextProperties() {
}

std::string SSLContextProperties::gridProxyFile() const {
    //cat.debug(std::string("Grid proxy file is ") + theGridProxyFile);
    return theGridProxyFile;
}

std::string SSLContextProperties::sslCAFiles() const {
    //cat.debug(std::string("CA certificates files are in ") + theSslCAFiles);
    return theSslCAFiles;
}

std::string SSLContextProperties::sslCertFile() const {
    //cat.debug(std::string("SSL Certificate file is ") + theSslCertFile);
    return theSslCertFile;
}

std::string SSLContextProperties::sslKey() const {
    //cat.debug(std::string("SSL key is ") + theSslKey);
    return theSslKey;
}

bool SSLContextProperties::sslKeyPasswordPresent(std::string& value) const {
    if (theSslKeyPasswordPresent) {
        value = theSslKeyPassword;
    }
    // n.b. Don't display the password in debug messages!
    //cat.debug(std::string("Key password ") + (theSslKeyPasswordPresent ? "present" : "absent"));
    return theSslKeyPasswordPresent;
}

bool SSLContextProperties::useCertificateAndKey() const {
    //cat.debug(useCertificateAndKeyFlag ?
    // "use certificate and key" :
    // "use grid proxy file");
    return useCertificateAndKeyFlag;
}

//const APILogging& SSLContextProperties::cat = APILogging::getInstance("SSLContextProperties");
}
}

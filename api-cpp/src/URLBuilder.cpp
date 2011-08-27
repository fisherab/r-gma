/* Copyright (c) 2003 EU DataGrid. */
/* For license conditions see http://www.eu-datagrid.org/license.html */

// When making changes to this class, note that it is used by SSLContextProperties
// as well as APIBase

#include "rgma/URLBuilder.h"
#include "rgma/Properties.h"

#include <stdlib.h>

namespace glite {
namespace rgma {

 std::string URLBuilder::initialiseURLBuilder() throw (RGMAPermanentException) {
    const char* rgmaPropsDir = ::getenv("RGMA_HOME");
    if (!rgmaPropsDir) {
        rgmaPropsDir = ::getenv("GLITE_LOCATION");
    }

    if (!rgmaPropsDir) {
        throw RGMAPermanentException("Neither RGMA_HOME nor GLITE_LOCATION environment variables are set");
    }
    std::string fname = std::string(rgmaPropsDir) + "/etc/rgma/rgma.conf";
    Properties prop(fname);

    if (!prop.present("hostname")) {
        throw RGMAPermanentException("hostname not specified in rgma.conf");
    }
    std::string hostname = prop.getProperty("hostname");

    if (!prop.present("port")) {
        throw RGMAPermanentException("port not specified in rgma.conf");
    }
    std::string port = prop.getProperty("port");

    std::string prefix("R-GMA");
    if (prop.present("prefix")) {
        prefix = prop.getProperty("prefix");
    }
    return "https://" + hostname + ":" + port + "/" + prefix + "/";
}

std::string URLBuilder::getURL(const std::string & key) throw (RGMAPermanentException) {
    static std::string urlBase(initialiseURLBuilder());
    if (key == "RGMAService") {
        return urlBase + key;
    }
    return urlBase + key + "Servlet";
}

}
}

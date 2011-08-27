/* Copyright (c) 2003 EU DataGrid. */
/* For license conditions see http://www.eu-datagrid.org/license.html */

#include "rgma/SSLUtils.h"
#include <openssl/ssl.h>
#include <string.h>

namespace glite {
namespace rgma {

bool SSLUtils::isAProxyCertificate(X509* certificate) {
    bool returnValue = false;
    char *subject = X509_NAME_oneline(X509_get_subject_name(certificate), NULL, 0);
    if (subject) {
        // A certificate has the form
        //  ..../CN=blah/CN=proxy (or limited proxy, or restricted proxy)
        const char* cnString = strstr(subject, "/CN=");
        if (cnString) {
            cnString += strlen("/CN=");
            const char* proxyString = strstr(cnString, "/CN=");
            if (proxyString) {
                proxyString += strlen("/CN=");
                returnValue = (!strcmp(proxyString, "proxy") || !strcmp(proxyString, "limited proxy") || !strcmp(
                        proxyString, "restricted proxy")) ? true : false;
            }
        }
    }
    OPENSSL_free(subject);
    subject = 0;
    return returnValue;
}

}
}

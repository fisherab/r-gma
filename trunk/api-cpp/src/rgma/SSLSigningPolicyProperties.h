/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef INFO_SSLSIGNINGPOLICYPROPERTIES_H
#define INFO_SSLSIGNINGPPOLICYROPERTIES_H

#include "rgma/Properties.h"
#include <vector>

// OpenSSL types
typedef struct x509_st X509;

namespace glite {
namespace rgma {
/**
 * Class is used to represent the contents of a signing policy file.
 * It only handles the mimumum subset of features needed for RGMA.
 * In particular it only looks at the first instances of access_id_CA,
 * pos_rights and cond_subjects fields.  These are what it matches:
 *
 * access_id_CA .* '<issuer>'
 * pos_rights .* CA:sign
 * cond_subjects .* '<subject1>[ <subject2>...]'
 */
class SSLSigningPolicyProperties: public Properties {

    public:

        SSLSigningPolicyProperties(X509* certificate, const std::string& directory) throw (RGMAPermanentException);

        /**
         * Gets a list of properties with the specified key. This method
         * throws an exception if the property is not found.
         * @param key the hashtable key
         * @return List of property values
         */
        std::vector<std::string> getListProperty(std::string key);

    protected:

        /**
         * Loads properties from specified file.
         */
        void load(std::string filename) throw (RGMAPermanentException);

    private:
        /**
         * Convert occurances of several spaces or tabs to a single space and
         * remove trailing spaces, newlines.
         */
        void collapseSpaces(char* line) const;

        /**
         * Return pointer value portion of line, modifiying line to remove the
         * last single quote from the value if necessary (the quotes aren't part
         * of the value).
         */
        const char* getValueField(char* line) const;

        //static const class APILogging& cat;
};
}
}
#endif                                            // EDG_INFO_SSLSIGNINGPPOLICYROPERTIES_H

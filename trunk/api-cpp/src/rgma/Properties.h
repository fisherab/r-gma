/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */

// When making changes to this class, note that it is used by SSLContextProperties
// as well as APIBase

#ifndef RGMA_PROPERTIES_H
#define RGMA_PROPERTIES_H

#include "rgma/RGMAPermanentException.h"
#include <map>
#include <stdio.h>
#include <string>

namespace glite {
namespace rgma {
/**
 *
 * R-GMA properties
 *
 * */
class Properties {
    public:

        /**
         * Constructor.
         * @param name the name of the property file. If the parameter is omitted the properties are not loaded.
         * This can be used by a derived class to define its own properties.
         */
        Properties(std::string className = "") throw (RGMAPermanentException);

        /**
         * Destructor.
         */
        virtual ~Properties();

        /**
         * Gets a property with the specified key. This method
         * throws an exception if the property is not found.
         * @param key the hashtable key
         * @return property value
         */
        std::string getProperty(std::string key) throw (RGMAPermanentException);

        /**
         * Gets a property with the specified key. This method
         * returns the default value is the property is not found.
         * @param key the hashtable key
         * @param defaultValue default if property not present
         * @return property value
         */
        std::string getProperty(std::string key, std::string defaultValue);

        /**
         * Tests if the specified key is present in the table.
         * @param key the hashtable key
         * @return true if present
         */
        bool present(std::string key);

    protected:
        typedef std::map<std::string, std::string> Table;
        Table table;

        /**
         * Mimics the GNU getline function.
         * It reads an entire line, storing the address of the buffer
         * containing the text into *line.  The buffer is
         * null-terminated and includes the newline character,
         * if a newline delimiter was found.
         * @return the number of characters read
         */
        int getLine(char** line, int* length, FILE* inStream);

    private:
};
}
}
#endif

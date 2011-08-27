/* Copyright (c) 2003 EU DataGrid. */
/* For license conditions see http://www.eu-datagrid.org/license.html */

#include "rgma/SSLSigningPolicyProperties.h"
#include <ctype.h>
#include <openssl/x509v3.h>
#include <iostream>
#include <string.h>

namespace glite {
namespace rgma {

SSLSigningPolicyProperties::SSLSigningPolicyProperties(X509* certificate, const std::string& directory)
        throw (RGMAPermanentException) {
    char partfilename[9];
    snprintf(partfilename, sizeof(partfilename), "%08lx", X509_NAME_hash(X509_get_issuer_name(certificate)));
    std::string filename = directory + "/" + partfilename + ".signing_policy";
    load(filename);
}

void SSLSigningPolicyProperties::load(std::string filename) throw (RGMAPermanentException) {
    FILE* file = fopen(filename.c_str(), "r");
    if (file) {
        char* line = 0;
        int length = 0;
        while (getLine(&line, &length, file) > 0) {
            collapseSpaces(line);
            const std::string key[] = { "access_id_CA", "pos_rights", "cond_subjects" };
            for (unsigned int index = 0; index < sizeof(key) / sizeof(std::string); ++index) {
                std::string match = key[index] + " ";
                if (!strncmp(match.c_str(), line, match.size()) && (table.find(key[index]) == table.end())) {
                    table[key[index]] = std::string(getValueField(line));
                }
            }
        }
        delete[] line;
        line = 0;
        fclose(file);
    } else {
        throw RGMAPermanentException("Unable to open " + filename);
    }
}

std::vector<std::string> SSLSigningPolicyProperties::getListProperty(std::string key) {
    std::vector<std::string> returnList;
    Table::iterator it = table.find(key);
    if (it != table.end()) {
        std::vector<std::string> returnList;
        // Split property into a list. Delimitor is a space - outside of
        // a quoted string
        std::string value = (*it).second;
        std::string thisElement;
        bool insideQuote = false;
        for (unsigned int index = 0; index < value.size(); ++index) {
            if (value[index] == '"')
                insideQuote = !insideQuote;
            // Space outside of a quoted string is delimitor
            else if (!insideQuote && (value[index] == ' ')) {
                returnList.push_back(thisElement);
                thisElement = "";
            } else
                thisElement += value[index];
        }
        if (thisElement.size())
            returnList.push_back(thisElement);
        return returnList;
    } else
        throw RGMAPermanentException("Failed to find property" + key);
}

const char* SSLSigningPolicyProperties::getValueField(char* line) const {
    // Skip the first two words
    unsigned int charIndex = 0;
    bool inQuote = false;
    int wordsSkipped = 0;
    for (charIndex = 0; charIndex < strlen(line); ++charIndex) {
        if (line[charIndex] == '"')
            inQuote = !inQuote;
        else if (!inQuote && line[charIndex] == ' ') {
            if (++wordsSkipped >= 2) {
                // Advance index to start of value field
                ++charIndex;
                // Check for quotes
                int lineLength = strlen(line);
                if (lineLength && (line[charIndex] == '\'') && (line[lineLength - 1] == '\'')) {
                    line[lineLength - 1] = '\0';
                    // Skip first quote
                    ++charIndex;
                }
                return line + charIndex;
            }
        }
    }
    // Less than three words in the file, so return pointer to
    // null terminator.
    return &line[charIndex];
}

void SSLSigningPolicyProperties::collapseSpaces(char* line) const {
    // Convert multiple spaces outside of double quoted strings to
    // single ones, and convert tabs to spaces
    unsigned int charIndex = 0;
    bool insideQuote = false;
    while (charIndex < strlen(line)) {
        // Safe to examine line[index+1] because it will be the terminator
        // when index = strlen(line) - 1
        bool incCharIndex = true;
        if (line[charIndex] == '"') {
            insideQuote = !insideQuote;
        } else if (!insideQuote && isspace(line[charIndex])) {
            if (isspace(line[charIndex + 1])) {
                strcpy(&line[charIndex], &line[charIndex + 1]);
                // Have copied the string back one character so next character
                // is now at charIndex rather than charIndex + 1
                incCharIndex = false;
            }
            line[charIndex] = ' ';
        }
        if (incCharIndex)
            ++charIndex;
    }

    // Remove leading space, if any. Multiple leading spaces will have been
    // collapsed down to one space
    if (line[0] == ' ')
        strcpy(line, &line[1]);

    // Remove trailing spaces.
    if (strlen(line)) {
        charIndex = strlen(line) - 1;
        while (line[charIndex] == ' ') {
            line[charIndex] = '\0';
            --charIndex;
        }
    }
}

}
}

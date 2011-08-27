/* Copyright (c) 2003 EU DataGrid. */
/* For license conditions see http://www.eu-datagrid.org/license.html */

// When making changes to this class, note that it is used by SSLContextProperties
// as well as APIBase

#include "rgma/Properties.h"
#include <string.h>

namespace glite {
namespace rgma {

Properties::Properties(std::string filename) throw (RGMAPermanentException) {
    if (filename != "") {
        FILE* file = fopen(filename.c_str(), "r");
        if (file) {
            char* line = 0;
            int length = 0;
            int charsLeft;
            while ((charsLeft = getLine(&line, &length, file)) > 0) {
                // false if processing key, true if processing value
                enum {
                    IN_KEY, IN_VALUE, IN_COMMENT, READ_PAIR
                } state = IN_KEY;
                const char* thisChar = line;

                std::string key;
                std::string value;

                while (charsLeft--) {
                    if (state != IN_COMMENT) {
                        switch (*thisChar) {
                        case ' ':
                        case '\t':
                        case '\n':
                        case '\r':
                            break;

                        case '#':
                        case '!':
                            // # or ! always marks a comment
                            state = IN_COMMENT;
                            break;

                        case '=':
                        case ':':
                            // End of key, start of value.  If in value already
                            // then it is real data
                            if (state == IN_KEY)
                                state = IN_VALUE;
                            else
                                value += *thisChar;
                            break;

                        default:
                            if (state == IN_KEY)
                                key += *thisChar;
                            else
                                value += *thisChar;
                            break;
                        }
                    }
                    ++thisChar;
                }

                if (state != IN_COMMENT && key.size()) {
                    //cat.debug("Key = " + key + ", value = " + value);
                    table[key] = value;
                }
            }
            delete[] line;
            line = 0;
        } else
            throw RGMAPermanentException("Unable to open properties file " + filename);
        fclose(file);
    }
}

Properties::~Properties() {
}

std::string Properties::getProperty(std::string key) throw (RGMAPermanentException) {
    Table::iterator it = table.find(key);
    if (it != table.end())
        return (*it).second;
    throw RGMAPermanentException("Property " + key + "not found in properties file");
}

std::string Properties::getProperty(std::string key, std::string defaultValue) {
    Table::iterator it = table.find(key);
    return (it != table.end()) ? (*it).second : defaultValue;
}

bool Properties::present(std::string key) {
    if (table.find(key) == table.end()) {
        //cat.debug(key + " absent");
        return false;
    } else {
        //cat.debug(key + " present");
        return true;
    }
}

int Properties::getLine(char** line, int* length, FILE* file) {
    const int lineSize = 255;
    // Sanity checks
    if (line && length && file) {
        if (ferror(file))
            return -1;

        if (!*line) {
            *line = new char[lineSize];
            if (!*line)
                return -1;
            *length = lineSize;
        }
        **line = '\0';

        bool backslash = false; // Set true when backslash encountered
        int index = 0;
        int character;
        while ((character = getc(file)) != EOF) {
            if (character == '\\' && !backslash)
                backslash = true;
            else {
                if (index + 1 >= *length) {
                    // Need more memory to hold line contents
                    int newLength = lineSize + *length;
                    char* newLine = new char[newLength];
                    if (!newLine)
                        return -1;
                    memcpy((void*) newLine, (void*) *line, index);
                    delete[] line;
                    *line = newLine;
                    *length = newLength;
                }
                // The only escaped character that is treated specially is newline
                // which means that the property continues on the next line.
                if (backslash) {
                    switch (character) {
                    case '\n':
                        continue;
                    default:
                        break;
                    }
                }
                (*line)[index++] = character;
                (*line)[index] = '\0';
                if (character == '\n')
                    return strlen(*line);
                backslash = false;
            }
        }
    }
    if (ferror(file))
        return -1;
    return strlen(*line);
}

}
}

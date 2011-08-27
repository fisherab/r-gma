// Copyright (c) 2001-2003 EU DataGrid.
// For license conditions see http://www.eu-datagrid.org/license.html

#include <iostream>
#include <string.h>
#include <stdlib.h>
#include <sstream>

#include "rgma/Properties.h"
#include "rgma/XMLConverter.h"

namespace glite {
namespace rgma {

/**
 * Function to return value from a string. A check is made that there is nothing else after valid input apart from white space.
 */
template<class T>
bool from_string(T& t, const std::string& s) {
    std::istringstream iss(s);
    if ((iss >> t).fail()) {
        return false;
    }
    int eof;
    iss >> eof;
    return iss.fail() && iss.eof();
}

void XMLConverter::convertXMLResponse(const std::string& xml, TupleSet& tupleSet) throw (RGMATemporaryException,
        RGMAPermanentException, UnknownResourceException) {

    /* Parse HTTP response. This should be an XMLResponse object. The
     function parsexml_parse is defined in rgma_parsexml.c. */
    char* xmlc = (char *) malloc(strlen(xml.c_str()) + 1);
    strcpy(xmlc, xml.c_str());
    ELEMENT* root = parsexml_parse(xmlc);
    if (root == NULL) {
        throw RGMAPermanentException("xml from servlet could not be parsed " + xml);
    }

    /* Extract either an RGMATupleSet or an RGMAException */
    if (getElementByName(root, "r")) {
        extract_TupleSet(tupleSet, root);
    } else if (getElementByName(root, "t")) {
        extract_exception(root, 't');
    } else if (getElementByName(root, "p")) {
        extract_exception(root, 'p');
    } else if (getElementByName(root, "u")) {
        extract_exception(root, 'u');
    } else {
        parsexml_free(root);
        throw RGMAPermanentException("xml from server represents neither tuple set nor exception");
    }
    parsexml_free(root);
}

void XMLConverter::extract_TupleSet(TupleSet & rs, ELEMENT *root) throw (RGMATemporaryException,
        RGMAPermanentException, UnknownResourceException) {

    /* Deal with the warning message (if any). */
    char* warning;
    if ((warning = getAttributeByName(root, "m"))) {
        rs.setWarning(warning);
    }

    char* value;
    value = getAttributeByName(root, "c");
    int numCols = value ? atoi(value) : 1;

    value = getAttributeByName(root, "r");
    int numTuples = value ? atoi(value) : 1;

    int i = 0;
    int j = 0;
    ELEMENT* ven;
    Tuple* tp = NULL;

    for (ven = root->children; ven != NULL; ven = ven->next) {
        if (ven->name[0] == 'e') {
            rs.setEndOfResults(true);
        } else {
            if (j == 0) {
                if (i == numTuples) {
                    parsexml_free(root); /* Must be called to tidy up before throwing */
                    throw RGMAPermanentException("More data returned in XML tuple set than specified in header");
                }
                tp = new Tuple();
            }

            if (ven->name[0] == 'v') {
                tp->addItem(ven->data ? ven->data : "");
            } else { /* n */
                tp->addItem("", true);
            }

            if (++j == numCols) {
                rs.addRow(*tp);
                delete tp;
                j = 0;
                i++;
            }
        }
    }
    if (j) {
        parsexml_free(root); /* Must be called to tidy up before throwing */
        delete tp;
        throw RGMAPermanentException("Incomplete XML tuple set");
    }
}

void XMLConverter::extract_exception(ELEMENT *root, char tpu) throw (RGMATemporaryException, RGMAPermanentException,
        UnknownResourceException) {

    if (tpu == 'u') {
        parsexml_free(root); /* Must be called to tidy up before throwing */
        throw UnknownResourceException("Unknown resource.", 0);
    } else {
        std::string message(getAttributeByName(root, "m"));
        char* nsoc = getAttributeByName(root, "o");
        int nso = nsoc ? atoi(nsoc) : 0;
        parsexml_free(root); /* Must be called to tidy up before throwing */
        if (tpu == 'p') {
            throw RGMAPermanentException(message, nso);
        } else {
            throw RGMATemporaryException(message, nso);
        }
    }
}

}
}

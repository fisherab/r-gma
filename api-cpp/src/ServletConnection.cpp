/* Copyright (c) 2003 EU DataGrid. */
/* For license conditions see http://www.eu-datagrid.org/license.html */

#include "rgma/ServletConnection.h"
#include "rgma/SSLSocket.h"
#include "rgma/XMLConverter.h"
#include <sstream>

namespace glite {
namespace rgma {

/**
 * Helper class to chop up urls
 */
class ParsedURL {
        std::string url;
        bool gotPort;
        short portNo;
    public:
        ParsedURL(std::string url);
        void set(const char* newUrl);
        std::string all() const;
        std::string host() const;
        std::string path() const;
        int port();
};

void ServletConnection::connect(const std::string & operation, TupleSet& result) throw(RGMATemporaryException,
        RGMAPermanentException, UnknownResourceException) {
    std::string url = m_servletURL + "/" + operation + m_parameters;
    std::string response;
    httpRequest(url, response);
    XMLConverter::convertXMLResponse(response, result);
}

void ServletConnection::clear() {
    m_parameters = "";
    m_submitMethod = "GET";
}

void ServletConnection::setRequestMethodPost() {
    m_submitMethod = "POST";
}

void ServletConnection::httpRequest(const std::string & url, std::string& response) throw (RGMATemporaryException,
        RGMAPermanentException) {

    // Set up the request.
    ParsedURL m_parsedUrl(url);
    std::string request(m_submitMethod + " ");
    request.append(m_parsedUrl.path());
    request.append(" HTTP/1.1\r\n");
    request.append("Cache-Control: no-cache\r\n");
    std::ostringstream strstream;
    strstream << m_parsedUrl.port();
    request.append("Host: " + m_parsedUrl.host() + ":" + strstream.str() + "\r\n");
    request.append("Accept: */*\r\n");
    request.append("Connection: keep-alive\r\n");
    request.append("\r\n");

    // Send the request and read back the response and accumulate it in answer
    response = "";

    if (m_sock == NULL) {
        m_sock = new SSLSocket();
        m_sock->connect(m_parsedUrl.host(), m_parsedUrl.port());
    }

    m_sock->write(request, response);

    response = stripHeader(response);

}

std::string ServletConnection::stripHeader(const std::string & inDocument) {
    // Separate document and header.  Also remove chunked byte counts from document
    // if present
    std::string document(inDocument);
    std::string header;
    int endOfHeader = 0;
    for (unsigned int index = 0; index < document.size() - 3; ++index) {
        if ((document[index] == 13) && (document[index + 1] == 10) && (document[index + 2] == 13) && (document[index
                + 3] == 10)) {
            header = document.substr(0, index + 3);
            // Convert header to upper case so case is no longer a consideration
            // in looking for substrings
            for (unsigned int headerIndex = 0; headerIndex < header.size(); ++headerIndex) {
                header[headerIndex] = toupper(header[headerIndex]);
            }
            document.erase(0, index + 4);
            endOfHeader = index + 4;
            break;
        }
    }

    // Check for chunked transfer and remove the chunk sizes if chunked
    // transfer was used
    if (strstr(header.c_str(), "TRANSFER-ENCODING: CHUNKED")) {
        std::string unchunkedDocument;

        // document string is whittled down one chunk at a time, and the chunks
        // accumulated in unchunkedDocument.  Once this has been done,
        // unchunkedDocument is copied to document.
        while (true) {

            int chunkSize = strtol(document.c_str(), NULL, 16);

            if (chunkSize) {
                //erase the chunck size line and the new line character
                document.erase(0, document.find_first_of("\n") + 1);
                //get the chunk
                unchunkedDocument += document.substr(0, chunkSize);
                // Erase the chunk from the document along with its following
                // carriage return/line feed
                document.erase(0, chunkSize + 2);
            } else {
                // Zero chunk size marks end of input
                return unchunkedDocument;
            }
        }
    }
    return document;
}

ServletConnection::ServletConnection(const std::string & servletURL) throw(RGMAPermanentException) :
    m_parameters(""), m_servletURL(servletURL), m_sock(0), m_submitMethod("GET") {

}

ServletConnection::~ServletConnection() {
    if (m_sock != NULL) {
        delete m_sock;
    }
}

void ServletConnection::addParameter(const std::string & name, const std::string & value) throw(RGMATemporaryException,
        RGMAPermanentException ) {

    // As this can be any string it must be escaped
    std::string escapedValue(value);
    size_t n;
    size_t m = 0;
    while ((n = escapedValue.substr(m).find_first_not_of(
            ".0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")) != std::string::npos) {
        n = n + m;
        std::ostringstream buff;
        buff << '%' << std::hex << static_cast<int>(escapedValue[n]);
        escapedValue.replace(n, 1, buff.str());
        m = n + 3;
    }
    addCheckedParameter(name, escapedValue);
}

void ServletConnection::addParameter(const std::string & name, int value) throw(RGMATemporaryException,
        RGMAPermanentException ) {
    std::ostringstream strstream;
    strstream << value;
    addCheckedParameter(name, strstream.str());
}

void ServletConnection::addCheckedParameter(const std::string & name, const std::string & value) {
    if (!m_parameters.size()) {
        m_parameters = std::string("?") + name + "=" + value;
    } else {
        m_parameters = m_parameters + "&" + name + "=" + value;
    }
}

void ServletConnection::addParameter(const std::string & name, bool value) throw(RGMATemporaryException,
        RGMAPermanentException ) {
    addCheckedParameter(name, value ? std::string("true") : std::string("false"));
}

// This private class takes a url and provides methods for extracting
// portions of it
ParsedURL::ParsedURL(std::string url) :
    url(url), gotPort(false), portNo(0) {
}

// Change the URL
void ParsedURL::set(const char* newUrl) {
    url = std::string(newUrl);
    gotPort = false;
}

// Return the entire URL
std::string ParsedURL::all() const {
    return url;
}

// Return just the hostname portion of the URL
std::string ParsedURL::host() const {
    std::string theHost = url;
    int start = theHost.find("://");
    if (start >= 0)
        theHost.erase(0, start + 3);
    int end = theHost.find(":");
    if (end < 0)
        end = theHost.find("/");
    if (end >= 0)
        theHost.erase(end);
    return theHost;
}
// Return the path (i.e. local file) portion of the URL
std::string ParsedURL::path() const {
    std::string thePath = url;
    int start = thePath.find("://");
    if (start >= 0) {
        thePath.erase(0, start + 3);
    }
    start = thePath.find("/");
    if (start >= 0) {
        thePath.erase(0, start);
    }
    return thePath;
}

// Return the port number of the URL
int ParsedURL::port() {
    // Need the port several times, so lazily evaluate
    if (!gotPort) {
        std::string thePort = url;
        int start = thePort.find("://");
        if (start >= 0) {
            thePort.erase(0, start + 3);
        }
        int end = thePort.find("/");
        if (end >= 0) {
            thePort.erase(end);
        }
        start = thePort.find(":");
        thePort.erase(0, start + 1);
        portNo = atoi(thePort.c_str());
        gotPort = true;
    }
    return portNo;
}

}
}

/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2010.
 * See http://eu-egee.org/partners/ for details on the copyright holders.
 * For license conditions see the license file or http://eu-egee.org/license.html
 */

/** This module handles the interaction with the R-GMA servlets, by
 forming and sending an HTTP request, and returning the HTTP response.

 We keep the socket open for the lifetime of the
 resource object (because the overhead in the SSL handshaking is huge).

 The HTTP/1.1 Protocol is described in RFC 2616. Most of the fiddliness in
 this code concerns detecting the end of an HTTP response. The salient rules
 are:

 (from 4.3) All 1xx (informational), 204 (no content) and 304 (not modified)
 responses MUST NOT include a message-body. All other responses
 do include a message-body, although it MAY be of zero length.
 (from 4.4) When a message-body is included with a message, the transfer-
 length of that body is determined by one of the following (in
 order of precedence):
 1. Any response which "MUST NOT" include a message body ... is
 always terminated by the first empty line after the header...
 2. If a Transfer-Encoding header field ... is present and has any
 value other than "identity", then the transfer length is
 defined by use of the "chunked" transfer-coding ... unless the
 message is terminated by closing the connection.
 3. If a Content-Length header field ... is present, its decimal
 value in OCTETS represents ... the transfer-length.
 4. [concerns "multipart/byteranges" media type, not used by us]
 5. By the server closing the connection.

 Note that Tomcat has been seen to use 2, 3 and 5, and 1 should be handled
 as well. For Transfer-Encoding, we currently only handle "chunked" data. */

#include <stdio.h>  /* for NULL */
#include <string.h> /* for strcat, strcmp, strstr, strlen */
#include <stdlib.h> /* for atoi, strtol, free */
#include <ctype.h>  /* for toupper, isalnum */

#include "rgma_http.h"
#include "rgma_tcp.h"

#define PRIVATE static
#define PUBLIC /* empty */

#define PROTOCOL_HTTP     0
#define PROTOCOL_HTTPS    1
#define PROTOCOL_UNKNOWN  2

#define STATUS_SUCCESS     0
#define STATUS_OUTOFMEMORY 1
#define STATUS_CANTCONNECT 2
#define STATUS_BADRESPONSE 3
#define STATUS_AUTHFAILURE 4
#define STATUS_TIMEDOUT    5

/* These MAX_* values are failsafe limits to avoid while(1) loops containing
 remote calls from becoming infinite if the protocol goes wrong. The values
 are just meant to be beyond all normal limits. */

#define MAX_HW 1000000  /* bytes per response header line */
#define MAX_HL 1000000  /* number of response header lines */
#define MAX_NC 1000000  /* number of chunks in response payload */

typedef struct {
    int protocol;
    char *url;
    char *host;
    char *port;
    char *path;
} PARSED_URL;

/* Private functions. */

PRIVATE int match_strings(const char *, const char *, int);
PRIVATE char *add_string(char *, const char *, int);
PRIVATE char *add_parameter(char *, const char *, const char *, const char *);
PRIVATE void parse_url(PARSED_URL *);
PRIVATE int get_response(BUFFERED_SOCKET **bsockP, char **responseP);

/* PUBLIC FUNCTIONS **********************************************************/

/**
 * Sends HTTP request to servlet and returns (cleaned up) response.
 * Uses TCP connection in *bsockP if it's not NULL, otherwise creates and
 * stores a new one.
 *
 * @param  bsockP                 Buffered socket.
 * @param  servlet_url            Servlet URL.
 * @param  operation              Operation to add to path (starts with "/").
 * @param  num_parameters         Number of parameters to append.
 * @param  parameters             Name followed by value for all parameters.
 * @param  responseP              Will contain response, on output.
 *
 * @return 0 (success), 1 (out of memory), 2 (can't connect), 3 (data error).
 */

PUBLIC int http_get(BUFFERED_SOCKET **bsockP, char *servlet_url, char *operation, int num_parameters,
        const char **parameters, char **responseP, char** erm) {
    BUFFERED_SOCKET *bsock;
    PARSED_URL u;
    char *request, *response;
    int status, i;

    /* Parse the URL into protocol, host, port and path. */

    u.url = add_string(NULL, servlet_url, 0);
    if (u.url == NULL)
        return STATUS_OUTOFMEMORY;
    parse_url(&u);
    if (u.protocol != PROTOCOL_HTTP && u.protocol != PROTOCOL_HTTPS) {
        free(u.url);
        return STATUS_CANTCONNECT; /* not supported in prototype */
    }

    /* Assemble the HTTP request (this isn't as efficient as it could be,
     but the cost is actually very small). HTTP1.1 requires an absolute
     URL for proxies only, and a "Host:" header entry in all cases. */

    if (strcmp(operation, "/insert") == 0 || strcmp(operation, "/insertList") == 0) {
        request = add_string(NULL, "POST /", 0);
    } else {
        request = add_string(NULL, "GET /", 0);
    }
    if (request)
        request = add_string(request, u.path, 0);
    if (request)
        request = add_string(request, operation, 0);
    if (request)
        for (i = 0; i < num_parameters; ++i) {
            request = add_parameter(request, parameters[2 * i], parameters[2 * i + 1], (i == 0) ? "?" : "&");
            if (!request)
                break;
        }
    if (request)
        request = add_string(request, " HTTP/1.1\r\n", 0);
    if (request)
        request = add_string(request, "Cache-Control: no-cache\r\n", 0);
    if (request)
        request = add_string(request, "Host: ", 0);
    if (request)
        request = add_string(request, u.host, 0);
    if (request)
        request = add_string(request, ":", 0);
    if (request)
        request = add_string(request, u.port, 0);
    if (request)
        request = add_string(request, "\r\n", 0);
    if (request)
        request = add_string(request, "Accept: */*\r\n", 0);
    if (bsockP && request)
        request = add_string(request, "Connection: keep-alive\r\n", 0);
    if (request)
        request = add_string(request, "\r\n", 0);

    if (!request) {
        free(u.url);
        return STATUS_OUTOFMEMORY;
    }

    /* Send the command and obtain the response. It's worth retrying an authentication
     failure (-3). If we are reusing an existing connection, the call to tcp_connect
     will just validate it. */

    if (bsockP) {
        bsock = *bsockP;
    } else {
        bsock = NULL;
    }

    status = tcp_connect(u.host, atoi(u.port), &bsock, erm);
    if (status == -3) {
        status = tcp_connect(u.host, atoi(u.port), &bsock, erm);
    }
    if (bsockP) {
        *bsockP = bsock;
    }
    free(u.url);
    if (status == -1) {
        free(request);
        return STATUS_CANTCONNECT;
    } else if (status == -2) {
        free(request);
        return STATUS_TIMEDOUT;
    } else if (status == -3) {
        free(request);
        return STATUS_AUTHFAILURE;
    }

    status = tcp_writeBytes(bsock, request, strlen(request));
    free(request);
    if (status != 0) {
        tcp_close(bsock);
        if (bsockP) {
            *bsockP = NULL;
        }
        return (status == -2) ? STATUS_TIMEDOUT : STATUS_CANTCONNECT;
    }

    response = NULL;
    status = get_response(&bsock, &response);
    if (bsockP) { /* get_response may have closed the connection */
        *bsockP = bsock;
    }
    if (status == -2) {/* read timed out */
        tcp_close(bsock);
        if (bsockP) {
            *bsockP = NULL;
        }
        return STATUS_TIMEDOUT;
    }
    if (status <= 0 || response == NULL) { /* any other error */
        tcp_close(bsock);
        if (bsockP) {
            *bsockP = NULL;
        }
        return STATUS_BADRESPONSE;
    }

    /* Shut connection if the caller doesn't want it to stay open. */

    if (!bsockP) {
        tcp_close(bsock);
    }

    *responseP = response;
    return STATUS_SUCCESS;
}

/* PRIVATE FUNCTIONS **********************************************************/

/**
 * Local function to do case-insensitive comparison of two strings, avoiding
 * the non-standard strcasecmp() function.
 *
 * @param  s        String to compare.
 * @param  t        String to compare.
 * @param  n        Maximum number of characters to compare.
 *
 * @return 1 is the strings match, 0 if they don't.
 */

PRIVATE int match_strings(const char *s, const char *t, int n) {
    while (n--) {
        if (toupper(*t) != toupper(*s))
            return 0; /* no match */
        if (*s == '\0' || *t == '\0')
            return 1; /* match */
        ++s;
        ++t;
    }
    return 1; /* match */
}

/**
 * Local function to append one string to another. If the first string is not
 * NULL, the memory it points to is resized to fit the whole string. If the
 * second string is NULL, just expands the memory by the requested size.
 *
 * @param  s        Original string (may be NULL).
 * @param  t        String to append (if NULL, n is used).
 * @param  n        Number of characters by which to expand string.
 *
 * @return Pointer to new string, or NULL if memory can't be allocated.
 */

PRIVATE char *add_string(char *s, const char *t, int n) {
    char *new;
    int size;

    if (s != NULL)
        size = strlen(s);
    else
        size = 0;
    if (t != NULL)
        size += strlen(t);
    else
        size += n;
    size += 1; /* terminating NULL */

    new = (char *) realloc(s, size);
    if (new == NULL) {
        if (s != NULL)
            free(s);
        return NULL;
    }
    if (s == NULL)
        new[0] = '\0';
    if (t != NULL)
        strcat(new, t);

    return (new);
}

/**
 * Local function to append a parameter to a URL, encoding the value if
 * necessary. Frees "url" on error.
 *
 * @param  url          Original URL (will be changed).
 * @param  name         Parameter name.
 * @param  value        Unencoded parameter value.
 * @param  separator    Parameter separator ("?" or "&").
 *
 * @return Pointer to new string, or NULL if memory can't be allocated.
 */

PRIVATE char *add_parameter(char *url, const char *name, const char *value, const char *separator) {
    static const char hex[] = "0123456789ABCDEF";

    int length;
    const char *s;
    char *t;

    /* Calculate length of new string (including the encoded value),
     and expand the original string to fit. */

    length = strlen(url) + 1 + strlen(name) + 1;
    for (s = value; *s != '\0'; ++s) {
        if (isalnum(*s) || *s == '.')
            length += 1;
        else
            length += 3;
    }
    length += 1; /* terminating zero */

    url = add_string(url, NULL, length);
    if (url == NULL)
        return NULL; /* out of memory */

    /* Assemble the string (original string + separator + name + "=" +
     encoded value). */

    strcat(url, separator);
    strcat(url, name);
    strcat(url, "=");
    t = url + strlen(url);
    for (s = value; *s != '\0'; ++s) {
        if (isalnum(*s) || *s == '.')
            *t++ = *s;
        else {
            *t++ = '%';
            *t++ = hex[(*s) >> 4];
            *t++ = hex[(*s) & 0xF];
        }
    }
    *t = '\0';

    return url;
}

/**
 * Local function to parse a URL into its component parts.
 *
 * @param  url              The URL to parse.
 * @param  parsed_url       Parsed URL structure (updated).
 *
 * @return Nothing.
 */

PRIVATE void parse_url(PARSED_URL *parsed_url) {
    char *s;

    if (match_strings(parsed_url->url, "https://", 8)) {
        parsed_url->protocol = PROTOCOL_HTTPS;
        parsed_url->port = "443"; /* default */
    } else if (match_strings(parsed_url->url, "http://", 7)) {
        parsed_url->protocol = PROTOCOL_HTTP;
        parsed_url->port = "80"; /* default */
    } else
        parsed_url->protocol = PROTOCOL_UNKNOWN;

    parsed_url->host = parsed_url->url;
    if ((s = strstr(parsed_url->host, "://"))) {
        parsed_url->host = s + 3;
    }

    if ((s = strstr(parsed_url->host, ":"))) {
        *s = '\0';
        parsed_url->port = ++s;
    } else
        s = parsed_url->host; /* rewind (no port specified) */

    if ((s = strstr(s, "/"))) {
        *s = '\0';
        parsed_url->path = s + 1;
    } else
        parsed_url->path = "";
}

/**
 * Local function to read HTTP Response.
 *
 * @param  bsock        Pointer to buffered socket from which to read bytes.
 *                      (the extra level of indirection is for tcp_close()).
 *         responseP    Pointer to response to be filled in.
 *
 * @return 1 on success, -2 on read timeout, 0/-1 on any other error.
 */

PRIVATE int get_response(BUFFERED_SOCKET **bsockP, char **responseP) {
    BUFFERED_SOCKET *bsock;
    char buf[100], *s, *response;
    int statusCode, chunked, contentLength, chunkLength, closeConnection, status, n, max;

    /* Read the first line of the response (status line). */

    bsock = *bsockP; /* for convenience */
    status = tcp_readLine(bsock, buf, sizeof(buf), MAX_HW);
    if (status <= 0) {
        return status; /* failed to read line */
    }
    if (!strtok(buf, " \t\n") || !(s = strtok(NULL, " \t\n"))) {
        return -1;
    }
    statusCode = atoi(s);

    /* Now get the rest of the header lines. */

    chunked = 0;
    contentLength = 0;
    closeConnection = 0;

    max = MAX_HL; /* failsafe: maximum number of header lines */
    while (max--) {
        status = tcp_readLine(bsock, buf, sizeof(buf), MAX_HW);
        if (status <= 0)
            return status; /* failed to read line */

        if (buf[0] == '\n') {
            break; /* end of header */
        } else if (strncmp(buf, "Transfer-Encoding:", 18) == 0) {
            s = strtok(buf + 18, " \t\n");
            if (s && strcmp(s, "chunked") == 0)
                chunked = 1;
        } else if (strncmp(buf, "Content-Length:", 15) == 0) {
            s = strtok(buf + 15, " \t\n");
            if (s)
                contentLength = atoi(s);
        } else if (strncmp(buf, "Connection:", 11) == 0) {
            s = strtok(buf + 11, " \t\n");
            if (s && strcmp(s, "close") == 0) {
                closeConnection = 1;
            }
        } else if (strncmp(buf, "Proxy-Connection:", 17) == 0) {
            /* For HTTP/1.0 Proxy Services (not in RFC2616). */
            s = strtok(buf + 17, " \t\n");
            if (s && strcmp(s, "close") == 0) {
                closeConnection = 1;
            }
        }
    }
    if (max == 0)
        return -1;

    if ((statusCode >= 100 && statusCode <= 199) || statusCode == 204 || statusCode == 304 || (!closeConnection
            && !chunked && contentLength == 0)) {
        return -1; /* no body */
    }

    /* Finally get the response body (removing chunks, if any). */

    response = NULL;
    if (chunked) {/* Transfer-Encoding: chunked */
        contentLength = 0;
        max = MAX_NC; /* failsafe: maximum number of chunks */
        while (max--) {
            status = tcp_readLine(bsock, buf, sizeof(buf), MAX_HW);
            if (status <= 0) {
                if (response)
                    free(response);
                return status;
            }

            if (!(s = strtok(buf, "; \t\n"))) {
                if (response)
                    free(response);
                return -1; /* mangled HTTP */
            }

            chunkLength = strtol(s, NULL, 16);
            if (chunkLength == 0) {
                break; /* no more chunks */
            }

            n = chunkLength + 2; /* incl \r\n */
            s = (char *) realloc(response, contentLength + n);
            if (s == NULL) {
                if (response)
                    free(response);
                return -1; /* out of memory */
            }
            response = s;
            status = tcp_readBytes(bsock, &response[contentLength], n);
            if (status != n) {
                free(response);
                if (status < 0)
                    return status; /* read error */
                else
                    return -1; /* mangled HTTP */
            }
            contentLength += chunkLength;
            response[contentLength] = '\0';
        }
        if (max == 0) {
            if (response)
                free(response);
            return -1; /* mangled HTTP */
        }

        max = MAX_HL; /* failsafe: maximum number of trailer lines */
        while (max--) /* skip over trailer */
        {
            status = tcp_readLine(bsock, buf, sizeof(buf), MAX_HW);
            if (status <= 0) {
                free(response);
                return status; /* read error */
            }
            if (buf[0] == '\n')
                break;
        }
        if (max == 0) {
            if (response)
                free(response);
            return -1; /* mangled HTTP */
        }
    } else if (contentLength > 0) { /* Content-Length: explicitly set */
        n = contentLength;
        response = malloc(contentLength + 1);
        if (response == NULL) {
            return -1;
        }
        status = tcp_readBytes(bsock, response, n);

        if (status != n) {
            free(response);
            if (status < 0)
                return status; /* some read error */
            else
                return -1; /* mangled HTTP */
        }
        response[contentLength] = '\0';
    } else { /* Server will close socket at the end of the response */
        max = MAX_NC; /* failsafe: maximum number of chunks */
        while (max--) {
            n = TCP_NETBUFSIZ; /* defined in rgma_tcp.h */
            s = (char *) realloc(response, contentLength + n + 1);
            if (s == NULL) {
                if (response) {
                    free(response);
                }
                return -1;
            }
            response = s;

            status = tcp_readBytes(bsock, &response[contentLength], n);
            if (status < 0) {
                if (response) {
                    free(response);
                }
                return status;
            }
            if (status == 0) {
                break; /* end-of-response */
            }
            contentLength += status; /* number of bytes read */
        }
        if (max == 0) {
            if (response)
                free(response);
            return -1;
        }
        response[contentLength] = '\0';
    }

    if (closeConnection) {
        tcp_close(bsock);
        *bsockP = NULL;
    }

    *responseP = response;
    return 1;
}

/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef SUPPORTEDQUERIES_H
#define SUPPORTEDQUERIES_H

#include <string>

namespace glite {
namespace rgma {

/**
 * Types of query supported by a PrimaryProducer or SecondaryProducer.
 */
class SupportedQueries {

    public:
        /** Continuous queries only. */
        static const SupportedQueries C;

        /** Continuous and History queries. */
        static const SupportedQueries CH;

        /** Continuous, History and Latest queries. */

        static const SupportedQueries CHL;

        /** Continuous and Latest queries. */
        static const SupportedQueries CL;

    private:

        friend class PrimaryProducer;
        friend class SecondaryProducer;

        SupportedQueries(std::string t);

        bool isLatest() const;
        bool isHistory() const;

        // Data
        std::string m_query;

};

}
}
#endif

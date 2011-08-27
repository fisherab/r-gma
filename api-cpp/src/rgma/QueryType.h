/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef QUERYTYPE_H
#define QUERYTYPE_H

#include <string>

namespace glite {
namespace rgma {

/**
 * Permitted query type for a consumer.
 */
class QueryType {

    public:
        /** Continuous Query. */
        static const QueryType C;

        /** Latest Query. */
        static const QueryType L;

        /** History Query. */

        static const QueryType H;

        /** Static Query. */
        static const QueryType S;


    private:
        friend class Consumer;

        static const QueryType NONE;

        bool operator==(const QueryType & qt) const;
        QueryType(std::string t);

        // Data
        std::string m_query;
};

}
}

#endif

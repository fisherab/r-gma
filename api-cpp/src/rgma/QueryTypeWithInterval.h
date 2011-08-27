/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef QUERYTYPEWITHINTERVAL_H
#define QUERYTYPEWITHINTERVAL_H

#include <string>

namespace glite {
namespace rgma {

/**
 * Permitted query type for a consumer where an interval for the query should be specified.
 */

class QueryTypeWithInterval {

    public:

        /** Continuous Query. */
        static const QueryTypeWithInterval C;

        /** History Query. */
        static const QueryTypeWithInterval H;

        /** Latest Query. */
        static const QueryTypeWithInterval L;

        /**
          * Compares two QueryTypeWithIntervals.
          *
          * @param qt Another QueryTypeWithInterval.
          *
          * @return True if the QueryTypeWithIntervals are equal.
          */

    private:

        friend class Consumer;

        static const QueryTypeWithInterval NONE;

        QueryTypeWithInterval(std::string t);

        bool operator==(const QueryTypeWithInterval & qt) const;

        // Data
        std::string m_query;
};

}
}
#endif

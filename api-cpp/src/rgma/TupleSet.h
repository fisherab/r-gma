/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef TUPLESET_H
#define TUPLESET_H
#include "rgma/Tuple.h"
#include <vector>
#include <ostream>

namespace glite {
namespace rgma {
/**
 * Holds a set of tuples, a warning message and the end of results flag.
 */
class TupleSet {
    public:

        TupleSet();

        /**
         * Is this the last one in a possible sequence of results set
         */
        bool isEndOfResults() const;

        /**
         * Get any warning provided with the result set.
         *
         * @return warning message. This will be an empty string if there is no warning.
         */
        const std::string & getWarning() const;

        /**
         * a const iterator of type vector<Tuple>
         * to traverse a vector of Tuples.
         */
        typedef std::vector<Tuple>::const_iterator const_iterator;

        /**
         * @return an iterator of type vector<Tuple> which points to
         * the first element of the Result Set
         */
        const_iterator begin() const;

        /**
         * @return an iterator of type vector<Tuple> which points to
         * the end of the Result Set
         */
        const_iterator end() const;

        /**
         * @return number of tuples in result set
         *
         */
        int size() const;

    private:

        friend class XMLConverter;
        friend class Consumer;

        void setEndOfResults(bool endOfResults);

        void addRow(Tuple row);

        void setWarning(const std::string & warning);

        void appendWarning(const std::string & warning);

        //Data
        bool m_endOfResults;
        std::vector<Tuple> m_tuples;
        std::string m_warning;

};

}
}
#endif

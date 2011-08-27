/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef TupleStore_H
#define TupleStore_H

#include <string>
#include <iostream>

namespace glite {
namespace rgma {

/**
 * Details of a named tuple store.
 */
class TupleStore {
    public:

        /**
         * Determines if this store is History.
         *
         * @return True if the store is History.
         */
        bool isHistory() const;

        /**
         * Determines if this store is Latest.
         *
         * @return True if the store system is Latest.
         */
        bool isLatest() const;

        /**
         * Gets the logical name for this storage system.
         *
         * @return Logical name as a String
         */
        const std::string & getLogicalName() const;

    private:

        friend class RGMAService;

        TupleStore(const std::string & logicalName, bool isHistory, bool isLatest);

        // Data
        std::string m_logicalName;
        bool m_isHistory;
        bool m_isLatest;

};
std::ostream& operator<<(std::ostream& stream, const TupleStore & ts);

}
}
#endif

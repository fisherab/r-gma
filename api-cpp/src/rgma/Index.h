/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef INDEX_H
#define INDEX_H

#include <vector>
#include <string>

namespace glite {
namespace rgma {

/**
 * An index on a table.
 */
class Index {
    public:

        /**
         * Returns a constant reference to the name of this index.
         *
         * @return a constant reference to the name of this index.
         */
        const std::string & getIndexName() const;

        /**
         * Returns a constant reference to the list of column names used by this index.
         *
         * @return a constant reference to the list of column names used by this index.
         */
        const std::vector<std::string> & getColumnNames() const;

    private:

        std::string m_name;
        std::vector<std::string> m_columnNames;

        friend class Schema;

        Index(const std::string & indexName, const std::vector<std::string> & columnNames);

};
std::ostream& operator<<(std::ostream& stream, const Index & index);
}
}
#endif

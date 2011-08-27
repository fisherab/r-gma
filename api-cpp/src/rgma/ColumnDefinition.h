/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef COLUMNDEFINITION_H
#define COLUMNDEFINITION_H

#include "rgma/RGMAType.h"

#include <string>

namespace glite {
namespace rgma {
/**
 * Definition of a column which may be used inside a TableDefinition.
 */
class ColumnDefinition {
    public:

        /**
         * Returns a constant reference to the name of the column.
         *
         * @return a constant reference to the name of the column
         */
        const std::string & getName() const;

        /**
         * Returns the NOT NULL flag.
         *
         * @return true if column is "NOT NULL"
         */
        bool isNotNull() const;

        /**
         * Returns the PRIMARY KEY flag.
         *
         * @return true if column is "PRIMARY KEY"
         */
        bool isPrimaryKey() const;

        /**
         * Returns a constant reference to the type of the column.
         *
         * @return a constant reference to the type of the column.
         */
        const RGMAType & getType() const;

        /**
         * Returns the size of the column type.
         *
         * @return the size of the column type (or 0 if none is specified)
         */
        int getSize() const;

    private:

        friend class Schema;
        friend class TableDefinition;

        ColumnDefinition(const std::string & name, const RGMAType & type, int size, bool isNotNull, bool isPrimaryKey);

        // Data
        std::string m_name;
        RGMAType m_type;
        int m_size;
        bool m_notNull;
        bool m_primaryKey;

};
std::ostream& operator<<(std::ostream& stream, const ColumnDefinition & colDef);
}
}
#endif

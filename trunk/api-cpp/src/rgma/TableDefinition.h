/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef TABLEDEFINITIONLIST_H
#define TABLEDEFINITIONLIST_H

#include "rgma/ColumnDefinition.h"

#include <vector>
#include <string>

namespace glite {
namespace rgma {
/**
 * Contains the column definitions for a table.
 */
class TableDefinition {
    public:

        /**
         * Returns the column definitions for this table.
         *
         * @return a const reference to the table columns.
         */
        const std::vector< ColumnDefinition> & getColumns() const;

        /**
         * Returns the tableName.
         *
         * @return a const reference to the table name.
         */
        const std::string & getTableName() const;

        /**
         * Returns name of table for which this is a view or an empty string.
         *
         * @return a constant reference to the name of the table for which this is a view or an empty string.
         */
        const std::string & getViewFor() const;

        /**
         * Returns true if this is a view rather than a table.
         *
         * @return true if this is a view rather than a table.
         */
        bool isView() const;

    private:

        friend class Schema;

        TableDefinition(std::string tableName, std::string viewFor, const std::vector< ColumnDefinition> & columns);

        // Data
        std::string m_tableName;
        std::string m_viewFor;
        std::vector< ColumnDefinition> m_columns;
};
std::ostream& operator<<(std::ostream& stream, const TableDefinition & td);
}
}
#endif

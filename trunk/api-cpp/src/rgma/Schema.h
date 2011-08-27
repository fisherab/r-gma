/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef SCHEMA_H
#define SCHEMA_H

#include "rgma/RGMATemporaryException.h"
#include "rgma/RGMAPermanentException.h"
#include "rgma/Index.h"
#include "rgma/TableDefinition.h"
#include "rgma/ServletConnection.h"

#include <string>
#include <vector>

namespace glite {
namespace rgma {

/**
 * A schema allows tables to be created and their definitions
 * manipulated for a specific VDB.
 */
class Schema {
    public:

        /**
         * Constructs a schema object for the specified VDB.
         *
         * @param vdbName
         *            name of VDB
         *
         * @throws RGMATemporaryException
         * @throws RGMAPermanentException
         */
        Schema(const std::string & vdbName) throw(RGMATemporaryException, RGMAPermanentException);

        /**
         * Gets a list of all tables and views in the schema.
         *
         * @return A StringList of table names.
         *
         * @throws RGMATemporaryException
         * @throws RGMAPermanentException
         */
        std::vector<std::string> getAllTables()  throw(RGMATemporaryException, RGMAPermanentException);

        /**
         * Gets the definition for the named table or view.
         *
         * @param tableName Name of table.
         *
         * @return Table definition.
         *
         * @throws RGMATemporaryException
         * @throws RGMAPermanentException
         */
        TableDefinition getTableDefinition(const std::string & tableName)  throw(RGMATemporaryException,
                RGMAPermanentException);

        /**
         * Gets the list of indexes for a table.
         *
         * @param tableName Name of table.
         *
         * @return List of Index objects.
         *
         * @throws RGMATemporaryException
         * @throws RGMAPermanentException
         */
        std::vector<Index> getTableIndexes(const std::string & tableName)  throw(RGMATemporaryException,
                RGMAPermanentException);

        /**
         * Sets the authorization rules for the given table or view.
         *
         * @param tableName Table name.
         * @param tableAuthz Table authorization details.
         *
         * @throws RGMATemporaryException
         * @throws RGMAPermanentException
         */
        void setAuthorizationRules(const std::string & tableName, const std::vector<std::string> & tableAuthz)
                throw(RGMATemporaryException, RGMAPermanentException);

        /**
         * Gets the authorization rules for a table or view.
         *
         * @param tableName Table name.
         *
         * @return Table authorization details.
         *
         * @throws RGMATemporaryException
         * @throws RGMAPermanentException
         */
        std::vector<std::string> getAuthorizationRules(const std::string & tableName)
                throw(RGMATemporaryException, RGMAPermanentException);

        /**
         * Creates a table in the schema.
         *
         * @param createTableStatement SQL CREATE TABLE statement.
         * @param tableAuthz Table authorization details.
         *
         * @throws RGMATemporaryException
         * @throws RGMAPermanentException
         */
        void createTable(const std::string & createTableStatement, const std::vector<std::string> & tableAuthz)
                throw(RGMATemporaryException, RGMAPermanentException);

        /**
         * Drops a table from the schema.
         *
         * @param tableName Table to drop.
         *
         * @throws RGMATemporaryException
         * @throws RGMAPermanentException
         */
        void dropTable(const std::string & tableName) throw(RGMATemporaryException, RGMAPermanentException);

        /**
         * Creates an index on a table.
         *
         * @param createIndexStatement SQL CREATE INDEX statement.
         *
         * @throws RGMATemporaryException
         * @throws RGMAPermanentException
         */
        void createIndex(const std::string & createIndexStatement) throw(RGMATemporaryException,
                RGMAPermanentException);

        /**
         * Drops an index from a table.
         *
         * @param indexName Index to drop.
         * @param tableName the table name to create the index for.
         *
         * @throws RGMATemporaryException
         * @throws RGMAPermanentException
         */
        void dropIndex(const std::string & tableName, const std::string & indexName) throw(RGMATemporaryException,
                RGMAPermanentException);

        /**
         * Creates a view on a table.
         *
         * @param createViewStatement SQL CREATE VIEW statement.
         * @param viewAuthz View authorization details.
         *
         * @throws RGMATemporaryException
         * @throws RGMAPermanentException
         */
        void createView(const std::string & createViewStatement, const std::vector<std::string> & viewAuthz)
                throw(RGMATemporaryException, RGMAPermanentException);

        /**
         * Drops a view from the schema.
         *
         * @param viewName View to drop.
         *
         * @throws RGMATemporaryException
         * @throws RGMAPermanentException
         */
        void dropView(const std::string & viewName) throw(RGMATemporaryException, RGMAPermanentException);

    private:

        void clearServletConnection();

        // Data
        const std::string m_vdbName;

        ServletConnection m_connection;
};
}
}
#endif

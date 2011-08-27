#include "rgma/Schema.h"
#include "rgma/ColumnDefinition.h"
#include "rgma/RGMAType.h"
#include "rgma/URLBuilder.h"

namespace glite {
namespace rgma {

Schema::Schema(const std::string & vdbName) throw(RGMATemporaryException, RGMAPermanentException) :
    m_vdbName(vdbName), m_connection(URLBuilder::getURL("Schema")) {
}

void Schema::clearServletConnection() {
    m_connection.clear();
    m_connection.addParameter("vdbName", m_vdbName);
}

std::vector<std::string> Schema::getAllTables() throw(RGMATemporaryException, RGMAPermanentException) {
    clearServletConnection();
    m_connection.addParameter("canForward", true);
    TupleSet result;
    m_connection.connect("getAllTables", result);

    TupleSet::const_iterator it;
    std::vector<std::string> tableNames;
    for (it = result.begin(); it != result.end(); ++it) {
        tableNames.push_back(it->getString(0));
    }
    return tableNames;
}

TableDefinition Schema::getTableDefinition(const std::string & tableName) throw(RGMATemporaryException,
        RGMAPermanentException) {
    clearServletConnection();
    m_connection.addParameter("tableName", tableName);
    m_connection.addParameter("canForward", true);
    TupleSet result;
    m_connection.connect("getTableDefinition", result);

    std::vector<ColumnDefinition> columnList;
    TupleSet::const_iterator it;

    for (it = result.begin(); it != result.end(); ++it) {
        std::string columnName(it->getString(1));
        std::string type(it->getString(2));
        int size(it->getInt(3));
        bool isNotNull = it->getBool(4);
        bool isPrimaryKey = it->getBool(5);
        ColumnDefinition columnDef(columnName, RGMAType::getFromValue(type), size, isNotNull, isPrimaryKey);
        columnList.push_back(columnDef);
    }
    it = result.begin();
    std::string viewFor(it->getString(6));
    std::string tName(it->getString(0));

    return TableDefinition(tName, viewFor, columnList);
}

std::vector<Index> Schema::getTableIndexes(const std::string & tableName) throw(RGMATemporaryException,
        RGMAPermanentException) {
    clearServletConnection();
    m_connection.addParameter("tableName", tableName);
    m_connection.addParameter("canForward", true);
    TupleSet result;
    m_connection.connect("getTableIndexes", result);

    TupleSet::const_iterator it;
    std::vector<Index> indexes;
    std::string currentIndex = "";
    std::vector<std::string> columnNames;

    for (it = result.begin(); it != result.end(); ++it) {
        std::string indexName(it->getString(0));
        std::string columnName(it->getString(1));

        if (currentIndex == "") {
            currentIndex = indexName;
            columnNames.push_back(columnName);
        } else {
            if (currentIndex == indexName) {
                columnNames.push_back(columnName);
            } else {
                Index index(currentIndex, columnNames);
                indexes.push_back(index);
                columnNames.clear();
                columnNames.push_back(columnName);
                currentIndex = indexName;
            }
        }
    }

    if (currentIndex != "") {
        Index index(currentIndex, columnNames);
        indexes.push_back(index);
    }

    return indexes;

}

void Schema::setAuthorizationRules(const std::string & tableName, const std::vector<std::string> & tableAuthz)
        throw(RGMATemporaryException, RGMAPermanentException) {
    clearServletConnection();
    m_connection.addParameter("tableName", tableName);
    m_connection.addParameter("canForward", true);
    std::vector<std::string>::const_iterator it(tableAuthz.begin());
    while (it != tableAuthz.end()) {
        m_connection.addParameter("tableAuthz", *it);
        ++it;
    }
    TupleSet result;
    m_connection.connect("setAuthorizationRules", result);
}

std::vector<std::string> Schema::getAuthorizationRules(const std::string & tableName) throw(RGMATemporaryException,
        RGMAPermanentException) {
    clearServletConnection();
    m_connection.addParameter("tableName", tableName);
    m_connection.addParameter("canForward", true);
    TupleSet result;
    m_connection.connect("getAuthorizationRules", result);

    TupleSet::const_iterator it;
    std::vector<std::string> tableAuthz;

    for (it = result.begin(); it != result.end(); ++it) {
        std::string columnName(it->getString(0));
        tableAuthz.push_back(columnName);
    }
    return tableAuthz;
}

void Schema::createTable(const std::string & createTableStatement, const std::vector<std::string> & tableAuthz)
        throw(RGMATemporaryException, RGMAPermanentException) {
    clearServletConnection();
    m_connection.addParameter("createTableStatement", createTableStatement);
    m_connection.addParameter("canForward", true);
    std::vector<std::string>::const_iterator it(tableAuthz.begin());
    while (it != tableAuthz.end()) {
        m_connection.addParameter("tableAuthz", *it);
        ++it;
    }
    TupleSet result;
    m_connection.connect("createTable", result);
}

void Schema::dropTable(const std::string & tableName) throw(RGMATemporaryException, RGMAPermanentException) {
    clearServletConnection();
    m_connection.addParameter("tableName", tableName);
    m_connection.addParameter("canForward", true);
    TupleSet result;
    m_connection.connect("dropTable", result);
}

void Schema::createIndex(const std::string & createIndexStatement) throw(RGMATemporaryException,
        RGMAPermanentException) {
    clearServletConnection();
    m_connection.addParameter("createIndexStatement", createIndexStatement);
    m_connection.addParameter("canForward", true);
    TupleSet result;
    m_connection.connect("createIndex", result);
}

void Schema::dropIndex(const std::string & tableName, const std::string & indexName) throw(RGMATemporaryException,
        RGMAPermanentException) {
    clearServletConnection();
    m_connection.addParameter("indexName", indexName);
    m_connection.addParameter("tableName", tableName);
    m_connection.addParameter("canForward", true);
    TupleSet result;
    m_connection.connect("dropIndex", result);
}

void Schema::createView(const std::string & createViewStatement, const std::vector<std::string> & viewAuthz)
        throw(RGMATemporaryException, RGMAPermanentException) {
    clearServletConnection();
    m_connection.addParameter("createViewStatement", createViewStatement);
    m_connection.addParameter("canForward", true);
    std::vector<std::string>::const_iterator it(viewAuthz.begin());
    while (it != viewAuthz.end()) {
        m_connection.addParameter("viewAuthz", *it);
        ++it;
    }
    TupleSet result;
    m_connection.connect("createView", result);
}

void Schema::dropView(const std::string & viewName) throw(RGMATemporaryException, RGMAPermanentException) {
    clearServletConnection();
    m_connection.addParameter("viewName", viewName);
    m_connection.addParameter("canForward", true);
    TupleSet result;
    m_connection.connect("dropView", result);
}

}
}

#include "rgma/ColumnDefinition.h"
#include "rgma/TableDefinition.h"

#include <sstream>

namespace glite {
namespace rgma {
TableDefinition::TableDefinition(std::string tableName, std::string viewFor,
        const std::vector< ColumnDefinition> & columns) :
    m_tableName(tableName), m_viewFor(viewFor), m_columns(columns) {
}

const std::vector< ColumnDefinition> & TableDefinition::getColumns() const {
    return m_columns;
}

const std::string & TableDefinition::getTableName() const {
    return m_tableName;
}

const std::string & TableDefinition::getViewFor() const {
    return m_viewFor;
}

 bool TableDefinition::isView() const {
    return (m_viewFor != "");
}

std::ostream& operator<<(std::ostream& stream, const TableDefinition & td) {
    std::stringstream strstream;
    stream << std::string("TableDefinition{\n");
    stream << std::string("tableName=") << td;
    std::vector< ColumnDefinition> columnDefs = td.getColumns();

    stream << std::string("]");

    int size = columnDefs.size();

    for (int i = 0; i < size; i++) {
        ColumnDefinition colDef = columnDefs[i];

        strstream << colDef.getSize();

        stream << colDef.getName() << "(" << strstream.str() << " )";
        if (colDef.isNotNull() || colDef.isPrimaryKey()) {
            stream << std::string(" NOT NULL");
        }
        if (colDef.isPrimaryKey()) {
            stream << std::string(" PRIMARY KEY");
        }

        if (i + 1 != size) {
            stream << ", ";
        }
    }
    stream << std::string("]");

    stream << std::string("}");
    return stream;
}
}
}

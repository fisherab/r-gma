#include "rgma/ColumnDefinition.h"
#include <string>
#include <sstream>

namespace glite {
namespace rgma {
/**
 * Creates a new ColumnDefinition object.
 */
ColumnDefinition::ColumnDefinition(const std::string & name, const RGMAType & type, int size, bool isNotNull, bool isPrimaryKey) :
    m_name(name), m_type(type), m_size(size), m_notNull(isNotNull), m_primaryKey(isPrimaryKey) {
}

const std::string & ColumnDefinition::getName() const {
    return m_name;
}

bool ColumnDefinition::isNotNull() const {
    return m_notNull;
}

bool ColumnDefinition::isPrimaryKey() const {
    return m_primaryKey;
}

const RGMAType & ColumnDefinition::getType() const {
    return m_type;
}

int ColumnDefinition::getSize() const {
    return m_size;
}

std::ostream& operator<<(std::ostream& stream, const ColumnDefinition & colDef) {
    stream << colDef.getName() << "(" << colDef.getSize() << " )";
    if (colDef.isNotNull()) {
        stream << " NOT NULL";
    }
    if (colDef.isPrimaryKey()) {
        stream << " PRIMARY KEY";
    }
    return stream;
}

}
}

#include "rgma/Index.h"

namespace glite {
namespace rgma {

Index::Index(const std::string & indexName, const std::vector<std::string> & columnNames) {
    m_name = indexName;
    m_columnNames.clear();
    m_columnNames = columnNames;

}

const std::string & Index::getIndexName() const {
    return m_name;
}

const std::vector<std::string> & Index::getColumnNames() const {
    return m_columnNames;
}

std::ostream& operator<<(std::ostream& stream, const Index & index) {
    const std::vector<std::string> & cols = index.getColumnNames();
    stream << std::string("Index[") << index.getIndexName() << std::string(", columns=");
    std::vector<std::string>::const_iterator it(cols.begin());
    while (it != cols.end()) {
        stream << *it << std::string(" ");
        ++it;
    }
    stream << std::string("]");
    return stream;
}
}
}

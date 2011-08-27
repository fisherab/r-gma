#include "rgma/TupleStore.h"

#include <string>
#include <sstream>

namespace glite {
namespace rgma {

TupleStore::TupleStore(const std::string & logicalName, bool isHistory, bool isLatest) :
    m_logicalName(logicalName), m_isHistory(isHistory), m_isLatest(isLatest) {
}

bool TupleStore::isHistory() const {
    return m_isHistory;
}

bool TupleStore::isLatest() const {
    return m_isLatest;
}

const std::string & TupleStore::getLogicalName() const {
    return m_logicalName;
}

std::ostream& operator<<(std::ostream& stream, const TupleStore & ts) {
    stream << std::string("TupleStore[logicalName=") << ts.getLogicalName();
    stream << std::string(", [");
    std::ostringstream strstream;
    strstream << (ts.isHistory() ? "H" : "-");
    strstream << (ts.isLatest() ? "L" : "-");
    stream << strstream.str();

    stream << std::string("]]");

    return stream;
}
}
}

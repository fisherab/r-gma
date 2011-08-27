#include "rgma/ProducerTableEntry.h"

#include <sstream>

namespace glite {
namespace rgma {

ProducerTableEntry::ProducerTableEntry(const ResourceEndpoint & endpoint,
        bool isSecondary, bool isContinuous, bool isStatic, bool isHistory, bool isLatest, const std::string & predicate, int hrpSec) :
    m_endpoint(endpoint), m_isSecondary(isSecondary), m_isContinuous(isContinuous), m_isStatic(
            isStatic), m_isHistory(isHistory), m_isLatest(isLatest),  m_predicate(predicate), m_hrp(TimeInterval(hrpSec)) {
}

const ResourceEndpoint & ProducerTableEntry::getEndpoint() const {
    return m_endpoint;
}

bool ProducerTableEntry::isContinuous() const {
    return m_isContinuous;
}

bool ProducerTableEntry::isHistory() const {
    return m_isHistory;
}

bool ProducerTableEntry::isLatest() const {
    return m_isLatest;
}

bool ProducerTableEntry::isSecondary() const {
    return m_isSecondary;
}

bool ProducerTableEntry::isStatic() const {
    return m_isStatic;
}

const TimeInterval & ProducerTableEntry::getRetentionPeriod() const {
    return m_hrp;
}

const std::string & ProducerTableEntry::getPredicate() const {
    return m_predicate;
}

std::ostream& operator<<(std::ostream& stream, const ProducerTableEntry & pte) {
    std::ostringstream buff;
    buff << "ProducerTableEntry[";
    buff << "endpoint=" << pte.m_endpoint;
    buff << ", type=";
    buff << (pte.m_isContinuous ? 'C' : '-');
    buff << (pte.m_isHistory ? 'H' : '-');
    buff << (pte.m_isLatest ? 'L' : '-');
    buff << (pte.m_isSecondary ? 'R' : '-');
    buff << (pte.m_isStatic ? 'S' : '-');
    buff << ", predicate=" << pte.m_predicate;
    buff << ", retention period=" << pte.m_hrp;
    buff << "]";
    return stream << buff.str();
}

}
}

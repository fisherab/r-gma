#include "rgma/RGMAService.h"
#include "rgma/ServletConnection.h"
#include "rgma/URLBuilder.h"

namespace glite {
namespace rgma {

std::string RGMAService::getVersion() throw(RGMATemporaryException, RGMAPermanentException) {
    ServletConnection connection(URLBuilder::getURL("RGMAService"));
    TupleSet result;
    connection.connect("getVersion", result);
    TupleSet::const_iterator it = result.begin();
    return it->getString(0);
}

std::vector<TupleStore> RGMAService::listTupleStores() throw (RGMATemporaryException, RGMAPermanentException) {
    ServletConnection connection(URLBuilder::getURL("RGMAService"));
    TupleSet result;
    connection.connect("listTupleStores", result);
    std::vector<TupleStore> stores;
    TupleSet::const_iterator it(result.begin());
    while (it != result.end()) {
        stores.push_back(TupleStore(it->getString(0), it->getBool(1), it->getBool(2)));
        ++it;
    }
    return stores;
}

TimeInterval RGMAService::getTerminationInterval() throw (RGMAPermanentException, RGMATemporaryException) {
    ServletConnection connection(URLBuilder::getURL("RGMAService"));
    TupleSet result;
    connection.connect("getTerminationInterval", result);
    TupleSet::const_iterator it = result.begin();
    return TimeInterval(it->getInt(0), TimeUnit::SECONDS);
}

void RGMAService::dropTupleStore(const std::string & logicalName) throw (RGMATemporaryException,
        RGMAPermanentException) {
    ServletConnection connection(URLBuilder::getURL("RGMAService"));
    connection.addParameter("logicalName", logicalName);
    TupleSet result;
    connection.connect("dropTupleStore", result);
}

}
}

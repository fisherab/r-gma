/*
 * Copyright (c) Members of the EGEE Collaboration. 2004-2009.
 */
#ifndef RGMASERVICE_H
#define RGMASERVICE_H

#include "rgma/TupleStore.h"
#include "rgma/TimeInterval.h"
#include "rgma/RGMATemporaryException.h"
#include "rgma/RGMAPermanentException.h"

#include <string>
#include <vector>

namespace glite {
namespace rgma {

/**
 * Provides various static methods
 */
class RGMAService {

    public:

        /**
         * Gets the version of this implementation.
         *
         * @return The version number.
         *
         * @throws RGMAPermanentException
         * @throws RGMATemporaryException
         */
        static std::string getVersion() throw(RGMATemporaryException, RGMAPermanentException);

        /**
         * Returns the termination interval that will be applied to all resources.
         *
         * @return the termination interval as a TimeInterval
         *
         * @throws RGMAPermanentException
         * @throws RGMATemporaryException
         */
         static TimeInterval getTerminationInterval() throw (RGMAPermanentException, RGMATemporaryException);

        /**
         * Returns a list of existing tuple stores that can be used by
         * Primary and Secondary Producers.
         *
         * @return A TupleStoreList object.
         *
         * @throws RGMAPermanentException
         * @throws RGMATemporaryException
         */
        static std::vector<TupleStore> listTupleStores() throw(RGMATemporaryException, RGMAPermanentException);

        /**
         * Permanently deletes a named tuple store.
         *
         * @param logicalName Name of tuple store.
         *
         * @throws RGMAPermanentException
         * @throws RGMATemporaryException
         */
        static void dropTupleStore(const std::string & logicalName) throw (RGMATemporaryException,
                RGMAPermanentException);

};
}
}
#endif
